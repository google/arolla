// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#include "py/arolla/types/qtype/py_object_boxing.h"

#include <Python.h>

#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/indestructible.h"
#include "arolla/util/repr.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::python {

namespace {

class PyObjectSerializationRegistry {
  // Thread-safe registry for PyObject* serialization functions.
 public:
  static PyObjectSerializationRegistry& instance() {
    static Indestructible<PyObjectSerializationRegistry> instance;
    return *instance;
  }

  void RegisterSerializationFn(PyObjectEncodingFn fn) {
    absl::MutexLock lock(&serialization_mutex_);
    serialization_fn_ = std::move(fn);
  }

  void RegisterDeserializationFn(PyObjectDecodingFn fn) {
    absl::MutexLock lock(&deserialization_mutex_);
    deserialization_fn_ = std::move(fn);
  }

  // Returns a copy for multi-thread safety.
  absl::StatusOr<PyObjectEncodingFn> GetSerializationFn() const {
    absl::MutexLock lock(&serialization_mutex_);
    if (!serialization_fn_) {
      return absl::FailedPreconditionError(
          "no PyObject serialization function has been registered");
    }
    return serialization_fn_;
  }

  // Returns a copy for multi-thread safety.
  absl::StatusOr<PyObjectDecodingFn> GetDeserializationFn() const {
    absl::MutexLock lock(&deserialization_mutex_);
    if (!deserialization_fn_) {
      return absl::FailedPreconditionError(
          "no PyObject serialization function has been registered");
    }
    return deserialization_fn_;
  }

 private:
  ~PyObjectSerializationRegistry() = delete;
  mutable absl::Mutex serialization_mutex_;
  mutable absl::Mutex deserialization_mutex_;
  PyObjectDecodingFn deserialization_fn_
      ABSL_GUARDED_BY(deserialization_mutex_);
  PyObjectEncodingFn serialization_fn_ ABSL_GUARDED_BY(serialization_mutex_);
};

// Shortens registered codecs, and leaves others untouched.
std::string GetShortenedCodec(absl::string_view codec) {
  constexpr absl::string_view registered_codec_prefix =
      "py_obj_codec:arolla.types.s11n.registered_py_object_codecs.";
  if (registered_codec_prefix ==
      codec.substr(0, registered_codec_prefix.length())) {
    return absl::StrCat("<registered> ",
                        codec.substr(registered_codec_prefix.length()));
  } else {
    return std::string(codec);
  }
}

// Represents a PyObject.
class WrappedPyObject {
 public:
  WrappedPyObject() = default;

  WrappedPyObject(PyObjectGILSafePtr object, std::optional<std::string> codec)
      : object_(std::move(object)), codec_(std::move(codec)) {}

  // Returns the serialization codec.
  const std::optional<std::string>& GetCodec() const { return codec_; }

  // Returns the python object.
  const PyObjectGILSafePtr& GetObject() const { return object_; }

 private:
  PyObjectGILSafePtr object_;
  std::optional<std::string> codec_;
};

class PyObjectQType final : public QType {
 public:
  PyObjectQType()
      : QType(ConstructorArgs{
            .name = "PY_OBJECT",
            .type_info = typeid(WrappedPyObject),
            .type_layout = MakeTypeLayout<WrappedPyObject>(),
        }) {}

  ReprToken UnsafeReprToken(const void* source) const final {
    AcquirePyGIL gil_acquire;
    const auto& serializable_py_object =
        *static_cast<const WrappedPyObject*>(source);
    const auto& src = serializable_py_object.GetObject();
    if (src == nullptr) {
      return ReprToken{"PyObject{nullptr}"};
    }
    const auto& codec = serializable_py_object.GetCodec();
    PyObjectPtr py_unicode;
    if (codec) {
      py_unicode = PyObjectPtr::Own(PyUnicode_FromFormat(
          "PyObject{%R, codec=b\'%s\'}", src.get(),
          absl::CHexEscape(GetShortenedCodec(codec.value())).c_str()));
    } else {
      py_unicode =
          PyObjectPtr::Own(PyUnicode_FromFormat("PyObject{%R}", src.get()));
    }
    if (py_unicode == nullptr) {
      PyErr_Print();
      return ReprToken{"PyObject{unknown error occurred}"};
    }
    Py_ssize_t data_size;
    const char* data = PyUnicode_AsUTF8AndSize(py_unicode.get(), &data_size);
    if (data == nullptr) {
      PyErr_Print();
      return ReprToken{"PyObject{unknown error occurred}"};
    }
    return ReprToken{std::string(data, data_size)};
  }

  void UnsafeCopy(const void* source, void* destination) const final {
    if (source != destination) {
      *static_cast<WrappedPyObject*>(destination) =
          *static_cast<const WrappedPyObject*>(source);
    }
  }

  void UnsafeCombineToFingerprintHasher(const void* source,
                                        FingerprintHasher* hasher) const final {
    auto& py_object_ptr = *static_cast<const WrappedPyObject*>(source);
    // TODO: Introduce reproducible fingerprinting based on the
    // ideas in:
    // https://docs.google.com/document/d/1G53kbUfBTmzIrl9EsNAhk2Z_SDaVE8APQbzLY35moQA
    hasher->Combine(
        absl::GetCurrentTimeNanos(),
        reinterpret_cast<uintptr_t>(py_object_ptr.GetObject().get()));
  }

  absl::string_view UnsafePyQValueSpecializationKey(
      const void* source) const final {
    return "::PyObject*";
  }
};

}  // namespace

QTypePtr GetPyObjectQType() {
  static const Indestructible<::arolla::python::PyObjectQType> result;
  return result.get();
}

absl::StatusOr<TypedValue> BoxPyObject(PyObject* object,
                                       std::optional<std::string> codec) {
  DCheckPyGIL();
  DCHECK_NE(object, nullptr);
  auto obj = PyObjectGILSafePtr::NewRef(object);
  if (IsPyQValueInstance(obj.get())) {
    auto& typed_value = UnsafeUnwrapPyQValue(obj.get());
    return absl::InvalidArgumentError(
        absl::StrCat("expected a python type, got a natively supported ",
                     typed_value.GetType()->name()));
  }
  return TypedValue::FromValueWithQType(
      WrappedPyObject(std::move(obj), std::move(codec)), GetPyObjectQType());
}

absl::StatusOr<TypedValue> DecodePyObject(absl::string_view data,
                                          std::string codec) {
  ASSIGN_OR_RETURN(
      auto deserialization_fn,
      PyObjectSerializationRegistry::instance().GetDeserializationFn());
  AcquirePyGIL gil_acquire;
  ASSIGN_OR_RETURN(auto py_obj, deserialization_fn(data, codec));
  return BoxPyObject(py_obj, std::move(codec));
}

absl::Status AssertPyObjectQValue(TypedRef value) {
  if (value.GetType() != GetPyObjectQType()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected %s, got %s", GetPyObjectQType()->name(),
                        value.GetType()->name()));
  } else {
    return absl::OkStatus();
  }
}

absl::StatusOr<PyObject*> UnboxPyObject(const TypedValue& value) {
  RETURN_IF_ERROR(AssertPyObjectQValue(value.AsRef()));
  const auto& wrapped_py_obj = value.UnsafeAs<WrappedPyObject>();
  DCHECK_NE(wrapped_py_obj.GetObject().get(), nullptr);
  return PyObjectGILSafePtr(wrapped_py_obj.GetObject())
      .release();  // increasing the ref-counter of the existing object
}

absl::StatusOr<std::optional<std::string>> GetPyObjectCodec(TypedRef value) {
  RETURN_IF_ERROR(AssertPyObjectQValue(value));
  return value.UnsafeAs<WrappedPyObject>().GetCodec();
}

absl::StatusOr<std::string> EncodePyObject(TypedRef value) {
  RETURN_IF_ERROR(AssertPyObjectQValue(value));
  const auto& wrapped_py_obj = value.UnsafeAs<WrappedPyObject>();
  const auto& maybe_codec = wrapped_py_obj.GetCodec();
  if (!maybe_codec.has_value()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("missing serialization codec for %s", value.Repr()));
  }
  ASSIGN_OR_RETURN(
      auto serialization_fn,
      PyObjectSerializationRegistry::instance().GetSerializationFn());
  auto py_obj = wrapped_py_obj.GetObject();
  return serialization_fn(py_obj.get(), *maybe_codec);
}

void RegisterPyObjectEncodingFn(PyObjectEncodingFn fn) {
  PyObjectSerializationRegistry::instance().RegisterSerializationFn(
      std::move(fn));
}

void RegisterPyObjectDecodingFn(PyObjectDecodingFn fn) {
  PyObjectSerializationRegistry::instance().RegisterDeserializationFn(
      std::move(fn));
}

}  // namespace arolla::python
