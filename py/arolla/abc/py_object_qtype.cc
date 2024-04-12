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
#include "py/arolla/abc/py_object_qtype.h"

#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
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
    if (codec.has_value()) {
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
};

absl::Status AssertPyObjectQValue(TypedRef value) {
  if (value.GetType() != GetPyObjectQType()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected %s, got %s", GetPyObjectQType()->name(),
                        value.GetType()->name()));
  } else {
    return absl::OkStatus();
  }
}

}  // namespace

QTypePtr GetPyObjectQType() {
  static const Indestructible<PyObjectQType> result;
  return result.get();
}

absl::StatusOr<TypedValue> MakePyObjectQValue(
    PyObjectPtr obj, std::optional<std::string> codec) {
  DCheckPyGIL();
  DCHECK_NE(obj.get(), nullptr);
  if (IsPyQValueInstance(obj.get())) {
    auto& typed_value = UnsafeUnwrapPyQValue(obj.get());
    return absl::InvalidArgumentError(
        absl::StrCat("expected a python type, got a natively supported ",
                     typed_value.GetType()->name()));
  }
  return TypedValue::FromValueWithQType(
      WrappedPyObject(PyObjectGILSafePtr::Own(obj.release()), std::move(codec)),
      GetPyObjectQType());
}

absl::StatusOr<PyObjectPtr> GetPyObjectValue(TypedRef qvalue) {
  RETURN_IF_ERROR(AssertPyObjectQValue(qvalue));
  const auto& wrapped_py_obj = qvalue.UnsafeAs<WrappedPyObject>();
  DCHECK_NE(wrapped_py_obj.GetObject().get(), nullptr);
  return PyObjectPtr::NewRef(wrapped_py_obj.GetObject().get());
}

absl::StatusOr<std::optional<std::string>> GetPyObjectCodec(TypedRef qvalue) {
  RETURN_IF_ERROR(AssertPyObjectQValue(qvalue));
  return qvalue.UnsafeAs<WrappedPyObject>().GetCodec();
}

}  // namespace arolla::python
