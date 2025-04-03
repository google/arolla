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
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/refcount_ptr.h"
#include "arolla/util/repr.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::python {
namespace {

// Shortens registered codecs, and leaves others untouched.
std::string GetShortenedCodec(absl::string_view codec) {
  constexpr absl::string_view registered_codec_prefix =
      "py_obj_codec:arolla.s11n.py_object_codec.registry.";
  if (registered_codec_prefix ==
      codec.substr(0, registered_codec_prefix.length())) {
    return absl::StrCat("<registered> ",
                        codec.substr(registered_codec_prefix.length()));
  } else {
    return std::string(codec);
  }
}

// Represents a PyObject.
struct WrappedPyObject : RefcountedBase {
  PyObjectGILSafePtr py_object;
  std::optional<std::string> codec;
  Fingerprint uuid = RandomFingerprint();
};

using WrappedPyObjectPtr = RefcountPtr<const WrappedPyObject>;

class PyObjectQType final : public QType {
 public:
  PyObjectQType()
      : QType(ConstructorArgs{
            .name = "PY_OBJECT",
            .type_info = typeid(WrappedPyObjectPtr),
            .type_layout = MakeTypeLayout<WrappedPyObjectPtr>(),
        }) {}

  ReprToken UnsafeReprToken(const void* source) const final {
    AcquirePyGIL gil_acquire;
    const WrappedPyObjectPtr& wrapped_py_object =
        *static_cast<const WrappedPyObjectPtr*>(source);
    if (wrapped_py_object == nullptr ||
        wrapped_py_object->py_object == nullptr) {
      return ReprToken{"PyObject{nullptr}"};
    }
    auto py_str =
        PyObjectPtr::Own(PyObject_Repr(wrapped_py_object->py_object.get()));
    if (py_str == nullptr) {
      PyErr_Print();
      return ReprToken{"PyObject{unknown error occurred}"};
    }
    Py_ssize_t str_size;
    const char* str = PyUnicode_AsUTF8AndSize(py_str.get(), &str_size);
    if (str == nullptr) {
      PyErr_Print();
      return ReprToken{"PyObject{unknown error occurred}"};
    }
    if (wrapped_py_object->codec.has_value()) {
      return ReprToken{absl::StrFormat(
          "PyObject{%s, codec=b'%s'}", absl::string_view(str, str_size),
          absl::CHexEscape(GetShortenedCodec(*wrapped_py_object->codec)))};
    } else {
      return ReprToken{
          absl::StrFormat("PyObject{%s}", absl::string_view(str, str_size))};
    }
  }

  void UnsafeCopy(const void* source, void* destination) const final {
    if (source != destination) {
      *static_cast<WrappedPyObjectPtr*>(destination) =
          *static_cast<const WrappedPyObjectPtr*>(source);
    }
  }

  void UnsafeCombineToFingerprintHasher(const void* source,
                                        FingerprintHasher* hasher) const final {
    const WrappedPyObjectPtr& wrapped_py_object =
        *static_cast<const WrappedPyObjectPtr*>(source);
    // TODO: Introduce reproducible fingerprinting based on the
    // ideas in:
    // https://docs.google.com/document/d/1G53kbUfBTmzIrl9EsNAhk2Z_SDaVE8APQbzLY35moQA
    if (wrapped_py_object != nullptr) {
      hasher->Combine(wrapped_py_object->uuid);
    }
  }
};

absl::Status AssertPyObjectQValue(TypedRef value) {
  if (value.GetType() != GetPyObjectQType()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected %s, got %s", GetPyObjectQType()->name(),
                        value.GetType()->name()));
  }
  return absl::OkStatus();
}

}  // namespace

QTypePtr GetPyObjectQType() {
  static const absl::NoDestructor<PyObjectQType> result;
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
  auto wrapped_py_object = std::make_unique<WrappedPyObject>();
  wrapped_py_object->py_object = PyObjectGILSafePtr::Own(obj.release());
  wrapped_py_object->codec = std::move(codec);
  return TypedValue::FromValueWithQType(
      WrappedPyObjectPtr::Own(std::move(wrapped_py_object)),
      GetPyObjectQType());
}

absl::StatusOr<PyObjectPtr> GetPyObjectValue(TypedRef qvalue) {
  DCheckPyGIL();
  RETURN_IF_ERROR(AssertPyObjectQValue(qvalue));
  const auto& wrapped_py_object = qvalue.UnsafeAs<WrappedPyObjectPtr>();
  if (wrapped_py_object == nullptr || wrapped_py_object->py_object == nullptr) {
    return absl::InvalidArgumentError(
        "wrappedPyObject has a non-fully initialized state");
  }
  return PyObjectPtr::NewRef(wrapped_py_object->py_object.get());
}

absl::StatusOr<std::optional<std::string>> GetPyObjectCodec(TypedRef qvalue) {
  RETURN_IF_ERROR(AssertPyObjectQValue(qvalue));
  const auto& wrapped_py_object = qvalue.UnsafeAs<WrappedPyObjectPtr>();
  if (wrapped_py_object == nullptr || wrapped_py_object->py_object == nullptr) {
    return absl::InvalidArgumentError(
        "wrappedPyObject has a non-fully initialized state");
  }
  return wrapped_py_object->codec;
}

}  // namespace arolla::python
