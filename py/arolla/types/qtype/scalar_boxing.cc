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
#include "py/arolla/types/qtype/scalar_boxing.h"

#include <Python.h>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <type_traits>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/qtype/weak_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/meta.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace arolla::python {
namespace {

using QValueConverters =
    absl::flat_hash_map<QTypePtr, PyObject* (*)(const TypedValue&)>;

// Unboxing functions.
// go/keep-sorted start block=yes newline_separated=yes
// def py_boolean(x, /) -> bool|None
PyObject* PyValueBoolean(PyObject* /*self*/, PyObject* py_arg) {
  if (py_arg == Py_None || py_arg == Py_True || py_arg == Py_False) {
    return Py_NewRef(py_arg);
  }
  if (IsPyQValueInstance(py_arg)) {
    const auto& qvalue = UnsafeUnwrapPyQValue(py_arg);
    const auto qtype = qvalue.GetType();
    if (qtype == GetQType<bool>()) {
      return PyBool_FromLong(qvalue.UnsafeAs<bool>());
    }
    if (qtype == GetOptionalQType<bool>()) {
      const auto& value = qvalue.UnsafeAs<OptionalValue<bool>>();
      if (!value.present) {
        Py_RETURN_NONE;
      }
      return PyBool_FromLong(value.value);
    }
  }
  // Parse a numpy-boolean-like scalar, without explicitly referring numpy.
  if (PyObject_CheckBuffer(py_arg)) {
    Py_buffer view;
    if (PyObject_GetBuffer(py_arg, &view, PyBUF_CONTIG_RO | PyBUF_FORMAT) < 0) {
      return nullptr;
    }
    auto cleanup = absl::MakeCleanup([&view] { PyBuffer_Release(&view); });
    if (view.ndim == 0 && view.format == absl::string_view("?")) {
      DCHECK_EQ(view.len, sizeof(bool));
      return PyBool_FromLong(*reinterpret_cast<const bool*>(view.buf));
    }
  }
  return PyErr_Format(PyExc_TypeError,
                      "'%s' object cannot be interpreted as a boolean",
                      Py_TYPE(py_arg)->tp_name);
}

// def py_bytes(x, /) -> bytes|None
PyObject* PyValueBytes(PyObject* /*self*/, PyObject* py_arg) {
  if (py_arg == Py_None) {
    Py_RETURN_NONE;
  }
  if (IsPyQValueInstance(py_arg)) {
    const auto& qvalue = UnsafeUnwrapPyQValue(py_arg);
    const auto qtype = qvalue.GetType();
    if (qtype == GetQType<Bytes>()) {
      absl::string_view value = qvalue.UnsafeAs<Bytes>();
      return PyBytes_FromStringAndSize(value.data(),
                                       value.size());
    }
    if (qtype == GetOptionalQType<Bytes>()) {
      const auto& value = qvalue.UnsafeAs<OptionalValue<Bytes>>();
      if (!value.present) {
        Py_RETURN_NONE;
      }
      absl::string_view value_view = value.value;
      return PyBytes_FromStringAndSize(value_view.data(),
                                       value_view.size());
    }
  }
  if (PyBytes_Check(py_arg)) {
    return Py_NewRef(py_arg);
  }
  // Call the method arg.__bytes__() to perform the type conversion.
  // Note: We avoid calling `PyObject_Bytes()` because it supports conversions
  // from many more types, like `int`.
  static auto* py_str_method_name = PyUnicode_InternFromString("__bytes__");
  auto py_member =
      PyType_LookupMemberOrNull(Py_TYPE(py_arg), py_str_method_name);
  if (py_member == nullptr) {
    return PyErr_Format(PyExc_TypeError,
                        "'%s' object cannot be interpreted as bytes",
                        Py_TYPE(py_arg)->tp_name);
  }
  return PyObject_VectorcallMember(std::move(py_member), &py_arg, 1, nullptr)
      .release();
}

// def py_float(x, /) -> float|None
PyObject* PyValueFloat(PyObject* /*self*/, PyObject* py_arg) {
  constexpr auto gen_scalar_converter = [](auto meta_type) {
    using T = typename decltype(meta_type)::type;
    return [](const TypedValue& qvalue) {
      return PyFloat_FromDouble(qvalue.UnsafeAs<T>());
    };
  };
  constexpr auto gen_optional_converter = [](auto meta_type) {
    using T = typename decltype(meta_type)::type;
    return [](const TypedValue& qvalue) {
      const auto& value = qvalue.UnsafeAs<OptionalValue<T>>();
      if (!value.present) {
        Py_RETURN_NONE;
      }
      return PyFloat_FromDouble(value.value);
    };
  };
  static const absl::NoDestructor qvalue_converters(QValueConverters{
      {GetQType<float>(), gen_scalar_converter(meta::type<float>())},
      {GetQType<double>(), gen_scalar_converter(meta::type<double>())},
      {GetWeakFloatQType(), gen_scalar_converter(meta::type<double>())},
      {GetQType<int32_t>(), gen_scalar_converter(meta::type<int32_t>())},
      {GetQType<int64_t>(), gen_scalar_converter(meta::type<int64_t>())},
      {GetQType<uint64_t>(), gen_scalar_converter(meta::type<uint64_t>())},
      {GetOptionalQType<float>(), gen_optional_converter(meta::type<float>())},
      {GetOptionalQType<double>(),
       gen_optional_converter(meta::type<double>())},
      {GetOptionalWeakFloatQType(),
       gen_optional_converter(meta::type<double>())},
      {GetOptionalQType<int32_t>(),
       gen_optional_converter(meta::type<int32_t>())},
      {GetOptionalQType<int64_t>(),
       gen_optional_converter(meta::type<int64_t>())},
      {GetOptionalQType<uint64_t>(),
       gen_optional_converter(meta::type<uint64_t>())}});
  if (py_arg == Py_None) {
    Py_RETURN_NONE;
  }
  if (IsPyQValueInstance(py_arg)) {
    const auto& qvalue = UnsafeUnwrapPyQValue(py_arg);
    auto it = qvalue_converters->find(qvalue.GetType());
    if (it != qvalue_converters->end()) {
      return it->second(qvalue);
    }
  }
  if (PyFloat_Check(py_arg)) {
    return Py_NewRef(py_arg);
  }
  // Use the public Python C API to convert the argument to `PyFloat`. If this
  // code becomes a bottleneck, consider directly using
  // `Py_TYPE(py_arg)->tp_as_number`.
  // Note: We avoid calling `PyNumber_Float()` because it supports conversions
  // from many more types, like `str`.
  auto value = PyFloat_AsDouble(py_arg);
  if (value == -1 && PyErr_Occurred()) {
    return nullptr;
  }
  return PyFloat_FromDouble(value);
}

// def py_index(x, /) -> int|None
PyObject* PyValueIndex(PyObject* /*self*/, PyObject* py_arg) {
  static const absl::NoDestructor qvalue_converters(QValueConverters{
      {GetQType<int32_t>(),
       [](const TypedValue& qvalue) {
         return PyLong_FromLong(qvalue.UnsafeAs<int32_t>());
       }},
      {GetQType<int64_t>(),
       [](const TypedValue& qvalue) {
         return PyLong_FromLongLong(qvalue.UnsafeAs<int64_t>());
       }},
      {GetQType<uint64_t>(),
       [](const TypedValue& qvalue) {
         return PyLong_FromUnsignedLongLong(qvalue.UnsafeAs<uint64_t>());
       }},
      {GetOptionalQType<int32_t>(),
       [](const TypedValue& qvalue) {
         if (auto value = qvalue.UnsafeAs<OptionalValue<int32_t>>()) {
           return PyLong_FromLong(value.value);
         }
         Py_RETURN_NONE;
       }},
      {GetOptionalQType<int64_t>(),
       [](const TypedValue& qvalue) {
         if (auto value = qvalue.UnsafeAs<OptionalValue<int64_t>>()) {
           return PyLong_FromLongLong(value.value);
         }
         Py_RETURN_NONE;
       }},
      {GetOptionalQType<uint64_t>(),
       [](const TypedValue& qvalue) {
         if (auto value = qvalue.UnsafeAs<OptionalValue<uint64_t>>()) {
           return PyLong_FromUnsignedLongLong(value.value);
         }
         Py_RETURN_NONE;
       }},
  });
  if (py_arg == Py_None) {
    Py_RETURN_NONE;
  }
  if (IsPyQValueInstance(py_arg)) {
    const auto& qvalue = UnsafeUnwrapPyQValue(py_arg);
    auto it = qvalue_converters->find(qvalue.GetType());
    if (it != qvalue_converters->end()) {
      return it->second(qvalue);
    }
  }
  return PyNumber_Index(py_arg);
}

// def py_text(x, /) -> str|None
PyObject* PyValueText(PyObject* /*self*/, PyObject* py_arg) {
  if (py_arg == Py_None) {
    Py_RETURN_NONE;
  }
  if (IsPyQValueInstance(py_arg)) {
    const auto& qvalue = UnsafeUnwrapPyQValue(py_arg);
    const auto qtype = qvalue.GetType();
    if (qtype == GetQType<Text>()) {
      const auto& value = qvalue.UnsafeAs<Text>();
      return PyUnicode_FromStringAndSize(value.view().data(),
                                         value.view().size());
    }
    if (qtype == GetOptionalQType<Text>()) {
      const auto& value = qvalue.UnsafeAs<OptionalValue<Text>>();
      if (!value.present) {
        Py_RETURN_NONE;
      }
      return PyUnicode_FromStringAndSize(value.value.view().data(),
                                         value.value.view().size());
    }
  }
  if (PyUnicode_Check(py_arg)) {
    return Py_NewRef(py_arg);
  }
  return PyErr_Format(PyExc_TypeError,
                      "'%s' object cannot be interpreted as a text",
                      Py_TYPE(py_arg)->tp_name);
}

// def py_unit(x, /) -> bool|None
PyObject* PyValueUnit(PyObject* /*self*/, PyObject* py_arg) {
  if (py_arg == Py_None || py_arg == Py_True) {
    return Py_NewRef(py_arg);
  }
  if (IsPyQValueInstance(py_arg)) {
    const auto& qvalue = UnsafeUnwrapPyQValue(py_arg);
    const auto qtype = qvalue.GetType();
    if (qtype == GetQType<Unit>()) {
      Py_RETURN_TRUE;
    }
    if (qtype == GetOptionalQType<Unit>()) {
      const auto& value = qvalue.UnsafeAs<OptionalValue<Unit>>();
      if (!value.present) {
        Py_RETURN_NONE;
      }
      Py_RETURN_TRUE;
    }
  }
  if (py_arg == Py_False) {
    return PyErr_Format(PyExc_ValueError, "%R cannot be interpreted as a unit",
                        py_arg);
  }
  return PyErr_Format(PyExc_TypeError,
                      "'%s' object cannot be interpreted as a unit",
                      Py_TYPE(py_arg)->tp_name);
}
// go/keep-sorted end

template <typename T>
struct ScalarTraitsBase {
  static auto MakeQValue(T&& value) {
    return TypedValue::FromValue(std::move(value));
  }
  static auto MakeQValue(std::optional<T>&& value) {
    return TypedValue::FromValue(OptionalValue<T>(std::move(value)));
  }
};

// go/keep-sorted start block=yes newline_separated=yes
struct BooleanTraits : ScalarTraitsBase<bool> {
  static constexpr char const* const kTypeErrorFmt =
      "'%s' object cannot be interpreted as a boolean";
  static auto ParsePyValue(PyObject* py_arg) { return ParsePyBoolean(py_arg); }
};

struct BytesTraits : ScalarTraitsBase<Bytes> {
  static constexpr char const* const kTypeErrorFmt =
      "'%s' object cannot be interpreted as bytes";
  static auto ParsePyValue(PyObject* py_arg) { return ParsePyBytes(py_arg); }
};

struct Float32Traits : ScalarTraitsBase<float> {
  static constexpr char const* const kTypeErrorFmt =
      "must be real number, not %s";
  static auto ParsePyValue(PyObject* py_arg) { return ParsePyFloat(py_arg); }
};

struct Float64Traits : ScalarTraitsBase<double> {
  static constexpr char const* const kTypeErrorFmt =
      "must be real number, not %s";
  static auto ParsePyValue(PyObject* py_arg) { return ParsePyFloat(py_arg); }
};

struct Int32Traits : ScalarTraitsBase<int32_t> {
  static constexpr char const* const kTypeErrorFmt =
      "'%s' object cannot be interpreted as an integer";
  static auto ParsePyValue(PyObject* py_arg) { return ParsePyInt32(py_arg); }
};

struct Int64Traits : ScalarTraitsBase<int64_t> {
  static constexpr char const* const kTypeErrorFmt =
      "'%s' object cannot be interpreted as an integer";
  static auto ParsePyValue(PyObject* py_arg) { return ParsePyInt64(py_arg); }
};

struct TextTraits : ScalarTraitsBase<Text> {
  static constexpr char const* const kTypeErrorFmt =
      "'%s' object cannot be interpreted as a text";
  static auto ParsePyValue(PyObject* py_arg) { return ParsePyText(py_arg); }
};

struct UInt64Traits : ScalarTraitsBase<uint64_t> {
  static constexpr char const* const kTypeErrorFmt =
      "'%s' object cannot be interpreted as an integer";
  static auto ParsePyValue(PyObject* py_arg) { return ParsePyUInt64(py_arg); }
};

struct UnitTraits : ScalarTraitsBase<Unit> {
  static auto ParsePyValue(PyObject* py_arg) { return ParsePyUnit(py_arg); }
};

struct WeakFloatTraits {
  static constexpr char const* const kTypeErrorFmt =
      "must be real number, not %s";
  static auto ParsePyValue(PyObject* py_arg) { return ParsePyFloat(py_arg); }
  static auto MakeQValue(double&& value) {
    return *TypedValue::FromValueWithQType<double>(value, GetWeakFloatQType());
  }
  static auto MakeQValue(std::optional<double>&& value) {
    return *TypedValue::FromValueWithQType(OptionalValue<double>(value),
                                           GetOptionalWeakFloatQType());
  }
};
// go/keep-sorted end

// def T(x, /) -> QValue
template <typename ScalarTraits>
PyObject* PyScalarT(PyObject* /*self*/, PyObject* py_arg) {
  if (py_arg == Py_None) {
    return PyErr_Format(PyExc_TypeError, ScalarTraits::kTypeErrorFmt,
                        Py_TYPE(py_arg)->tp_name);
  }
  auto result = ScalarTraits::ParsePyValue(py_arg);
  if (!result.has_value()) {
    if (!PyErr_Occurred()) {
      PyErr_SetNone(PyExc_MissingOptionalError);
    }
    return nullptr;
  }
  return WrapAsPyQValue(ScalarTraits::MakeQValue(*std::move(result)));
}

// def optional_T(x, /) -> QValue
template <typename ScalarTraits>
PyObject* PyOptionalT(PyObject* /*self*/, PyObject* py_arg) {
  auto result = ScalarTraits::ParsePyValue(py_arg);
  if (!result.has_value() && PyErr_Occurred()) {
    return nullptr;
  }
  return WrapAsPyQValue(ScalarTraits::MakeQValue(std::move(result)));
}

// def unit() -> QValue
PyObject* PyUnit(PyObject* /*self*/, PyObject* /*py_arg*/) {
  return WrapAsPyQValue(TypedValue::FromValue(Unit()));
}

}  // namespace

PyObject* PyExc_MissingOptionalError;

bool InitScalarBoxing() {
  DCheckPyGIL();
  if (PyExc_MissingOptionalError == nullptr) {
    PyExc_MissingOptionalError = PyErr_NewExceptionWithDoc(
        "arolla.types.MissingOptionalError",
        "Indicates that an optional value is unexpectedly missing.",
        PyExc_ValueError, nullptr);
  }
  return PyExc_MissingOptionalError != nullptr;
}

// go/keep-sorted start block=yes newline_separated=yes
std::optional<Bytes> ParsePyBytes(PyObject* py_arg) {
  auto py_bytes = PyObjectPtr::Own(PyValueBytes(nullptr, py_arg));
  if (py_bytes == nullptr || py_bytes.get() == Py_None) {
    return std::nullopt;
  }
  char* data = nullptr;
  Py_ssize_t size = -1;
  if (PyBytes_AsStringAndSize(py_bytes.get(), &data, &size) < 0) {
    return std::nullopt;
  }
  return Bytes(absl::string_view(data, size));
}

std::optional<Text> ParsePyText(PyObject* py_arg) {
  auto py_str = PyObjectPtr::Own(PyValueText(nullptr, py_arg));
  if (py_str == nullptr || py_str.get() == Py_None) {
    return std::nullopt;
  }
  Py_ssize_t size = -1;
  const char* data = PyUnicode_AsUTF8AndSize(py_str.get(), &size);
  if (data == nullptr) {
    return std::nullopt;
  }
  return Text(absl::string_view(data, size));
}

std::optional<Unit> ParsePyUnit(PyObject* py_arg) {
  auto py_bool = PyObjectPtr::Own(PyValueUnit(nullptr, py_arg));
  if (py_bool == nullptr || py_bool.get() == Py_None) {
    return std::nullopt;
  }
  return Unit{};
}

std::optional<bool> ParsePyBoolean(PyObject* py_arg) {
  auto py_bool = PyObjectPtr::Own(PyValueBoolean(nullptr, py_arg));
  if (py_bool == nullptr || py_bool.get() == Py_None) {
    return std::nullopt;
  }
  return py_bool.get() == Py_True;
}

std::optional<double> ParsePyFloat(PyObject* py_arg) {
  auto py_float = PyObjectPtr::Own(PyValueFloat(nullptr, py_arg));
  if (py_float == nullptr || py_float.get() == Py_None) {
    return std::nullopt;
  }
  double result = PyFloat_AsDouble(py_float.get());
  if (result == -1 && PyErr_Occurred()) {
    // Note: This shouldn't happen in practice since we work with a normalized
    // py_float.
    return std::nullopt;
  }
  return result;
}

std::optional<int32_t> ParsePyInt32(PyObject* py_arg) {
  auto py_long = PyObjectPtr::Own(PyValueIndex(nullptr, py_arg));
  if (py_long == nullptr || py_long.get() == Py_None) {
    return std::nullopt;
  }
  int overflow = 0;
  auto result = PyLong_AsLongAndOverflow(py_long.get(), &overflow);
  if (result == -1 && PyErr_Occurred()) {
    // Note: This shouldn't happen in practice since we work with a normalized
    // py_long.
    return std::nullopt;
  }
  // Ensure the value range if `long` != `int32_t`.
  if constexpr (!std::is_same_v<decltype(result), int32_t>) {
    static_assert(sizeof(result) >= sizeof(int32_t));
    if (overflow == 0) {
      if (result < std::numeric_limits<int32_t>::min()) {
        overflow = -1;
      } else if (result > std::numeric_limits<int32_t>::max()) {
        overflow = 1;
      }
    }
  }
  if (overflow) {
    PyErr_Format(PyExc_OverflowError,
                 "%R does not fit into 32-bit integer type", py_arg);
    return std::nullopt;
  }
  return result;
}

std::optional<int64_t> ParsePyInt64(PyObject* py_arg) {
  auto py_long = PyObjectPtr::Own(PyValueIndex(nullptr, py_arg));
  if (py_long == nullptr || py_long.get() == Py_None) {
    return std::nullopt;
  }
  int overflow = 0;
  auto result = PyLong_AsLongLongAndOverflow(py_long.get(), &overflow);
  if (result == -1 && PyErr_Occurred()) {
    // Note: This shouldn't happen in practice since we work with a normalized
    // py_long.
    return std::nullopt;
  }
  // Ensure the value range if `long long` != `int64_t`.
  if constexpr (!std::is_same_v<decltype(result), int64_t>) {
    static_assert(sizeof(result) >= sizeof(int64_t));
    if (overflow == 0) {
      if (result < std::numeric_limits<int64_t>::min()) {
        overflow = -1;
      } else if (result > std::numeric_limits<int64_t>::max()) {
        overflow = 1;
      }
    }
  }
  if (overflow) {
    PyErr_Format(PyExc_OverflowError,
                 "%R does not fit into 64-bit integer type", py_arg);
    return std::nullopt;
  }
  return result;
}

std::optional<uint64_t> ParsePyUInt64(PyObject* py_arg) {
  bool overflow = false;
  auto py_long = PyObjectPtr::Own(PyValueIndex(nullptr, py_arg));
  if (py_long == nullptr || py_long.get() == Py_None) {
    return std::nullopt;
  }
  auto result = PyLong_AsUnsignedLongLong(py_long.get());
  if (result == -1 && PyErr_Occurred()) {
    if (!PyErr_ExceptionMatches(PyExc_OverflowError)) {
      // Note: This shouldn't happen in practice since we work with a normalized
      // py_long.
      return std::nullopt;
    }
    PyErr_Clear();
    overflow = true;
  }
  // Ensure the value range if `unsigned long long` != `uint64_t`.
  if constexpr (!std::is_same_v<decltype(result), uint64_t>) {
    static_assert(sizeof(result) >= sizeof(uint64_t));
    if (!overflow) {
      if (result > std::numeric_limits<uint64_t>::max()) {
        overflow = true;
      }
    }
  }
  if (overflow) {
    PyErr_Format(PyExc_OverflowError,
                 "%R does not fit into 64-bit unsigned integer type", py_arg);
    return std::nullopt;
  }
  return result;
}
// go/keep-sorted end

// go/keep-sorted start block=yes newline_separated=yes
const PyMethodDef kDefPyValueBoolean = {
    "py_unit",
    &PyValueUnit,
    METH_O,
    ("py_unit(x, /)\n"
     "--\n\n"
     "Return True if `x` represents `unit`, otherwise raises TypeError."),
};

const PyMethodDef kDefPyValueBytes = {
    "py_bytes",
    &PyValueBytes,
    METH_O,
    ("py_bytes(x, /)\n"
     "--\n\n"
     "Return bytes, if `x` is bytes-like, otherwise raises TypeError."),
};

const PyMethodDef kDefPyValueFloat = {
    "py_float",
    &PyValueFloat,
    METH_O,
    ("py_float(x, /)\n"
     "--\n\n"
     "Return float, if `x` is float-like, otherwise raises TypeError."),
};

const PyMethodDef kDefPyValueIndex = {
    "py_index",
    &PyValueIndex,
    METH_O,
    ("py_index(x, /)\n"
     "--\n\n"
     "Return int, if `x` is int-like, otherwise raises TypeError."),
};

const PyMethodDef kDefPyValueText = {
    "py_text",
    &PyValueText,
    METH_O,
    ("py_text(x, /)\n"
     "--\n\n"
     "Return str if `x` is text-like, otherwise raises TypeError."),
};

const PyMethodDef kDefPyValueUnit = {
    "py_boolean",
    &PyValueBoolean,
    METH_O,
    ("py_boolean(x, /)\n"
     "--\n\n"
     "Return bool, if `x` is boolean-like, otherwise raises TypeError."),
};
// go/keep-sorted end

// go/keep-sorted start block=yes newline_separated=yes
const PyMethodDef kDefPyBoolean = {
    "boolean",
    &PyScalarT<BooleanTraits>,
    METH_O,
    ("boolean(x, /)\n"
     "--\n\n"
     "Returns BOOLEAN qvalue."),
};

const PyMethodDef kDefPyBytes = {
    "bytes",
    &PyScalarT<BytesTraits>,
    METH_O,
    ("bytes(x, /)\n"
     "--\n\n"
     "Returns BYTES qvalue."),
};

const PyMethodDef kDefPyFloat32 = {
    "float32",
    &PyScalarT<Float32Traits>,
    METH_O,
    ("float32(x, /)\n"
     "--\n\n"
     "Returns FLOAT32 qvalue."),
};

const PyMethodDef kDefPyFloat64 = {
    "float64",
    &PyScalarT<Float64Traits>,
    METH_O,
    ("float64(x, /)\n"
     "--\n\n"
     "Returns FLOAT64 qvalue."),
};

const PyMethodDef kDefPyInt32 = {
    "int32",
    &PyScalarT<Int32Traits>,
    METH_O,
    ("int32(x, /)\n"
     "--\n\n"
     "Returns INT32 qvalue."),
};

const PyMethodDef kDefPyInt64 = {
    "int64",
    &PyScalarT<Int64Traits>,
    METH_O,
    ("int64(x, /)\n"
     "--\n\n"
     "Returns INT64 qvalue."),
};

const PyMethodDef kDefPyText = {
    "text",
    &PyScalarT<TextTraits>,
    METH_O,
    ("text(x, /)\n"
     "--\n\n"
     "Returns TEXT qvalue."),
};

const PyMethodDef kDefPyUInt64 = {
    "uint64",
    &PyScalarT<UInt64Traits>,
    METH_O,
    ("uint64(x, /)\n"
     "--\n\n"
     "Returns UINT64 qvalue."),
};

const PyMethodDef kDefPyUnit = {
    "unit",
    &PyUnit,
    METH_NOARGS,
    ("unit()\n"
     "--\n\n"
     "Returns UNIT qvalue."),
};

const PyMethodDef kDefPyWeakFloat = {
    "weak_float",
    &PyScalarT<WeakFloatTraits>,
    METH_O,
    ("weak_float(x, /)\n"
     "--\n\n"
     "Returns WEAK_FLOAT qvalue."),
};
// go/keep-sorted end

// go/keep-sorted start block=yes newline_separated=yes
const PyMethodDef kDefPyOptionalBoolean = {
    "optional_boolean",
    &PyOptionalT<BooleanTraits>,
    METH_O,
    ("optional_boolean(x, /)\n"
     "--\n\n"
     "Returns OPTIONAL_BOOLEAN qvalue."),
};

const PyMethodDef kDefPyOptionalBytes = {
    "optional_bytes",
    &PyOptionalT<BytesTraits>,
    METH_O,
    ("optional_bytes(x, /)\n"
     "--\n\n"
     "Returns OPTIONAL_BYTES qvalue."),
};

const PyMethodDef kDefPyOptionalFloat32 = {
    "optional_float32",
    &PyOptionalT<Float32Traits>,
    METH_O,
    ("optional_float32(x, /)\n"
     "--\n\n"
     "Returns OPTIONAL_FLOAT32 qvalue."),
};

const PyMethodDef kDefPyOptionalFloat64 = {
    "optional_float64",
    &PyOptionalT<Float64Traits>,
    METH_O,
    ("optional_float64(x, /)\n"
     "--\n\n"
     "Returns OPTIONAL_FLOAT64 qvalue."),
};

const PyMethodDef kDefPyOptionalInt32 = {
    "optional_int32",
    &PyOptionalT<Int32Traits>,
    METH_O,
    ("optional_int32(x, /)\n"
     "--\n\n"
     "Returns INT32 qvalue."),
};

const PyMethodDef kDefPyOptionalInt64 = {
    "optional_int64",
    &PyOptionalT<Int64Traits>,
    METH_O,
    ("optional_int64(x, /)\n"
     "--\n\n"
     "Returns OPTIONAL_INT64 qvalue."),
};

const PyMethodDef kDefPyOptionalText = {
    "optional_text",
    &PyOptionalT<TextTraits>,
    METH_O,
    ("optional_text(x, /)\n"
     "--\n\n"
     "Returns OPTIONAL_TEXT qvalue."),
};

const PyMethodDef kDefPyOptionalUInt64 = {
    "optional_uint64",
    &PyOptionalT<UInt64Traits>,
    METH_O,
    ("optional_uint64(x, /)\n"
     "--\n\n"
     "Returns OPTIONAL_UINT64 qvalue."),
};

const PyMethodDef kDefPyOptionalUnit = {
    "optional_unit",
    &PyOptionalT<UnitTraits>,
    METH_O,
    ("optional_unit(x,/)\n"
     "--\n\n"
     "Returns OPTIONAL_UNIT qvalue."),
};

const PyMethodDef kDefPyOptionalWeakFloat = {
    "optional_weak_float",
    &PyOptionalT<WeakFloatTraits>,
    METH_O,
    ("optional_weak_float(x, /)\n"
     "--\n\n"
     "Returns OPTIONAL_WEAK_FLOAT qvalue."),
};
// go/keep-sorted end

}  // namespace arolla::python
