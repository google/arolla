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
#include "py/arolla/types/qtype/array_boxing.h"

#include <Python.h>

#include <cstdint>
#include <optional>

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/arolla/types/qtype/scalar_boxing.h"
#include "arolla/array/array.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/qtype/weak_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/meta.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/view_types.h"

namespace arolla::python {
namespace {

template <typename T>
struct ArrayTraitsBase {
  using value_type = T;
  static auto array_qtype() { return GetArrayQType<T>(); }
  static auto dense_array_qtype() { return GetDenseArrayQType<T>(); }
  static auto MakeQValue(OptionalValue<T>&& x) {
    return TypedValue::FromValue(std::move(x));
  }
  static auto MakeQValue(DenseArray<T>&& x) {
    return TypedValue::FromValue(std::move(x));
  }
};

// go/keep-sorted start block=yes newline_separated=yes
struct ArrayBooleanTraits : ArrayTraitsBase<bool> {
  static auto ParsePyScalar(PyObject* py_arg) { return ParsePyBoolean(py_arg); }
  static auto MakePyScalar(bool x) { return PyBool_FromLong(x); }
};

struct ArrayBytesTraits : ArrayTraitsBase<Bytes> {
  static auto ParsePyScalar(PyObject* py_arg) { return ParsePyBytes(py_arg); }
  static auto MakePyScalar(absl::string_view x) {
    return PyBytes_FromStringAndSize(x.data(), x.size());
  }
};

struct ArrayFloat32Traits : ArrayTraitsBase<float> {
  static auto ParsePyScalar(PyObject* py_arg) { return ParsePyFloat(py_arg); }
  static auto MakePyScalar(float x) { return PyFloat_FromDouble(x); }
};

struct ArrayFloat64Traits : ArrayTraitsBase<double> {
  static auto ParsePyScalar(PyObject* py_arg) { return ParsePyFloat(py_arg); }
  static auto MakePyScalar(double x) { return PyFloat_FromDouble(x); }
};

struct ArrayInt32Traits : ArrayTraitsBase<int32_t> {
  static auto ParsePyScalar(PyObject* py_arg) { return ParsePyInt32(py_arg); }
  static auto MakePyScalar(int32_t x) { return PyLong_FromLong(x); }
};

struct ArrayInt64Traits : ArrayTraitsBase<int64_t> {
  static auto ParsePyScalar(PyObject* py_arg) { return ParsePyInt64(py_arg); }
  static auto MakePyScalar(int64_t x) { return PyLong_FromLongLong(x); }
};

struct ArrayTextTraits : ArrayTraitsBase<Text> {
  static auto ParsePyScalar(PyObject* py_arg) { return ParsePyText(py_arg); }
  static auto MakePyScalar(absl::string_view x) {
    return PyUnicode_FromStringAndSize(x.data(), x.size());
  }
};

struct ArrayUInt64Traits : ArrayTraitsBase<uint64_t> {
  static auto ParsePyScalar(PyObject* py_arg) { return ParsePyUInt64(py_arg); }
  static auto MakePyScalar(uint64_t x) {
    return PyLong_FromUnsignedLongLong(x);
  }
};

struct ArrayUnitTraits : ArrayTraitsBase<Unit> {
  static auto ParsePyScalar(PyObject* py_arg) { return ParsePyUnit(py_arg); }
  static auto MakePyScalar(Unit) { Py_RETURN_TRUE; }
};

struct ArrayWeakFloatTraits {
  using value_type = double;
  static auto array_qtype() { return GetArrayWeakFloatQType(); }
  static auto dense_array_qtype() { return GetDenseArrayWeakFloatQType(); }
  static auto ParsePyScalar(PyObject* py_arg) { return ParsePyFloat(py_arg); }
  static auto MakePyScalar(double x) { return PyFloat_FromDouble(x); }
  static auto MakeQValue(OptionalValue<double>&& value) {
    return *TypedValue::FromValueWithQType(std::move(value),
                                           GetOptionalWeakFloatQType());
  }
  static auto MakeQValue(DenseArray<double>&& value) {
    return *TypedValue::FromValueWithQType(std::move(value),
                                           GetDenseArrayWeakFloatQType());
  }
};
// go/keep-sorted end

using ArrayTraits =
    meta::type_list<ArrayBooleanTraits, ArrayBytesTraits, ArrayFloat32Traits,
                    ArrayFloat64Traits, ArrayInt32Traits, ArrayInt64Traits,
                    ArrayTextTraits, ArrayUInt64Traits, ArrayUnitTraits,
                    ArrayWeakFloatTraits>;

// def dense_array_T_from_values(values, /) -> QValue
template <typename ArrayTraits>
PyObject* PyDenseArrayTFromValues(PyObject* /*self*/, PyObject* py_arg) {
  using T = typename ArrayTraits::value_type;
  auto py_sequence_fast = PyObjectPtr::Own(
      PySequence_Fast(py_arg, "expected a sequence of values"));
  if (py_sequence_fast == nullptr) {
    return nullptr;
  }
  const int64_t size = PySequence_Fast_GET_SIZE(py_sequence_fast.get());
  const absl::Span<PyObject*> py_values(
      PySequence_Fast_ITEMS(py_sequence_fast.get()), size);
  bitmap::AlmostFullBuilder bitmap_builder(size);
  typename Buffer<T>::Builder values_builder(size);
  for (int64_t i = 0; i < size; ++i) {
    if (auto value = ArrayTraits::ParsePyScalar(py_values[i])) {
      values_builder.Set(i, *value);
    } else if (!PyErr_Occurred()) {
      bitmap_builder.AddMissed(i);
    } else {
      return nullptr;
    }
  }
  return WrapAsPyQValue(ArrayTraits::MakeQValue(DenseArray<T>{
      std::move(values_builder).Build(), std::move(bitmap_builder).Build()}));
}

// def get_array_item(array: QValue, i: int, /) -> QValue
PyObject* PyGetArrayItem(PyObject* /*self*/, PyObject** py_args,
                         Py_ssize_t nargs) {
  using GetItemFn = PyObject* (*)(const TypedValue&, int64_t);
  static constexpr auto gen_getitem_fn = [](auto meta_array_traits,
                                            auto meta_array) -> GetItemFn {
    using ArrayTraits = typename decltype(meta_array_traits)::type;
    using T = typename ArrayTraits::value_type;
    using Array = typename decltype(meta_array)::type;
    return [](const TypedValue& qvalue, int64_t i) {
      const auto& array = qvalue.UnsafeAs<Array>();
      if (i < -array.size() || i >= array.size()) {
        return PyErr_Format(PyExc_IndexError, "index out of range: %lld", i);
      }
      if (i < 0) {
        i += array.size();
      }
      return WrapAsPyQValue(
          ArrayTraits::MakeQValue(OptionalValue<T>(array[i])));
    };
  };
  static const absl::NoDestructor getitem_fns([] {
    absl::flat_hash_map<QTypePtr, GetItemFn> result;
    meta::foreach_type<ArrayTraits>([&](auto meta_array_traits) {
      using ArrayTraits = typename decltype(meta_array_traits)::type;
      using T = typename ArrayTraits::value_type;
      result[ArrayTraits::array_qtype()] =
          gen_getitem_fn(meta_array_traits, meta::type<Array<T>>());
      result[ArrayTraits::dense_array_qtype()] =
          gen_getitem_fn(meta_array_traits, meta::type<DenseArray<T>>());
    });
    return result;
  }());
  if (nargs == 0) {
    PyErr_SetString(PyExc_TypeError,
                    "missing 2 required positional arguments: 'array', 'i'");
    return nullptr;
  } else if (nargs == 1) {
    PyErr_SetString(PyExc_TypeError,
                    "missing 1 required positional argument: 'i'");
    return nullptr;
  } else if (nargs > 2) {
    return PyErr_Format(PyExc_TypeError,
                        "expected 2 positional arguments, but %zu were given",
                        nargs);
  }
  // Parse `array`.
  if (!IsPyQValueInstance(py_args[0])) {
    return PyErr_Format(PyExc_TypeError, "expected an array, got %s",
                        Py_TYPE(py_args[0])->tp_name);
  }
  const auto& qvalue_array = UnsafeUnwrapPyQValue(py_args[0]);
  // Parse `i`.
  if (py_args[1] == Py_None) {
    return PyErr_Format(PyExc_TypeError,
                        "'%s' object cannot be interpreted as an integer",
                        Py_TYPE(py_args[1])->tp_name);
  }
  const std::optional<int64_t> i = ParsePyInt64(py_args[1]);
  if (!i.has_value()) {
    if (!PyErr_Occurred()) {
      PyErr_SetNone(PyExc_MissingOptionalError);
    }
    return nullptr;
  }
  auto it = getitem_fns->find(qvalue_array.GetType());
  if (it != getitem_fns->end()) {
    return it->second(qvalue_array, *i);
  }
  return PyErr_Format(PyExc_TypeError, "expected an array, got %s",
                      Py_TYPE(py_args[0])->tp_name);
}

// def get_array_py_value(array: QValue, /) -> list[Any]
PyObject* PyGetArrayPyValue(PyObject* /*self*/, PyObject* py_arg) {
  using PyValueFn = PyObject* (*)(const TypedValue&);
  static constexpr auto gen_py_value_fn = [](auto meta_array_traits,
                                             auto meta_array) -> PyValueFn {
    using ArrayTraits = typename decltype(meta_array_traits)::type;
    using T = typename ArrayTraits::value_type;
    using Array = typename decltype(meta_array)::type;
    return [](const TypedValue& qvalue) -> PyObject* {
      const auto& array = qvalue.UnsafeAs<Array>();
      auto result = PyObjectPtr::Own(PyList_New(array.size()));
      if (result == nullptr) {
        return nullptr;
      }
      bool error = false;
      array.ForEach([&](int64_t i, bool present, view_type_t<T> value) {
        if (error) {  // Fast pass if there was an error.
          return;
        }
        PyObjectPtr py_item;
        if (present) {
          PyList_SET_ITEM(result.get(), i, ArrayTraits::MakePyScalar(value));
          error = (PyList_GET_ITEM(result.get(), i) == nullptr);
        } else {
          PyList_SET_ITEM(result.get(), i, Py_NewRef(Py_None));
        }
      });
      if (error) {
        return nullptr;
      }
      return result.release();
    };
  };
  static const absl::NoDestructor py_value_fns([] {
    absl::flat_hash_map<QTypePtr, PyValueFn> result;
    meta::foreach_type<ArrayTraits>([&](auto meta_array_traits) {
      using ArrayTraits = typename decltype(meta_array_traits)::type;
      using T = typename ArrayTraits::value_type;
      result[ArrayTraits::array_qtype()] =
          gen_py_value_fn(meta_array_traits, meta::type<Array<T>>());
      result[ArrayTraits::dense_array_qtype()] =
          gen_py_value_fn(meta_array_traits, meta::type<DenseArray<T>>());
    });
    return result;
  }());
  if (IsPyQValueInstance(py_arg)) {
    const auto& qvalue = UnsafeUnwrapPyQValue(py_arg);
    auto it = py_value_fns->find(qvalue.GetType());
    if (it != py_value_fns->end()) {
      return it->second(qvalue);
    }
  }
  return PyErr_Format(PyExc_TypeError, "expected an array, got %s",
                      Py_TYPE(py_arg)->tp_name);
}

}  // namespace

[[nodiscard]] bool InitArrayBoxing() {
  // Ensure the [dense_]array qtype registration.
  meta::foreach_type<ArrayTraits>([&](auto meta_array_traits) {
    using ArrayTraits = typename decltype(meta_array_traits)::type;
    ArrayTraits::array_qtype();
    ArrayTraits::dense_array_qtype();
  });
  return InitScalarBoxing();
}

// go/keep-sorted start block=yes newline_separated=yes
const PyMethodDef kDefPyDenseArrayBooleanFromValues = {
    "dense_array_boolean_from_values",
    &PyDenseArrayTFromValues<ArrayBooleanTraits>,
    METH_O,
    ("dense_array_boolean_from_values(values, /)\n"
     "--\n\n"
     "Returns DENSE_ARRAY_BOOLEAN qvalue."),
};

const PyMethodDef kDefPyDenseArrayBytesFromValues = {
    "dense_array_bytes_from_values",
    &PyDenseArrayTFromValues<ArrayBytesTraits>,
    METH_O,
    ("dense_array_bytes_from_values(values, /)\n"
     "--\n\n"
     "Returns DENSE_ARRAY_BYTES qvalue."),
};

const PyMethodDef kDefPyDenseArrayFloat32FromValues = {
    "dense_array_float32_from_values",
    &PyDenseArrayTFromValues<ArrayFloat32Traits>,
    METH_O,
    ("dense_array_float32_from_values(values, /)\n"
     "--\n\n"
     "Returns DENSE_ARRAY_FLOAT32 qvalue."),
};

const PyMethodDef kDefPyDenseArrayFloat64FromValues = {
    "dense_array_float64_from_values",
    &PyDenseArrayTFromValues<ArrayFloat64Traits>,
    METH_O,
    ("dense_array_float64_from_values(values, /)\n"
     "--\n\n"
     "Returns DENSE_ARRAY_FLOAT64 qvalue."),
};

const PyMethodDef kDefPyDenseArrayInt32FromValues = {
    "dense_array_int32_from_values",
    &PyDenseArrayTFromValues<ArrayInt32Traits>,
    METH_O,
    ("dense_array_int32_from_values(values, /)\n"
     "--\n\n"
     "Returns DENSE_ARRAY_INT32 qvalue."),
};

const PyMethodDef kDefPyDenseArrayInt64FromValues = {
    "dense_array_int64_from_values",
    &PyDenseArrayTFromValues<ArrayInt64Traits>,
    METH_O,
    ("dense_array_int64_from_values(values, /)\n"
     "--\n\n"
     "Returns DENSE_ARRAY_INT64 qvalue."),
};

const PyMethodDef kDefPyDenseArrayTextFromValues = {
    "dense_array_text_from_values",
    &PyDenseArrayTFromValues<ArrayTextTraits>,
    METH_O,
    ("dense_array_text_from_values(values, /)\n"
     "--\n\n"
     "Returns DENSE_ARRAY_TEXT qvalue."),
};

const PyMethodDef kDefPyDenseArrayUInt64FromValues = {
    "dense_array_uint64_from_values",
    &PyDenseArrayTFromValues<ArrayUInt64Traits>,
    METH_O,
    ("dense_array_uint64_from_values(values, /)\n"
     "--\n\n"
     "Returns DENSE_ARRAY_UINT64 qvalue."),
};

const PyMethodDef kDefPyDenseArrayUnitFromValues = {
    "dense_array_unit_from_values",
    &PyDenseArrayTFromValues<ArrayUnitTraits>,
    METH_O,
    ("dense_array_unit_from_values(values, /)\n"
     "--\n\n"
     "Returns DENSE_ARRAY_UNIT qvalue."),
};

const PyMethodDef kDefPyDenseArrayWeakFloatFromValues = {
    "dense_array_weak_float_from_values",
    &PyDenseArrayTFromValues<ArrayWeakFloatTraits>,
    METH_O,
    ("dense_array_weak_float_from_values(values, /)\n"
     "--\n\n"
     "Returns DENSE_ARRAY_WEAK_FLOAT qvalue."),
};

const PyMethodDef kDefPyGetArrayItem = {
    "get_array_item",
    reinterpret_cast<PyCFunction>(&PyGetArrayItem),
    METH_FASTCALL,
    ("get_array_item(array, i, /)\n"
     "--\n\n"
     "Returns i-th item of the array."),
};

const PyMethodDef kDefPyGetArrayPyValue = {
    "get_array_py_value",
    &PyGetArrayPyValue,
    METH_O,
    ("get_array_py_value(array, /)\n"
     "--\n\n"
     "Returns a list of python values from array."),
};
// go/keep-sorted end

}  // namespace arolla::python
