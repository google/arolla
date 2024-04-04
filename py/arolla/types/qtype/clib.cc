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
// Python extension module with the boxing utilities for the standard types.
//

#include <cstdint>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/arolla/types/qtype/array_boxing.h"
#include "py/arolla/types/qtype/py_object_boxing.h"
#include "py/arolla/types/qtype/scalar_boxing.h"
#include "pybind11/functional.h"
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"
#include "pybind11_abseil/absl_casters.h"
#include "arolla/array/array.h"
#include "arolla/array/edge.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/buffer.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/dict/dict_types.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/shape_qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/sequence/mutable_sequence.h"
#include "arolla/sequence/sequence_qtype.h"
#include "arolla/util/unit.h"

namespace arolla::python {
namespace {

namespace py = pybind11;

PYBIND11_MODULE(clib, m) {
  if (!InitScalarBoxing() || !InitArrayBoxing()) {
    throw py::error_already_set();
  }

  py::options options;
  options.disable_function_signatures();

  pybind11_module_add_functions<            // go/keep-sorted start
      kDefPyBoolean,                        //
      kDefPyBytes,                          //
      kDefPyDenseArrayBooleanFromValues,    //
      kDefPyDenseArrayBytesFromValues,      //
      kDefPyDenseArrayFloat32FromValues,    //
      kDefPyDenseArrayFloat64FromValues,    //
      kDefPyDenseArrayInt32FromValues,      //
      kDefPyDenseArrayInt64FromValues,      //
      kDefPyDenseArrayTextFromValues,       //
      kDefPyDenseArrayUInt64FromValues,     //
      kDefPyDenseArrayUnitFromValues,       //
      kDefPyDenseArrayWeakFloatFromValues,  //
      kDefPyFloat32,                        //
      kDefPyFloat64,                        //
      kDefPyGetArrayItem,                   //
      kDefPyGetArrayPyValue,                //
      kDefPyInt32,                          //
      kDefPyInt64,                          //
      kDefPyOptionalBoolean,                //
      kDefPyOptionalBytes,                  //
      kDefPyOptionalFloat32,                //
      kDefPyOptionalFloat64,                //
      kDefPyOptionalInt32,                  //
      kDefPyOptionalInt64,                  //
      kDefPyOptionalText,                   //
      kDefPyOptionalUInt64,                 //
      kDefPyOptionalUnit,                   //
      kDefPyOptionalWeakFloat,              //
      kDefPyText,                           //
      kDefPyUInt64,                         //
      kDefPyUnit,                           //
      kDefPyValueBoolean,                   //
      kDefPyValueBytes,                     //
      kDefPyValueFloat,                     //
      kDefPyValueIndex,                     //
      kDefPyValueText,                      //
      kDefPyValueUnit,                      //
      kDefPyWeakFloat                       //
      >(m);                                 // go/keep-sorted end

  m.add_object("MissingOptionalError",
               py::cast<py::type>(PyExc_MissingOptionalError));

  // Register qtypes.
  // go/keep-sorted start block=yes
  m.add_object("ARRAY_EDGE", py::cast(GetQType<ArrayEdge>()));
  m.add_object("ARRAY_SHAPE", py::cast(GetQType<ArrayShape>()));
  m.add_object("ARRAY_TO_SCALAR_EDGE",
               py::cast(GetQType<ArrayGroupScalarEdge>()));
  m.add_object("ARRAY_UNIT", py::cast(GetArrayQType<Unit>()));
  m.add_object("DENSE_ARRAY_EDGE", py::cast(GetQType<DenseArrayEdge>()));
  m.add_object("DENSE_ARRAY_SHAPE", py::cast(GetQType<DenseArrayShape>()));
  m.add_object("DENSE_ARRAY_TO_SCALAR_EDGE",
               py::cast(GetQType<DenseArrayGroupScalarEdge>()));
  m.add_object("OPTIONAL_SCALAR_SHAPE",
               py::cast(GetQType<OptionalScalarShape>()));
  m.add_object("SCALAR_SHAPE", py::cast(GetQType<ScalarShape>()));
  m.add_object("SCALAR_TO_SCALAR_EDGE",
               py::cast(GetQType<ScalarToScalarEdge>()));
  // go/keep-sorted end

  // go/keep-sorted start block=yes newline_separated=yes
  m.def(
      "dense_array_boolean_from_values_buffer",
      [](absl::Span<const bool> buffer) {
        return TypedValue::FromValue(DenseArray<bool>{
            Buffer<bool>::Create(buffer.begin(), buffer.end())});
      },
      py::arg("buffer"), py::pos_only(),
      py::doc("dense_array_boolean_from_values_buffer(buffer, /)\n"
              "--\n\n"
              "Returns DENSE_ARRAY_BOOLEAN qvalue."));

  m.def(
      "dense_array_float32_from_values_buffer",
      [](absl::Span<const float> buffer) {
        return TypedValue::FromValue(DenseArray<float>{
            Buffer<float>::Create(buffer.begin(), buffer.end())});
      },
      py::arg("buffer"), py::pos_only(),
      py::doc("dense_array_float32_from_values_buffer(buffer, /)\n"
              "--\n\n"
              "Returns DENSE_ARRAY_FLOAT32 qvalue."));

  m.def(
      "dense_array_float64_from_values_buffer",
      [](absl::Span<const double> buffer) {
        return TypedValue::FromValue(DenseArray<double>{
            Buffer<double>::Create(buffer.begin(), buffer.end())});
      },
      py::arg("buffer"), py::pos_only(),
      py::doc("dense_array_float64_from_values_buffer(buffer, /)\n"
              "--\n\n"
              "Returns DENSE_ARRAY_FLOAT64 qvalue."));

  m.def(
      "dense_array_int32_from_values_buffer",
      [](absl::Span<const int32_t> buffer) {
        return TypedValue::FromValue(DenseArray<int32_t>{
            Buffer<int32_t>::Create(buffer.begin(), buffer.end())});
      },
      py::arg("buffer"), py::pos_only(),
      py::doc("dense_array_int32_from_values_buffer(buffer, /)\n"
              "--\n\n"
              "Returns DENSE_ARRAY_INT32 qvalue."));

  m.def(
      "dense_array_int64_from_values_buffer",
      [](absl::Span<const int64_t> buffer) {
        return TypedValue::FromValue(DenseArray<int64_t>{
            Buffer<int64_t>::Create(buffer.begin(), buffer.end())});
      },
      py::arg("buffer"), py::pos_only(),
      py::doc("dense_array_int64_from_values_buffer(buffer, /)\n"
              "--\n\n"
              "Returns DENSE_ARRAY_INT64 qvalue."));

  m.def(
      "dense_array_uint64_from_values_buffer",
      [](absl::Span<const uint64_t> buffer) {
        return TypedValue::FromValue(DenseArray<uint64_t>{
            Buffer<uint64_t>::Create(buffer.begin(), buffer.end())});
      },
      py::arg("buffer"), py::pos_only(),
      py::doc("dense_array_uint64_from_values_buffer(buffer, /)\n"
              "--\n\n"
              "Returns DENSE_ARRAY_UINT64 qvalue."));

  m.def(
      "dense_array_weak_float_from_values_buffer",
      [](absl::Span<const double> buffer) {
        return *TypedValue::FromValueWithQType(
            DenseArray<double>{
                Buffer<double>::Create(buffer.begin(), buffer.end())},
            GetDenseArrayWeakFloatQType());
      },
      py::arg("buffer"), py::pos_only(),
      py::doc("dense_array_weak_float_from_values_buffer(buffer, /)\n"
              "--\n\n"
              "Returns DENSE_ARRAY_WEAK_FLOAT qvalue."));

  m.def(
      "get_namedtuple_field_index",
      [](QTypePtr qtype, absl::string_view field_name) {
        return GetFieldIndexByName(qtype, field_name);
      },
      py::arg("qtype"), py::arg("field_name"), py::pos_only(),
      py::doc("get_namedtuple_field_index(qtype, field_name, /)\n"
              "--\n\n"
              "Returns the index of the field by its name, or None if no such "
              "field exists."));

  m.def(
      "get_namedtuple_field_names",
      [](QTypePtr qtype) { return GetFieldNames(qtype); }, py::arg("qtype"),
      py::pos_only(),
      py::doc("get_namedtuple_field_names(qtype, /)\n"
              "--\n\n"
              "Returns the field names of the namedtuple type."));

  m.def(
      "get_nth",
      [](const TypedValue& qvalue, int64_t n) {
        if (n < 0 || n >= qvalue.GetFieldCount()) {
          throw py::index_error(
              absl::StrFormat("field index is out of range: %d", n));
        }
        return TypedValue(qvalue.GetField(n));
      },
      py::arg("qvalue"), py::arg("n"), py::pos_only(),
      py::doc("get_nth(qvalue, n, /)\n"
              "--\n\n"
              "Returns n-th field of the value."));

  m.def(
      "get_py_object_codec",
      [](const TypedValue& value) -> std::optional<py::bytes> {
        return pybind11_unstatus_or(GetPyObjectCodec(value.AsRef()));
      },
      py::arg("value"), py::pos_only(),
      py::doc("get_py_object_codec(value, /)\n"
              "--\n\n"
              "Returns the codec stored in the given PY_OBJECT qvalue."));

  m.def(
      "get_py_object_data",
      [](const TypedValue& value) {
        return py::bytes(pybind11_unstatus_or(EncodePyObject(value.AsRef())));
      },
      py::arg("value"), py::pos_only(),
      py::doc("get_py_object_data(value, /)\n"
              "--\n\n"
              "Returns the serialized data of the object stored in a PY_OBJECT "
              "instance."));

  m.def(
      "internal_make_namedtuple_qtype",
      [](const std::vector<std::string>& field_names, QTypePtr tuple_qtype) {
        return pybind11_unstatus_or(
            MakeNamedTupleQType(field_names, tuple_qtype));
      },
      py::arg("field_names"), py::arg("tuple_qtype"), py::pos_only(),
      py::doc("internal_make_namedtuple_qtype(field_names, tuple_qtype, /)\n"
              "--\n\n"
              "(internal) Returns a namedtuple qtype with the given field "
              "types."));

  m.def(
      "internal_make_tuple_qtype",
      [](const std::vector<QTypePtr>& field_qtypes) {
        return MakeTupleQType(field_qtypes);
      },
      py::arg("field_qtypes"), py::pos_only(),
      py::doc("internal_make_tuple_qtype(field_qtypes, /)\n"
              "--\n\n"
              "(internal) Returns a tuple qtype with the given field types."));

  m.def(
      "internal_register_py_object_decoding_fn",
      [](std::function<py::object(py::bytes data, py::bytes codec)>
             decoding_fn) {
        if (decoding_fn) {
          RegisterPyObjectDecodingFn(
              [decoding_fn = std::move(decoding_fn)](
                  absl::string_view data,
                  absl::string_view codec) -> absl::StatusOr<PyObject*> {
                pybind11::gil_scoped_acquire guard;
                try {
                  return decoding_fn(py::bytes(data), py::bytes(codec))
                      .release()
                      .ptr();
                } catch (pybind11::error_already_set& ex) {
                  ex.restore();
                } catch (pybind11::builtin_exception& ex) {
                  ex.set_error();
                } catch (...) {
                  return absl::InternalError("unknown error");
                }
                return StatusWithRawPyErr(
                    absl::StatusCode::kFailedPrecondition,
                    absl::StrFormat("PY_OBJECT decoder has failed, codec='%s'",
                                    absl::Utf8SafeCHexEscape(codec)));
              });
        } else {
          RegisterPyObjectDecodingFn(nullptr);
        }
      },
      py::arg("decoding_fn"), py::pos_only(),
      py::doc(
          "internal_register_py_object_decoding_fn(decoding_fn, /)\n"
          "--\n\n"
          "(internal) Registers a function used to decode python objects.\n\n"
          "Note: Use `None` to reset the `decoding_fn` state."));

  m.def(
      "internal_register_py_object_encoding_fn",
      [](std::function<std::string(py::handle object, py::bytes codec)>
             encoding_fn) {
        if (encoding_fn) {
          RegisterPyObjectEncodingFn(
              [encoding_fn = std::move(encoding_fn)](
                  PyObject* py_obj,
                  absl::string_view codec) -> absl::StatusOr<std::string> {
                pybind11::gil_scoped_acquire guard;
                try {
                  return encoding_fn(py::handle(py_obj), py::bytes(codec));
                } catch (pybind11::error_already_set& ex) {
                  ex.restore();
                } catch (pybind11::builtin_exception& ex) {
                  ex.set_error();
                } catch (...) {
                  return absl::InternalError("unknown error");
                }
                return StatusWithRawPyErr(
                    absl::StatusCode::kFailedPrecondition,
                    absl::StrFormat("PY_OBJECT encoder has failed, codec='%s'",
                                    absl::Utf8SafeCHexEscape(codec)));
              });
        } else {
          RegisterPyObjectEncodingFn(nullptr);
        }
      },
      py::arg("encoding_fn"), py::pos_only(),
      py::doc("internal_register_py_object_encoding_fn(encoding_fn, /)\n"
              "--\n\n"
              "Registers a function used to encode python objects.\n\n"
              "Note: Use `None` to reset the `encoding_fn` state."

              ));

  m.def(
      "is_dict_qtype", [](QTypePtr qtype) { return IsDictQType(qtype); },
      py::arg("qtype"), py::pos_only(),
      py::doc("is_dict_qtype(qtype, /)\n"
              "--\n\n"
              "Returns True iff the given qtype is a dict."));

  m.def(
      "is_key_to_row_dict_qtype",
      [](QTypePtr qtype) { return IsKeyToRowDictQType(qtype); },
      py::arg("qtype"), py::pos_only(),
      py::doc("is_key_to_row_dict_qtype(qtype, /)\n"
              "--\n\n"
              "Returns True iff the given qtype is a key-to-row-dict."));

  m.def(
      "is_namedtuple_qtype",
      [](QTypePtr qtype) { return IsNamedTupleQType(qtype); }, py::arg("qtype"),
      py::pos_only(),
      py::doc("is_namedtuple_qtype(qtype, /)\n"
              "--\n\n"
              "Returns True iff the given qtype is a namedtuple."));

  m.def(
      "is_sequence_qtype",
      [](QTypePtr qtype) { return IsSequenceQType(qtype); }, py::arg("qtype"),
      py::pos_only(),
      py::doc("is_sequence_qtype(qtype, /)\n"
              "--\n\n"
              "Returns True iff the given qtype is a sequence."));

  m.def(
      "is_tuple_qtype", [](QTypePtr qtype) { return IsTupleQType(qtype); },
      py::arg("qtype"), py::pos_only(),
      py::doc("is_tuple_qtype(qtype, /)\n"
              "--\n\n"
              "Returns True iff the given qtype is a tuple."));

  m.def(
      "make_dict_qtype",
      [](QTypePtr key_qtype, QTypePtr value_qtype) {
        return pybind11_unstatus_or(GetDictQType(key_qtype, value_qtype));
      },
      py::arg("key_qtype"), py::arg("value_qtype"), py::pos_only(),
      py::doc(
          "make_dict_qtype(key_qtype, value_qtype, /)\n"
          "--\n\n"
          "Returns a dict qtype corresponding to the given key/value types."));

  m.def(
      "make_key_to_row_dict_qtype",
      [](QTypePtr key_qtype) {
        return pybind11_unstatus_or(GetKeyToRowDictQType(key_qtype));
      },
      py::arg("key_qtype"), py::pos_only(),
      py::doc("make_key_to_row_dict_qtype(key_qtype, /)\n"
              "--\n\n"
              "Returns a key-to-row-dict qtype corresponding to the given key "
              "qtype."));

  m.def(
      "make_qvalue_from_fields",
      [](QTypePtr compound_qtype,
         const std::vector<TypedValue>& field_qvalues) {
        return pybind11_unstatus_or(
            TypedValue::FromFields(compound_qtype, field_qvalues));
      },
      py::arg("compound_qtype"), py::arg("field_qvalues"), py::pos_only(),
      py::doc("make_qvalue_from_fields(compound_qtype, field_qvalues, /)\n"
              "--\n\n"
              "Returns a qvalue constructed from its type and the field "
              "values."));

  m.def(
      "make_sequence_qtype",
      [](QTypePtr value_qtype) { return GetSequenceQType(value_qtype); },
      py::arg("value_qtype"), py::pos_only(),
      py::doc("make_sequence_qtype(value_qtype, /)\n"
              "--\n\n"
              "Returns the sequence qtype corresponding to a value qtype."));

  m.def(
      "make_sequence_qvalue",
      [](const std::vector<TypedValue>& values,
         std::optional<QTypePtr> value_qtype) {
        if (!value_qtype.has_value()) {
          if (values.empty()) {
            value_qtype = GetNothingQType();
          } else {
            value_qtype = values[0].GetType();
          }
        }
        auto mutable_sequence = pybind11_unstatus_or(
            MutableSequence::Make(*value_qtype, values.size()));
        for (size_t i = 0; i < values.size(); ++i) {
          if (values[i].GetType() != *value_qtype) {
            throw py::type_error(absl::StrFormat(
                "expected all elements to be %s, got values[%u]: %s",
                (**value_qtype).name(), i, values[i].GetType()->name()));
          }
          mutable_sequence.UnsafeSetRef(i, values[i].AsRef());
        }
        return pybind11_unstatus_or(
            TypedValue::FromValueWithQType(std::move(mutable_sequence).Finish(),
                                           GetSequenceQType(*value_qtype)));
      },
      py::arg("values"), py::pos_only(), py::arg("value_qtype") = py::none(),
      py::doc("make_sequence_qvalue(values, /, value_qtype=None)\n"
              "--\n\n"
              "Returns a sequence constructed from the given values."));

  m.def(
      "py_object",
      [](py::handle object, std::optional<std::string> codec) {
        return pybind11_unstatus_or(
            BoxPyObject(object.ptr(), std::move(codec)));
      },
      py::arg("object"), py::pos_only(), py::arg("codec") = py::none(),
      py::doc("py_object(object, /, codec=None)\n"
              "--\n\n"
              "Wraps an object as an opaque PY_OBJECT qvalue.\n\n"
              "NOTE: If `obj` is a qvalue instances, the function raises "
              "ValueError."));

  m.def(
      "py_object_from_data",
      [](absl::string_view data, std::string codec) {
        return pybind11_unstatus_or(DecodePyObject(data, std::move(codec)));
      },
      py::arg("data"), py::arg("codec"), py::pos_only(),
      py::doc(
          "py_object_from_data(data, codec, /)\n"
          "--\n\n"
          "Returns a PY_OBJECT instance decoded from the serialized data."));

  m.def(
      "unbox_py_object",
      [](const TypedValue& value) {
        return py::reinterpret_steal<py::object>(
            pybind11_unstatus_or(UnboxPyObject(value)));
      },
      py::arg("value"), py::pos_only(),
      py::doc("unbox_py_object(value, /)\n"
              "--\n\n"
              "Returns an object stored in the given PY_OBJECT qvalue."));
  // go/keep-sorted end

  struct QValueBufferProxy {
    TypedValue qvalue;
    const void* ptr;
    ssize_t itemsize;
    ssize_t size;
    std::string format;
  };
  py::class_<QValueBufferProxy>(m, "_QValueBufferProxy", py::buffer_protocol())
      .def_buffer([](const QValueBufferProxy& proxy) {
        return py::buffer_info(const_cast<void*>(proxy.ptr), proxy.itemsize,
                               proxy.format,
                               /*ndim=*/1,
                               /*shape_in=*/{proxy.size},
                               /*strides_in=*/{proxy.itemsize},
                               /*readonly=*/true);
      });
  m.def(
      "get_dense_array_memoryview",
      [](const TypedValue& qvalue) {
        QValueBufferProxy result{qvalue};
        const auto setup_result = [&result](const auto& dense_array) {
          using T = typename std::decay_t<decltype(dense_array)>::base_type;
          if (!dense_array.IsFull()) {
            throw py::value_error(
                "dense array has missing elements, cannot provide a "
                "memoryview");
          }
          result.ptr = const_cast<T*>(dense_array.values.span().data());
          result.itemsize = sizeof(T);
          result.size = dense_array.size();
          result.format = py::format_descriptor<T>::format();
        };
        const auto& qtype = qvalue.GetType();
        if (qtype == GetDenseArrayQType<bool>()) {
          setup_result(qvalue.UnsafeAs<DenseArray<bool>>());
        } else if (qtype == GetDenseArrayQType<float>()) {
          setup_result(qvalue.UnsafeAs<DenseArray<float>>());
        } else if (qtype == GetDenseArrayQType<double>()) {
          setup_result(qvalue.UnsafeAs<DenseArray<double>>());
        } else if (qtype == GetDenseArrayWeakFloatQType()) {
          setup_result(qvalue.UnsafeAs<DenseArray<double>>());
        } else if (qtype == GetDenseArrayQType<int32_t>()) {
          setup_result(qvalue.UnsafeAs<DenseArray<int32_t>>());
        } else if (qtype == GetDenseArrayQType<int64_t>()) {
          setup_result(qvalue.UnsafeAs<DenseArray<int64_t>>());
        } else if (qtype == GetDenseArrayQType<uint64_t>()) {
          setup_result(qvalue.UnsafeAs<DenseArray<uint64_t>>());
        } else {
          PyErr_Format(PyExc_NotImplementedError,
                       "cannot provide a memoryview (qtype=%s)",
                       std::string(qtype->name()).c_str());
          throw py::error_already_set();
        }
        return py::memoryview(py::cast(std::move(result)));
      },
      py::arg("dense_array"), py::pos_only(),
      py::doc("get_dense_array_memoryview(dense_array, /)\n--\n\n"
              "Returns a memoryview of the internal buffer of `dense_array`."));
}

}  // namespace
}  // namespace arolla::python
