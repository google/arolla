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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "py/arolla/types/qtype/array_boxing.h"
#include "py/arolla/types/qtype/scalar_boxing.h"
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

  // go/keep-sorted end
}

}  // namespace
}  // namespace arolla::python
