# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This module provides the standard arolla types."""

from arolla.abc import abc as _rl_abc
from arolla.types.qtype import array_qtype as _rl_array_qtype
from arolla.types.qtype import boxing as _rl_boxing
from arolla.types.qtype import casting as _rl_casting
from arolla.types.qtype import dense_array_qtype as _rl_dense_array_qtype
from arolla.types.qtype import dict_qtype as _rl_dict_qtype
from arolla.types.qtype import optional_qtype as _rl_optional_qtype
from arolla.types.qtype import scalar_qtype as _rl_scalar_qtype
from arolla.types.qtype import scalar_utils as _rl_scalar_utils
from arolla.types.qtype import sequence_qtype as _rl_sequence_qtype
from arolla.types.qtype import tuple_qtype as _rl_tuple_qtype
from arolla.types.qvalue import array_qvalue as _rl_array_qvalue
from arolla.types.qvalue import backend_operator_qvalue as _rl_backend_operator_qvalue
from arolla.types.qvalue import dense_array_qvalue as _rl_dense_array_qvalue
from arolla.types.qvalue import dict_qvalue as _rl_dict_qvalue
from arolla.types.qvalue import dispatch_operator_qvalue as _rl_dispatch_operator_qvalue
from arolla.types.qvalue import dummy_operator_qvalue as _rl_dummy_operator_qvalue
from arolla.types.qvalue import generic_operator_qvalue as _rl_generic_operator_qvalue
from arolla.types.qvalue import get_nth_operator_qvalue as _rl_get_nth_operator_qvalue
from arolla.types.qvalue import lambda_operator_qvalue as _rl_lambda_operator_qvalue
from arolla.types.qvalue import optional_qvalue as _rl_optional_qvalue
from arolla.types.qvalue import overloaded_operator_qvalue as _rl_overloaded_operator_qvalue
from arolla.types.qvalue import py_function_operator_qvalue as _rl_py_function_operator_qvalue
from arolla.types.qvalue import restricted_lambda_operator_qvalue as _rl_restricted_lambda_operator_qvalue
from arolla.types.qvalue import scalar_qvalue as _rl_scalar_qvalue
from arolla.types.qvalue import sequence_qvalue as _rl_sequence_qvalue
from arolla.types.qvalue import slice_qvalue as _rl_slice_qvalue
from arolla.types.qvalue import tuple_qvalue as _rl_tuple_qvalue
from arolla.types.s11n import py_object_pickle_codec as _rl_py_object_pickle_codec
from arolla.types.s11n import py_object_reference_codec as _rl_py_object_reference_codec
from arolla.types.s11n import py_object_s11n as _rl_py_object_s11n
from arolla.types.s11n import registered_py_object_codecs as _rl_registered_py_object_codecs


#
# QTypes
#

UNIT = _rl_scalar_qtype.UNIT
BOOLEAN = _rl_scalar_qtype.BOOLEAN
BYTES = _rl_scalar_qtype.BYTES
TEXT = _rl_scalar_qtype.TEXT
INT32 = _rl_scalar_qtype.INT32
INT64 = _rl_scalar_qtype.INT64
UINT64 = _rl_scalar_qtype.UINT64  # non-public
FLOAT32 = _rl_scalar_qtype.FLOAT32
FLOAT64 = _rl_scalar_qtype.FLOAT64
WEAK_FLOAT = _rl_scalar_qtype.WEAK_FLOAT
SCALAR_TO_SCALAR_EDGE = _rl_scalar_qtype.SCALAR_TO_SCALAR_EDGE
SCALAR_SHAPE = _rl_scalar_qtype.SCALAR_SHAPE

OPTIONAL_UNIT = _rl_optional_qtype.OPTIONAL_UNIT
OPTIONAL_BOOLEAN = _rl_optional_qtype.OPTIONAL_BOOLEAN
OPTIONAL_BYTES = _rl_optional_qtype.OPTIONAL_BYTES
OPTIONAL_TEXT = _rl_optional_qtype.OPTIONAL_TEXT
OPTIONAL_INT32 = _rl_optional_qtype.OPTIONAL_INT32
OPTIONAL_INT64 = _rl_optional_qtype.OPTIONAL_INT64
OPTIONAL_UINT64 = _rl_optional_qtype.OPTIONAL_UINT64  # non-public
OPTIONAL_FLOAT32 = _rl_optional_qtype.OPTIONAL_FLOAT32
OPTIONAL_FLOAT64 = _rl_optional_qtype.OPTIONAL_FLOAT64
OPTIONAL_WEAK_FLOAT = _rl_optional_qtype.OPTIONAL_WEAK_FLOAT
OPTIONAL_SCALAR_SHAPE = _rl_optional_qtype.OPTIONAL_SCALAR_SHAPE

DENSE_ARRAY_UNIT = _rl_dense_array_qtype.DENSE_ARRAY_UNIT
DENSE_ARRAY_BOOLEAN = _rl_dense_array_qtype.DENSE_ARRAY_BOOLEAN
DENSE_ARRAY_BYTES = _rl_dense_array_qtype.DENSE_ARRAY_BYTES
DENSE_ARRAY_TEXT = _rl_dense_array_qtype.DENSE_ARRAY_TEXT
DENSE_ARRAY_INT32 = _rl_dense_array_qtype.DENSE_ARRAY_INT32
DENSE_ARRAY_INT64 = _rl_dense_array_qtype.DENSE_ARRAY_INT64
DENSE_ARRAY_UINT64 = _rl_dense_array_qtype.DENSE_ARRAY_UINT64
DENSE_ARRAY_FLOAT32 = _rl_dense_array_qtype.DENSE_ARRAY_FLOAT32
DENSE_ARRAY_FLOAT64 = _rl_dense_array_qtype.DENSE_ARRAY_FLOAT64
DENSE_ARRAY_WEAK_FLOAT = _rl_dense_array_qtype.DENSE_ARRAY_WEAK_FLOAT
DENSE_ARRAY_EDGE = _rl_dense_array_qtype.DENSE_ARRAY_EDGE
DENSE_ARRAY_TO_SCALAR_EDGE = _rl_dense_array_qtype.DENSE_ARRAY_TO_SCALAR_EDGE
DENSE_ARRAY_SHAPE = _rl_dense_array_qtype.DENSE_ARRAY_SHAPE

ARRAY_UNIT = _rl_array_qtype.ARRAY_UNIT
ARRAY_BOOLEAN = _rl_array_qtype.ARRAY_BOOLEAN
ARRAY_BYTES = _rl_array_qtype.ARRAY_BYTES
ARRAY_TEXT = _rl_array_qtype.ARRAY_TEXT
ARRAY_INT32 = _rl_array_qtype.ARRAY_INT32
ARRAY_INT64 = _rl_array_qtype.ARRAY_INT64
ARRAY_UINT64 = _rl_array_qtype.ARRAY_UINT64
ARRAY_FLOAT32 = _rl_array_qtype.ARRAY_FLOAT32
ARRAY_FLOAT64 = _rl_array_qtype.ARRAY_FLOAT64
ARRAY_WEAK_FLOAT = _rl_array_qtype.ARRAY_WEAK_FLOAT
ARRAY_EDGE = _rl_array_qtype.ARRAY_EDGE
ARRAY_TO_SCALAR_EDGE = _rl_array_qtype.ARRAY_TO_SCALAR_EDGE
ARRAY_SHAPE = _rl_array_qtype.ARRAY_SHAPE

PY_OBJECT = _rl_abc.PY_OBJECT

INTEGRAL_QTYPES = _rl_scalar_qtype.INTEGRAL_QTYPES
FLOATING_POINT_QTYPES = _rl_scalar_qtype.FLOATING_POINT_QTYPES
NUMERIC_QTYPES = _rl_scalar_qtype.NUMERIC_QTYPES
ORDERED_QTYPES = _rl_scalar_qtype.ORDERED_QTYPES

SCALAR_QTYPES = _rl_scalar_qtype.SCALAR_QTYPES
OPTIONAL_QTYPES = _rl_optional_qtype.OPTIONAL_QTYPES
DENSE_ARRAY_QTYPES = _rl_dense_array_qtype.DENSE_ARRAY_QTYPES
DENSE_ARRAY_NUMERIC_QTYPES = _rl_dense_array_qtype.DENSE_ARRAY_NUMERIC_QTYPES
ARRAY_QTYPES = _rl_array_qtype.ARRAY_QTYPES
ARRAY_NUMERIC_QTYPES = _rl_array_qtype.ARRAY_NUMERIC_QTYPES

#
# Factories
#

unit = _rl_scalar_qtype.unit
boolean = _rl_scalar_qtype.boolean
bytes_ = _rl_scalar_qtype.bytes_
text = _rl_scalar_qtype.text
int32 = _rl_scalar_qtype.int32
int64 = _rl_scalar_qtype.int64
uint64 = _rl_scalar_qtype.uint64  # non-public
float32 = _rl_scalar_qtype.float32
float64 = _rl_scalar_qtype.float64
weak_float = _rl_scalar_qtype.weak_float
true = _rl_scalar_qtype.true
false = _rl_scalar_qtype.false

optional_unit = _rl_optional_qtype.optional_unit
optional_boolean = _rl_optional_qtype.optional_boolean
optional_bytes = _rl_optional_qtype.optional_bytes
optional_text = _rl_optional_qtype.optional_text
optional_int32 = _rl_optional_qtype.optional_int32
optional_int64 = _rl_optional_qtype.optional_int64
optional_uint64 = _rl_optional_qtype.optional_uint64  # non-public
optional_float32 = _rl_optional_qtype.optional_float32
optional_float64 = _rl_optional_qtype.optional_float64
optional_weak_float = _rl_optional_qtype.optional_weak_float
missing_unit = _rl_optional_qtype.missing_unit
present_unit = _rl_optional_qtype.present_unit
missing = _rl_optional_qtype.missing
present = _rl_optional_qtype.present

dense_array_unit = _rl_dense_array_qtype.dense_array_unit
dense_array_boolean = _rl_dense_array_qtype.dense_array_boolean
dense_array_bytes = _rl_dense_array_qtype.dense_array_bytes
dense_array_text = _rl_dense_array_qtype.dense_array_text
dense_array_int32 = _rl_dense_array_qtype.dense_array_int32
dense_array_int64 = _rl_dense_array_qtype.dense_array_int64
dense_array_uint64 = _rl_dense_array_qtype.dense_array_uint64
dense_array_float32 = _rl_dense_array_qtype.dense_array_float32
dense_array_float64 = _rl_dense_array_qtype.dense_array_float64
dense_array_weak_float = _rl_dense_array_qtype.dense_array_weak_float

array_unit = _rl_array_qtype.array_unit
array_boolean = _rl_array_qtype.array_boolean
array_bytes = _rl_array_qtype.array_bytes
array_text = _rl_array_qtype.array_text
array_int32 = _rl_array_qtype.array_int32
array_int64 = _rl_array_qtype.array_int64
array_uint64 = _rl_array_qtype.array_uint64
array_float32 = _rl_array_qtype.array_float32
array_float64 = _rl_array_qtype.array_float64
array_weak_float = _rl_array_qtype.array_weak_float

#
# Utilities
#

MissingOptionalError = _rl_scalar_utils.MissingOptionalError
QTypeError = _rl_casting.QTypeError

common_qtype = _rl_casting.common_qtype
broadcast_qtype = _rl_casting.broadcast_qtype

is_tuple_qtype = _rl_tuple_qtype.is_tuple_qtype
make_tuple_qtype = _rl_tuple_qtype.make_tuple_qtype

is_namedtuple_qtype = _rl_tuple_qtype.is_namedtuple_qtype
make_namedtuple_qtype = _rl_tuple_qtype.make_namedtuple_qtype
get_namedtuple_field_qtypes = _rl_tuple_qtype.get_namedtuple_field_qtypes

is_sequence_qtype = _rl_sequence_qtype.is_sequence_qtype
make_sequence_qtype = _rl_sequence_qtype.make_sequence_qtype

is_scalar_qtype = _rl_scalar_qtype.is_scalar_qtype
get_scalar_qtype = _rl_scalar_qtype.get_scalar_qtype

is_optional_qtype = _rl_optional_qtype.is_optional_qtype
make_optional_qtype = _rl_optional_qtype.make_optional_qtype

is_dense_array_qtype = _rl_dense_array_qtype.is_dense_array_qtype
make_dense_array_qtype = _rl_dense_array_qtype.make_dense_array_qtype

is_array_qtype = _rl_array_qtype.is_array_qtype
make_array_qtype = _rl_array_qtype.make_array_qtype
array_concat = _rl_array_qtype.array_concat


is_dict_qtype = _rl_dict_qtype.is_dict_qtype
make_dict_qtype = _rl_dict_qtype.make_dict_qtype
is_key_to_row_dict_qtype = _rl_dict_qtype.is_key_to_row_dict_qtype
make_key_to_row_dict_qtype = _rl_dict_qtype.make_key_to_row_dict_qtype

as_qvalue = _rl_boxing.as_qvalue
as_expr = _rl_boxing.as_expr
as_qvalue_or_expr = _rl_boxing.as_qvalue_or_expr
literal = _rl_boxing.literal
eval_ = _rl_boxing.eval_
dense_array = _rl_boxing.dense_array
array = _rl_boxing.array
tuple_ = _rl_boxing.tuple_
namedtuple = _rl_boxing.namedtuple

py_object_codec_str_from_class = _rl_py_object_s11n.codec_str_from_class
register_py_object_codec = (
    _rl_registered_py_object_codecs.register_py_object_codec
)
PyObjectCodecInterface = _rl_py_object_s11n.PyObjectCodecInterface
PICKLE_CODEC = _rl_py_object_pickle_codec.PICKLE_CODEC
PyObjectReferenceCodec = _rl_py_object_reference_codec.PyObjectReferenceCodec

#
# QValue specialisations
#

Tuple = _rl_tuple_qvalue.Tuple
NamedTuple = _rl_tuple_qvalue.NamedTuple
Slice = _rl_slice_qvalue.Slice

Scalar = _rl_scalar_qvalue.Scalar
Unit = _rl_scalar_qvalue.Unit
Bytes = _rl_scalar_qvalue.Bytes
Text = _rl_scalar_qvalue.Text
Int = _rl_scalar_qvalue.Int
UInt = _rl_scalar_qvalue.UInt
Float = _rl_scalar_qvalue.Float
ScalarShape = _rl_scalar_qvalue.ScalarShape
ScalarToScalarEdge = _rl_scalar_qvalue.ScalarToScalarEdge

OptionalScalar = _rl_optional_qvalue.OptionalScalar
OptionalUnit = _rl_optional_qvalue.OptionalUnit
OptionalBytes = _rl_optional_qvalue.OptionalBytes
OptionalText = _rl_optional_qvalue.OptionalText
OptionalInt = _rl_optional_qvalue.OptionalInt
OptionalUInt = _rl_optional_qvalue.OptionalUInt
OptionalFloat = _rl_optional_qvalue.OptionalFloat
OptionalScalarShape = _rl_optional_qvalue.OptionalScalarShape

DenseArray = _rl_dense_array_qvalue.DenseArray
DenseArrayEdge = _rl_dense_array_qvalue.DenseArrayEdge
DenseArrayToScalarEdge = _rl_dense_array_qvalue.DenseArrayToScalarEdge
DenseArrayShape = _rl_dense_array_qvalue.DenseArrayShape

Array = _rl_array_qvalue.Array
ArrayEdge = _rl_array_qvalue.ArrayEdge
ArrayToScalarEdge = _rl_array_qvalue.ArrayToScalarEdge
ArrayShape = _rl_array_qvalue.ArrayShape

Sequence = _rl_sequence_qvalue.Sequence

Dict = _rl_dict_qvalue.Dict

PyObject = _rl_abc.PyObject

BackendOperator = _rl_backend_operator_qvalue.BackendOperator
DispatchCase = _rl_dispatch_operator_qvalue.DispatchCase
DispatchOperator = _rl_dispatch_operator_qvalue.DispatchOperator
DummyOperator = _rl_dummy_operator_qvalue.DummyOperator
GetNthOperator = _rl_get_nth_operator_qvalue.GetNthOperator
LambdaOperator = _rl_lambda_operator_qvalue.LambdaOperator
Operator = _rl_abc.Operator
OverloadedOperator = _rl_overloaded_operator_qvalue.OverloadedOperator
PyFunctionOperator = _rl_py_function_operator_qvalue.PyFunctionOperator
RegisteredOperator = _rl_abc.RegisteredOperator
RestrictedLambdaOperator = (
    _rl_restricted_lambda_operator_qvalue.RestrictedLambdaOperator
)
GenericOperator = _rl_generic_operator_qvalue.GenericOperator
GenericOperatorOverload = _rl_generic_operator_qvalue.GenericOperatorOverload
