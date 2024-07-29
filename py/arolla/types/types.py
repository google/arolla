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

from arolla.abc import abc as _arolla_abc
from arolla.types.qtype import array_qtype as _array_qtype
from arolla.types.qtype import boxing as _boxing
from arolla.types.qtype import casting as _casting
from arolla.types.qtype import dense_array_qtype as _dense_array_qtype
from arolla.types.qtype import dict_qtype as _dict_qtype
from arolla.types.qtype import optional_qtype as _optional_qtype
from arolla.types.qtype import scalar_qtype as _scalar_qtype
from arolla.types.qtype import scalar_utils as _scalar_utils
from arolla.types.qtype import sequence_qtype as _sequence_qtype
from arolla.types.qtype import tuple_qtype as _tuple_qtype
from arolla.types.qvalue import array_qvalue as _array_qvalue
from arolla.types.qvalue import backend_operator_qvalue as _backend_operator_qvalue
from arolla.types.qvalue import dense_array_qvalue as _dense_array_qvalue
from arolla.types.qvalue import dict_qvalue as _dict_qvalue
from arolla.types.qvalue import dispatch_operator_qvalue as _dispatch_operator_qvalue
from arolla.types.qvalue import dummy_operator_qvalue as _dummy_operator_qvalue
from arolla.types.qvalue import generic_operator_qvalue as _generic_operator_qvalue
from arolla.types.qvalue import get_nth_operator_qvalue as _get_nth_operator_qvalue
from arolla.types.qvalue import lambda_operator_qvalue as _lambda_operator_qvalue
from arolla.types.qvalue import optional_qvalue as _optional_qvalue
from arolla.types.qvalue import overloaded_operator_qvalue as _overloaded_operator_qvalue
from arolla.types.qvalue import py_function_operator_qvalue as _py_function_operator_qvalue
from arolla.types.qvalue import restricted_lambda_operator_qvalue as _restricted_lambda_operator_qvalue
from arolla.types.qvalue import scalar_qvalue as _scalar_qvalue
from arolla.types.qvalue import sequence_qvalue as _sequence_qvalue
from arolla.types.qvalue import slice_qvalue as _slice_qvalue
from arolla.types.qvalue import tuple_qvalue as _tuple_qvalue
from arolla.types.s11n import py_object_pickle_codec as _py_object_pickle_codec
from arolla.types.s11n import py_object_reference_codec as _py_object_reference_codec
from arolla.types.s11n import py_object_s11n as _py_object_s11n
from arolla.types.s11n import registered_py_object_codecs as _registered_py_object_codecs


#
# QTypes
#

UNIT = _scalar_qtype.UNIT
BOOLEAN = _scalar_qtype.BOOLEAN
BYTES = _scalar_qtype.BYTES
TEXT = _scalar_qtype.TEXT
INT32 = _scalar_qtype.INT32
INT64 = _scalar_qtype.INT64
UINT64 = _scalar_qtype.UINT64  # non-public
FLOAT32 = _scalar_qtype.FLOAT32
FLOAT64 = _scalar_qtype.FLOAT64
WEAK_FLOAT = _scalar_qtype.WEAK_FLOAT
SCALAR_TO_SCALAR_EDGE = _scalar_qtype.SCALAR_TO_SCALAR_EDGE
SCALAR_SHAPE = _scalar_qtype.SCALAR_SHAPE

OPTIONAL_UNIT = _optional_qtype.OPTIONAL_UNIT
OPTIONAL_BOOLEAN = _optional_qtype.OPTIONAL_BOOLEAN
OPTIONAL_BYTES = _optional_qtype.OPTIONAL_BYTES
OPTIONAL_TEXT = _optional_qtype.OPTIONAL_TEXT
OPTIONAL_INT32 = _optional_qtype.OPTIONAL_INT32
OPTIONAL_INT64 = _optional_qtype.OPTIONAL_INT64
OPTIONAL_UINT64 = _optional_qtype.OPTIONAL_UINT64  # non-public
OPTIONAL_FLOAT32 = _optional_qtype.OPTIONAL_FLOAT32
OPTIONAL_FLOAT64 = _optional_qtype.OPTIONAL_FLOAT64
OPTIONAL_WEAK_FLOAT = _optional_qtype.OPTIONAL_WEAK_FLOAT
OPTIONAL_SCALAR_SHAPE = _optional_qtype.OPTIONAL_SCALAR_SHAPE

DENSE_ARRAY_UNIT = _dense_array_qtype.DENSE_ARRAY_UNIT
DENSE_ARRAY_BOOLEAN = _dense_array_qtype.DENSE_ARRAY_BOOLEAN
DENSE_ARRAY_BYTES = _dense_array_qtype.DENSE_ARRAY_BYTES
DENSE_ARRAY_TEXT = _dense_array_qtype.DENSE_ARRAY_TEXT
DENSE_ARRAY_INT32 = _dense_array_qtype.DENSE_ARRAY_INT32
DENSE_ARRAY_INT64 = _dense_array_qtype.DENSE_ARRAY_INT64
DENSE_ARRAY_UINT64 = _dense_array_qtype.DENSE_ARRAY_UINT64
DENSE_ARRAY_FLOAT32 = _dense_array_qtype.DENSE_ARRAY_FLOAT32
DENSE_ARRAY_FLOAT64 = _dense_array_qtype.DENSE_ARRAY_FLOAT64
DENSE_ARRAY_WEAK_FLOAT = _dense_array_qtype.DENSE_ARRAY_WEAK_FLOAT
DENSE_ARRAY_EDGE = _dense_array_qtype.DENSE_ARRAY_EDGE
DENSE_ARRAY_TO_SCALAR_EDGE = _dense_array_qtype.DENSE_ARRAY_TO_SCALAR_EDGE
DENSE_ARRAY_SHAPE = _dense_array_qtype.DENSE_ARRAY_SHAPE

ARRAY_UNIT = _array_qtype.ARRAY_UNIT
ARRAY_BOOLEAN = _array_qtype.ARRAY_BOOLEAN
ARRAY_BYTES = _array_qtype.ARRAY_BYTES
ARRAY_TEXT = _array_qtype.ARRAY_TEXT
ARRAY_INT32 = _array_qtype.ARRAY_INT32
ARRAY_INT64 = _array_qtype.ARRAY_INT64
ARRAY_UINT64 = _array_qtype.ARRAY_UINT64
ARRAY_FLOAT32 = _array_qtype.ARRAY_FLOAT32
ARRAY_FLOAT64 = _array_qtype.ARRAY_FLOAT64
ARRAY_WEAK_FLOAT = _array_qtype.ARRAY_WEAK_FLOAT
ARRAY_EDGE = _array_qtype.ARRAY_EDGE
ARRAY_TO_SCALAR_EDGE = _array_qtype.ARRAY_TO_SCALAR_EDGE
ARRAY_SHAPE = _array_qtype.ARRAY_SHAPE

PY_OBJECT = _arolla_abc.PY_OBJECT

INTEGRAL_QTYPES = _scalar_qtype.INTEGRAL_QTYPES
FLOATING_POINT_QTYPES = _scalar_qtype.FLOATING_POINT_QTYPES
NUMERIC_QTYPES = _scalar_qtype.NUMERIC_QTYPES
ORDERED_QTYPES = _scalar_qtype.ORDERED_QTYPES

SCALAR_QTYPES = _scalar_qtype.SCALAR_QTYPES
OPTIONAL_QTYPES = _optional_qtype.OPTIONAL_QTYPES
DENSE_ARRAY_QTYPES = _dense_array_qtype.DENSE_ARRAY_QTYPES
DENSE_ARRAY_NUMERIC_QTYPES = _dense_array_qtype.DENSE_ARRAY_NUMERIC_QTYPES
ARRAY_QTYPES = _array_qtype.ARRAY_QTYPES
ARRAY_NUMERIC_QTYPES = _array_qtype.ARRAY_NUMERIC_QTYPES

#
# Factories
#

unit = _scalar_qtype.unit
boolean = _scalar_qtype.boolean
bytes_ = _scalar_qtype.bytes_
text = _scalar_qtype.text
int32 = _scalar_qtype.int32
int64 = _scalar_qtype.int64
uint64 = _scalar_qtype.uint64  # non-public
float32 = _scalar_qtype.float32
float64 = _scalar_qtype.float64
weak_float = _scalar_qtype.weak_float
true = _scalar_qtype.true
false = _scalar_qtype.false

optional_unit = _optional_qtype.optional_unit
optional_boolean = _optional_qtype.optional_boolean
optional_bytes = _optional_qtype.optional_bytes
optional_text = _optional_qtype.optional_text
optional_int32 = _optional_qtype.optional_int32
optional_int64 = _optional_qtype.optional_int64
optional_uint64 = _optional_qtype.optional_uint64  # non-public
optional_float32 = _optional_qtype.optional_float32
optional_float64 = _optional_qtype.optional_float64
optional_weak_float = _optional_qtype.optional_weak_float
missing_unit = _optional_qtype.missing_unit
present_unit = _optional_qtype.present_unit
missing = _optional_qtype.missing
present = _optional_qtype.present

dense_array_unit = _dense_array_qtype.dense_array_unit
dense_array_boolean = _dense_array_qtype.dense_array_boolean
dense_array_bytes = _dense_array_qtype.dense_array_bytes
dense_array_text = _dense_array_qtype.dense_array_text
dense_array_int32 = _dense_array_qtype.dense_array_int32
dense_array_int64 = _dense_array_qtype.dense_array_int64
dense_array_uint64 = _dense_array_qtype.dense_array_uint64
dense_array_float32 = _dense_array_qtype.dense_array_float32
dense_array_float64 = _dense_array_qtype.dense_array_float64
dense_array_weak_float = _dense_array_qtype.dense_array_weak_float

array_unit = _array_qtype.array_unit
array_boolean = _array_qtype.array_boolean
array_bytes = _array_qtype.array_bytes
array_text = _array_qtype.array_text
array_int32 = _array_qtype.array_int32
array_int64 = _array_qtype.array_int64
array_uint64 = _array_qtype.array_uint64
array_float32 = _array_qtype.array_float32
array_float64 = _array_qtype.array_float64
array_weak_float = _array_qtype.array_weak_float

#
# Utilities
#

MissingOptionalError = _scalar_utils.MissingOptionalError
QTypeError = _casting.QTypeError

common_qtype = _casting.common_qtype
common_float_qtype = _casting.common_float_qtype
broadcast_qtype = _casting.broadcast_qtype

is_tuple_qtype = _tuple_qtype.is_tuple_qtype
make_tuple_qtype = _tuple_qtype.make_tuple_qtype

is_namedtuple_qtype = _tuple_qtype.is_namedtuple_qtype
make_namedtuple_qtype = _tuple_qtype.make_namedtuple_qtype
get_namedtuple_field_qtypes = _tuple_qtype.get_namedtuple_field_qtypes

is_sequence_qtype = _sequence_qtype.is_sequence_qtype
make_sequence_qtype = _sequence_qtype.make_sequence_qtype

is_scalar_qtype = _scalar_qtype.is_scalar_qtype
get_scalar_qtype = _scalar_qtype.get_scalar_qtype

is_optional_qtype = _optional_qtype.is_optional_qtype
make_optional_qtype = _optional_qtype.make_optional_qtype

is_dense_array_qtype = _dense_array_qtype.is_dense_array_qtype
make_dense_array_qtype = _dense_array_qtype.make_dense_array_qtype

is_array_qtype = _array_qtype.is_array_qtype
make_array_qtype = _array_qtype.make_array_qtype
array_concat = _array_qtype.array_concat


is_dict_qtype = _dict_qtype.is_dict_qtype
make_dict_qtype = _dict_qtype.make_dict_qtype
is_key_to_row_dict_qtype = _dict_qtype.is_key_to_row_dict_qtype
make_key_to_row_dict_qtype = _dict_qtype.make_key_to_row_dict_qtype

as_qvalue = _boxing.as_qvalue
as_expr = _boxing.as_expr
as_qvalue_or_expr = _boxing.as_qvalue_or_expr
literal = _boxing.literal
eval_ = _boxing.eval_
dense_array = _boxing.dense_array
array = _boxing.array
tuple_ = _boxing.tuple_
namedtuple = _boxing.namedtuple

py_object_codec_str_from_class = _py_object_s11n.codec_str_from_class
register_py_object_codec = _registered_py_object_codecs.register_py_object_codec
PyObjectCodecInterface = _py_object_s11n.PyObjectCodecInterface
PICKLE_CODEC = _py_object_pickle_codec.PICKLE_CODEC
PyObjectReferenceCodec = _py_object_reference_codec.PyObjectReferenceCodec

#
# QValue specialisations
#

Tuple = _tuple_qvalue.Tuple
NamedTuple = _tuple_qvalue.NamedTuple
Slice = _slice_qvalue.Slice

Scalar = _scalar_qvalue.Scalar
Unit = _scalar_qvalue.Unit
Bytes = _scalar_qvalue.Bytes
Text = _scalar_qvalue.Text
Int = _scalar_qvalue.Int
UInt = _scalar_qvalue.UInt
Float = _scalar_qvalue.Float
ScalarShape = _scalar_qvalue.ScalarShape
ScalarToScalarEdge = _scalar_qvalue.ScalarToScalarEdge

OptionalScalar = _optional_qvalue.OptionalScalar
OptionalUnit = _optional_qvalue.OptionalUnit
OptionalBytes = _optional_qvalue.OptionalBytes
OptionalText = _optional_qvalue.OptionalText
OptionalInt = _optional_qvalue.OptionalInt
OptionalUInt = _optional_qvalue.OptionalUInt
OptionalFloat = _optional_qvalue.OptionalFloat
OptionalScalarShape = _optional_qvalue.OptionalScalarShape

DenseArray = _dense_array_qvalue.DenseArray
DenseArrayEdge = _dense_array_qvalue.DenseArrayEdge
DenseArrayToScalarEdge = _dense_array_qvalue.DenseArrayToScalarEdge
DenseArrayShape = _dense_array_qvalue.DenseArrayShape

Array = _array_qvalue.Array
ArrayEdge = _array_qvalue.ArrayEdge
ArrayToScalarEdge = _array_qvalue.ArrayToScalarEdge
ArrayShape = _array_qvalue.ArrayShape

Sequence = _sequence_qvalue.Sequence

Dict = _dict_qvalue.Dict

PyObject = _arolla_abc.PyObject

BackendOperator = _backend_operator_qvalue.BackendOperator
DispatchCase = _dispatch_operator_qvalue.DispatchCase
DispatchOperator = _dispatch_operator_qvalue.DispatchOperator
DummyOperator = _dummy_operator_qvalue.DummyOperator
GetNthOperator = _get_nth_operator_qvalue.GetNthOperator
LambdaOperator = _lambda_operator_qvalue.LambdaOperator
Operator = _arolla_abc.Operator
OverloadedOperator = _overloaded_operator_qvalue.OverloadedOperator
PyFunctionOperator = _py_function_operator_qvalue.PyFunctionOperator
RegisteredOperator = _arolla_abc.RegisteredOperator
RestrictedLambdaOperator = (
    _restricted_lambda_operator_qvalue.RestrictedLambdaOperator
)
GenericOperator = _generic_operator_qvalue.GenericOperator
GenericOperatorOverload = _generic_operator_qvalue.GenericOperatorOverload
