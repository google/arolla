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
from arolla.types.qtype import array_qtypes as _array_qtypes
from arolla.types.qtype import boxing as _boxing
from arolla.types.qtype import casting as _casting
from arolla.types.qtype import dense_array_qtypes as _dense_array_qtypes
from arolla.types.qtype import dict_qtypes as _dict_qtypes
from arolla.types.qtype import optional_qtypes as _optional_qtypes
from arolla.types.qtype import scalar_qtypes as _scalar_qtypes
from arolla.types.qtype import scalar_utils as _scalar_utils
from arolla.types.qtype import sequence_qtypes as _sequence_qtypes
from arolla.types.qtype import tuple_qtypes as _tuple_qtypes
from arolla.types.qvalue import array_qvalues as _array_qvalues
from arolla.types.qvalue import backend_operator_qvalues as _backend_operator_qvalues
from arolla.types.qvalue import dense_array_qvalues as _dense_array_qvalues
from arolla.types.qvalue import dict_qvalues as _dict_qvalues
from arolla.types.qvalue import dispatch_operator_qvalues as _dispatch_operator_qvalues
from arolla.types.qvalue import dummy_operator_qvalues as _dummy_operator_qvalues
from arolla.types.qvalue import generic_operator_qvalues as _generic_operator_qvalues
from arolla.types.qvalue import get_nth_operator_qvalues as _get_nth_operator_qvalues
from arolla.types.qvalue import lambda_operator_qvalues as _lambda_operator_qvalues
from arolla.types.qvalue import optional_qvalues as _optional_qvalues
from arolla.types.qvalue import overloaded_operator_qvalues as _overloaded_operator_qvalues
from arolla.types.qvalue import py_function_operator_qvalues as _py_function_operator_qvalues
from arolla.types.qvalue import restricted_lambda_operator_qvalues as _restricted_lambda_operator_qvalues
from arolla.types.qvalue import scalar_qvalues as _scalar_qvalues
from arolla.types.qvalue import sequence_qvalues as _sequence_qvalues
from arolla.types.qvalue import slice_qvalues as _slice_qvalues
from arolla.types.qvalue import tuple_qvalues as _tuple_qvalues
from arolla.types.s11n import py_object_pickle_codec as _py_object_pickle_codec
from arolla.types.s11n import py_object_reference_codec as _py_object_reference_codec
from arolla.types.s11n import py_object_s11n as _py_object_s11n
from arolla.types.s11n import registered_py_object_codecs as _registered_py_object_codecs


#
# QTypes
#

UNIT = _scalar_qtypes.UNIT
BOOLEAN = _scalar_qtypes.BOOLEAN
BYTES = _scalar_qtypes.BYTES
TEXT = _scalar_qtypes.TEXT
INT32 = _scalar_qtypes.INT32
INT64 = _scalar_qtypes.INT64
UINT64 = _scalar_qtypes.UINT64  # non-public
FLOAT32 = _scalar_qtypes.FLOAT32
FLOAT64 = _scalar_qtypes.FLOAT64
WEAK_FLOAT = _scalar_qtypes.WEAK_FLOAT
SCALAR_TO_SCALAR_EDGE = _scalar_qtypes.SCALAR_TO_SCALAR_EDGE
SCALAR_SHAPE = _scalar_qtypes.SCALAR_SHAPE

OPTIONAL_UNIT = _optional_qtypes.OPTIONAL_UNIT
OPTIONAL_BOOLEAN = _optional_qtypes.OPTIONAL_BOOLEAN
OPTIONAL_BYTES = _optional_qtypes.OPTIONAL_BYTES
OPTIONAL_TEXT = _optional_qtypes.OPTIONAL_TEXT
OPTIONAL_INT32 = _optional_qtypes.OPTIONAL_INT32
OPTIONAL_INT64 = _optional_qtypes.OPTIONAL_INT64
OPTIONAL_UINT64 = _optional_qtypes.OPTIONAL_UINT64  # non-public
OPTIONAL_FLOAT32 = _optional_qtypes.OPTIONAL_FLOAT32
OPTIONAL_FLOAT64 = _optional_qtypes.OPTIONAL_FLOAT64
OPTIONAL_WEAK_FLOAT = _optional_qtypes.OPTIONAL_WEAK_FLOAT
OPTIONAL_SCALAR_SHAPE = _optional_qtypes.OPTIONAL_SCALAR_SHAPE

DENSE_ARRAY_UNIT = _dense_array_qtypes.DENSE_ARRAY_UNIT
DENSE_ARRAY_BOOLEAN = _dense_array_qtypes.DENSE_ARRAY_BOOLEAN
DENSE_ARRAY_BYTES = _dense_array_qtypes.DENSE_ARRAY_BYTES
DENSE_ARRAY_TEXT = _dense_array_qtypes.DENSE_ARRAY_TEXT
DENSE_ARRAY_INT32 = _dense_array_qtypes.DENSE_ARRAY_INT32
DENSE_ARRAY_INT64 = _dense_array_qtypes.DENSE_ARRAY_INT64
DENSE_ARRAY_UINT64 = _dense_array_qtypes.DENSE_ARRAY_UINT64
DENSE_ARRAY_FLOAT32 = _dense_array_qtypes.DENSE_ARRAY_FLOAT32
DENSE_ARRAY_FLOAT64 = _dense_array_qtypes.DENSE_ARRAY_FLOAT64
DENSE_ARRAY_WEAK_FLOAT = _dense_array_qtypes.DENSE_ARRAY_WEAK_FLOAT
DENSE_ARRAY_EDGE = _dense_array_qtypes.DENSE_ARRAY_EDGE
DENSE_ARRAY_TO_SCALAR_EDGE = _dense_array_qtypes.DENSE_ARRAY_TO_SCALAR_EDGE
DENSE_ARRAY_SHAPE = _dense_array_qtypes.DENSE_ARRAY_SHAPE

ARRAY_UNIT = _array_qtypes.ARRAY_UNIT
ARRAY_BOOLEAN = _array_qtypes.ARRAY_BOOLEAN
ARRAY_BYTES = _array_qtypes.ARRAY_BYTES
ARRAY_TEXT = _array_qtypes.ARRAY_TEXT
ARRAY_INT32 = _array_qtypes.ARRAY_INT32
ARRAY_INT64 = _array_qtypes.ARRAY_INT64
ARRAY_UINT64 = _array_qtypes.ARRAY_UINT64
ARRAY_FLOAT32 = _array_qtypes.ARRAY_FLOAT32
ARRAY_FLOAT64 = _array_qtypes.ARRAY_FLOAT64
ARRAY_WEAK_FLOAT = _array_qtypes.ARRAY_WEAK_FLOAT
ARRAY_EDGE = _array_qtypes.ARRAY_EDGE
ARRAY_TO_SCALAR_EDGE = _array_qtypes.ARRAY_TO_SCALAR_EDGE
ARRAY_SHAPE = _array_qtypes.ARRAY_SHAPE

PY_OBJECT = _arolla_abc.PY_OBJECT

INTEGRAL_QTYPES = _scalar_qtypes.INTEGRAL_QTYPES
FLOATING_POINT_QTYPES = _scalar_qtypes.FLOATING_POINT_QTYPES
NUMERIC_QTYPES = _scalar_qtypes.NUMERIC_QTYPES
ORDERED_QTYPES = _scalar_qtypes.ORDERED_QTYPES

SCALAR_QTYPES = _scalar_qtypes.SCALAR_QTYPES
OPTIONAL_QTYPES = _optional_qtypes.OPTIONAL_QTYPES
DENSE_ARRAY_QTYPES = _dense_array_qtypes.DENSE_ARRAY_QTYPES
DENSE_ARRAY_NUMERIC_QTYPES = _dense_array_qtypes.DENSE_ARRAY_NUMERIC_QTYPES
ARRAY_QTYPES = _array_qtypes.ARRAY_QTYPES
ARRAY_NUMERIC_QTYPES = _array_qtypes.ARRAY_NUMERIC_QTYPES

#
# Factories
#

unit = _scalar_qtypes.unit
boolean = _scalar_qtypes.boolean
bytes_ = _scalar_qtypes.bytes_
text = _scalar_qtypes.text
int32 = _scalar_qtypes.int32
int64 = _scalar_qtypes.int64
uint64 = _scalar_qtypes.uint64  # non-public
float32 = _scalar_qtypes.float32
float64 = _scalar_qtypes.float64
weak_float = _scalar_qtypes.weak_float
true = _scalar_qtypes.true
false = _scalar_qtypes.false

optional_unit = _optional_qtypes.optional_unit
optional_boolean = _optional_qtypes.optional_boolean
optional_bytes = _optional_qtypes.optional_bytes
optional_text = _optional_qtypes.optional_text
optional_int32 = _optional_qtypes.optional_int32
optional_int64 = _optional_qtypes.optional_int64
optional_uint64 = _optional_qtypes.optional_uint64  # non-public
optional_float32 = _optional_qtypes.optional_float32
optional_float64 = _optional_qtypes.optional_float64
optional_weak_float = _optional_qtypes.optional_weak_float
missing_unit = _optional_qtypes.missing_unit
present_unit = _optional_qtypes.present_unit
missing = _optional_qtypes.missing
present = _optional_qtypes.present

dense_array_unit = _dense_array_qtypes.dense_array_unit
dense_array_boolean = _dense_array_qtypes.dense_array_boolean
dense_array_bytes = _dense_array_qtypes.dense_array_bytes
dense_array_text = _dense_array_qtypes.dense_array_text
dense_array_int32 = _dense_array_qtypes.dense_array_int32
dense_array_int64 = _dense_array_qtypes.dense_array_int64
dense_array_uint64 = _dense_array_qtypes.dense_array_uint64
dense_array_float32 = _dense_array_qtypes.dense_array_float32
dense_array_float64 = _dense_array_qtypes.dense_array_float64
dense_array_weak_float = _dense_array_qtypes.dense_array_weak_float

array_unit = _array_qtypes.array_unit
array_boolean = _array_qtypes.array_boolean
array_bytes = _array_qtypes.array_bytes
array_text = _array_qtypes.array_text
array_int32 = _array_qtypes.array_int32
array_int64 = _array_qtypes.array_int64
array_uint64 = _array_qtypes.array_uint64
array_float32 = _array_qtypes.array_float32
array_float64 = _array_qtypes.array_float64
array_weak_float = _array_qtypes.array_weak_float

#
# Utilities
#

MissingOptionalError = _scalar_utils.MissingOptionalError
QTypeError = _casting.QTypeError

common_qtype = _casting.common_qtype
common_float_qtype = _casting.common_float_qtype
broadcast_qtype = _casting.broadcast_qtype

is_tuple_qtype = _tuple_qtypes.is_tuple_qtype
make_tuple_qtype = _tuple_qtypes.make_tuple_qtype

is_namedtuple_qtype = _tuple_qtypes.is_namedtuple_qtype
make_namedtuple_qtype = _tuple_qtypes.make_namedtuple_qtype
get_namedtuple_field_qtypes = _tuple_qtypes.get_namedtuple_field_qtypes

is_sequence_qtype = _sequence_qtypes.is_sequence_qtype
make_sequence_qtype = _sequence_qtypes.make_sequence_qtype

is_scalar_qtype = _scalar_qtypes.is_scalar_qtype
get_scalar_qtype = _scalar_qtypes.get_scalar_qtype

is_optional_qtype = _optional_qtypes.is_optional_qtype
make_optional_qtype = _optional_qtypes.make_optional_qtype

is_dense_array_qtype = _dense_array_qtypes.is_dense_array_qtype
make_dense_array_qtype = _dense_array_qtypes.make_dense_array_qtype

is_array_qtype = _array_qtypes.is_array_qtype
make_array_qtype = _array_qtypes.make_array_qtype
array_concat = _array_qtypes.array_concat


is_dict_qtype = _dict_qtypes.is_dict_qtype
make_dict_qtype = _dict_qtypes.make_dict_qtype
is_key_to_row_dict_qtype = _dict_qtypes.is_key_to_row_dict_qtype
make_key_to_row_dict_qtype = _dict_qtypes.make_key_to_row_dict_qtype

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

Tuple = _tuple_qvalues.Tuple
NamedTuple = _tuple_qvalues.NamedTuple
Slice = _slice_qvalues.Slice

Scalar = _scalar_qvalues.Scalar
Unit = _scalar_qvalues.Unit
Bytes = _scalar_qvalues.Bytes
Text = _scalar_qvalues.Text
Int = _scalar_qvalues.Int
UInt = _scalar_qvalues.UInt
Float = _scalar_qvalues.Float
ScalarShape = _scalar_qvalues.ScalarShape
ScalarToScalarEdge = _scalar_qvalues.ScalarToScalarEdge

OptionalScalar = _optional_qvalues.OptionalScalar
OptionalUnit = _optional_qvalues.OptionalUnit
OptionalBytes = _optional_qvalues.OptionalBytes
OptionalText = _optional_qvalues.OptionalText
OptionalInt = _optional_qvalues.OptionalInt
OptionalUInt = _optional_qvalues.OptionalUInt
OptionalFloat = _optional_qvalues.OptionalFloat
OptionalScalarShape = _optional_qvalues.OptionalScalarShape

DenseArray = _dense_array_qvalues.DenseArray
DenseArrayEdge = _dense_array_qvalues.DenseArrayEdge
DenseArrayToScalarEdge = _dense_array_qvalues.DenseArrayToScalarEdge
DenseArrayShape = _dense_array_qvalues.DenseArrayShape

Array = _array_qvalues.Array
ArrayEdge = _array_qvalues.ArrayEdge
ArrayToScalarEdge = _array_qvalues.ArrayToScalarEdge
ArrayShape = _array_qvalues.ArrayShape

Sequence = _sequence_qvalues.Sequence

Dict = _dict_qvalues.Dict

PyObject = _arolla_abc.PyObject

BackendOperator = _backend_operator_qvalues.BackendOperator
DispatchCase = _dispatch_operator_qvalues.DispatchCase
DispatchOperator = _dispatch_operator_qvalues.DispatchOperator
DummyOperator = _dummy_operator_qvalues.DummyOperator
GetNthOperator = _get_nth_operator_qvalues.GetNthOperator
LambdaOperator = _lambda_operator_qvalues.LambdaOperator
Operator = _arolla_abc.Operator
OverloadedOperator = _overloaded_operator_qvalues.OverloadedOperator
PyFunctionOperator = _py_function_operator_qvalues.PyFunctionOperator
RegisteredOperator = _arolla_abc.RegisteredOperator
RestrictedLambdaOperator = (
    _restricted_lambda_operator_qvalues.RestrictedLambdaOperator
)
GenericOperator = _generic_operator_qvalues.GenericOperator
GenericOperatorOverload = _generic_operator_qvalues.GenericOperatorOverload
