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

"""Arolla Python API."""

from arolla.abc import abc as _arolla_abc
from arolla.expr import expr as _arolla_expr
from arolla.optools import optools as _arolla_optools
from arolla.s11n import s11n as _arolla_s11n
from arolla.testing import testing as _arolla_testing
from arolla.types import types as _arolla_types

# Load the builtin operator library (if it's available)
try:
  from arolla.operators import operators_clib as _  # pytype: disable=import-error  # pylint: disable=g-import-not-at-top
except ImportError:
  pass


#
# ABC
#

QTYPE = _arolla_abc.QTYPE
QType = _arolla_abc.QType

QValue = _arolla_abc.QValue
AnyQValue = _arolla_abc.AnyQValue

Expr = _arolla_abc.Expr

quote = _arolla_abc.ExprQuote


# `NOTHING` a type with no values. It is an uninhabited type.
NOTHING = _arolla_abc.NOTHING

# The main purpose of `unspecified` is to serve as a default value
# for a parameter in situations where the actual default value must be
# derived from some other parameters.
UNSPECIFIED = _arolla_abc.UNSPECIFIED
unspecified = _arolla_abc.unspecified

OPERATOR = _arolla_abc.OPERATOR

get_leaf_keys = _arolla_abc.get_leaf_keys

sub_by_fingerprint = _arolla_abc.sub_by_fingerprint
sub_by_name = _arolla_abc.sub_by_name
sub_leaves = _arolla_abc.sub_leaves
sub_placeholders = _arolla_abc.sub_placeholders


#
# TYPES
#

## QTypes

UNIT = _arolla_types.UNIT
BOOLEAN = _arolla_types.BOOLEAN
BYTES = _arolla_types.BYTES
TEXT = _arolla_types.TEXT
INT32 = _arolla_types.INT32
INT64 = _arolla_types.INT64
FLOAT32 = _arolla_types.FLOAT32
FLOAT64 = _arolla_types.FLOAT64
WEAK_FLOAT = _arolla_types.WEAK_FLOAT
SCALAR_SHAPE = _arolla_types.SCALAR_SHAPE

OPTIONAL_UNIT = _arolla_types.OPTIONAL_UNIT
OPTIONAL_BOOLEAN = _arolla_types.OPTIONAL_BOOLEAN
OPTIONAL_BYTES = _arolla_types.OPTIONAL_BYTES
OPTIONAL_TEXT = _arolla_types.OPTIONAL_TEXT
OPTIONAL_INT32 = _arolla_types.OPTIONAL_INT32
OPTIONAL_INT64 = _arolla_types.OPTIONAL_INT64
OPTIONAL_FLOAT32 = _arolla_types.OPTIONAL_FLOAT32
OPTIONAL_FLOAT64 = _arolla_types.OPTIONAL_FLOAT64
OPTIONAL_WEAK_FLOAT = _arolla_types.OPTIONAL_WEAK_FLOAT
OPTIONAL_SCALAR_SHAPE = _arolla_types.OPTIONAL_SCALAR_SHAPE

DENSE_ARRAY_UNIT = _arolla_types.DENSE_ARRAY_UNIT
DENSE_ARRAY_BOOLEAN = _arolla_types.DENSE_ARRAY_BOOLEAN
DENSE_ARRAY_BYTES = _arolla_types.DENSE_ARRAY_BYTES
DENSE_ARRAY_TEXT = _arolla_types.DENSE_ARRAY_TEXT
DENSE_ARRAY_INT32 = _arolla_types.DENSE_ARRAY_INT32
DENSE_ARRAY_INT64 = _arolla_types.DENSE_ARRAY_INT64
DENSE_ARRAY_FLOAT32 = _arolla_types.DENSE_ARRAY_FLOAT32
DENSE_ARRAY_FLOAT64 = _arolla_types.DENSE_ARRAY_FLOAT64
DENSE_ARRAY_WEAK_FLOAT = _arolla_types.DENSE_ARRAY_WEAK_FLOAT
DENSE_ARRAY_EDGE = _arolla_types.DENSE_ARRAY_EDGE
DENSE_ARRAY_TO_SCALAR_EDGE = _arolla_types.DENSE_ARRAY_TO_SCALAR_EDGE
DENSE_ARRAY_SHAPE = _arolla_types.DENSE_ARRAY_SHAPE

ARRAY_UNIT = _arolla_types.ARRAY_UNIT
ARRAY_BOOLEAN = _arolla_types.ARRAY_BOOLEAN
ARRAY_BYTES = _arolla_types.ARRAY_BYTES
ARRAY_TEXT = _arolla_types.ARRAY_TEXT
ARRAY_INT32 = _arolla_types.ARRAY_INT32
ARRAY_INT64 = _arolla_types.ARRAY_INT64
ARRAY_FLOAT32 = _arolla_types.ARRAY_FLOAT32
ARRAY_FLOAT64 = _arolla_types.ARRAY_FLOAT64
ARRAY_WEAK_FLOAT = _arolla_types.ARRAY_WEAK_FLOAT
ARRAY_EDGE = _arolla_types.ARRAY_EDGE
ARRAY_TO_SCALAR_EDGE = _arolla_types.ARRAY_TO_SCALAR_EDGE
ARRAY_SHAPE = _arolla_types.ARRAY_SHAPE

## Factory functions

unit = _arolla_types.unit
boolean = _arolla_types.boolean
bytes_ = _arolla_types.bytes_
text = _arolla_types.text
int32 = _arolla_types.int32
int64 = _arolla_types.int64
float32 = _arolla_types.float32
float64 = _arolla_types.float64
weak_float = _arolla_types.weak_float

optional_unit = _arolla_types.optional_unit
optional_boolean = _arolla_types.optional_boolean
optional_bytes = _arolla_types.optional_bytes
optional_text = _arolla_types.optional_text
optional_int32 = _arolla_types.optional_int32
optional_int64 = _arolla_types.optional_int64
optional_float32 = _arolla_types.optional_float32
optional_float64 = _arolla_types.optional_float64
optional_weak_float = _arolla_types.optional_weak_float
missing_unit = _arolla_types.missing_unit
present_unit = _arolla_types.present_unit
missing = _arolla_types.missing
present = _arolla_types.present

dense_array_unit = _arolla_types.dense_array_unit
dense_array_boolean = _arolla_types.dense_array_boolean
dense_array_bytes = _arolla_types.dense_array_bytes
dense_array_text = _arolla_types.dense_array_text
dense_array_int32 = _arolla_types.dense_array_int32
dense_array_int64 = _arolla_types.dense_array_int64
dense_array_float32 = _arolla_types.dense_array_float32
dense_array_float64 = _arolla_types.dense_array_float64
dense_array_weak_float = _arolla_types.dense_array_weak_float


array_unit = _arolla_types.array_unit
array_boolean = _arolla_types.array_boolean
array_bytes = _arolla_types.array_bytes
array_text = _arolla_types.array_text
array_int32 = _arolla_types.array_int32
array_int64 = _arolla_types.array_int64
array_float32 = _arolla_types.array_float32
array_float64 = _arolla_types.array_float64
array_weak_float = _arolla_types.array_weak_float

## Utilities

array = _arolla_types.array
as_expr = _arolla_types.as_expr
as_qvalue = _arolla_types.as_qvalue
dense_array = _arolla_types.dense_array
eval_ = _arolla_types.eval_
is_array_qtype = _arolla_types.is_array_qtype
is_dense_array_qtype = _arolla_types.is_dense_array_qtype
is_dict_qtype = _arolla_types.is_dict_qtype
is_namedtuple_qtype = _arolla_types.is_namedtuple_qtype
is_optional_qtype = _arolla_types.is_optional_qtype
is_scalar_qtype = _arolla_types.is_scalar_qtype
is_tuple_qtype = _arolla_types.is_tuple_qtype
literal = _arolla_types.literal
make_array_qtype = _arolla_types.make_array_qtype
make_dense_array_qtype = _arolla_types.make_dense_array_qtype
make_namedtuple_qtype = _arolla_types.make_namedtuple_qtype
make_optional_qtype = _arolla_types.make_optional_qtype
make_tuple_qtype = _arolla_types.make_tuple_qtype
namedtuple = _arolla_types.namedtuple
tuple_ = _arolla_types.tuple_

LambdaOperator = _arolla_types.LambdaOperator
OverloadedOperator = _arolla_types.OverloadedOperator

#
# EXPR
#

LeafContainer = _arolla_expr.LeafContainer
PlaceholderContainer = _arolla_expr.PlaceholderContainer
OperatorsContainer = _arolla_expr.OperatorsContainer
unsafe_operators_container = _arolla_expr.unsafe_operators_container
L = LeafContainer()
P = PlaceholderContainer()
M = OperatorsContainer()


#
# Public submodules
#
abc = _arolla_abc
optools = _arolla_optools
s11n = _arolla_s11n
testing = _arolla_testing
types = _arolla_types

# Functions that collide with Python builtins:
bytes = bytes_  # pylint: disable=redefined-builtin
eval = eval_  # pylint: disable=redefined-builtin
tuple = tuple_  # pylint: disable=redefined-builtin
