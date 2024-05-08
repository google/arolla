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

"""(Private) Scalar types and corresponding factory functions.

Please avoid using this module directly. Use arolla.rl (preferrably) or
arolla.types.types instead.
"""

from typing import Callable

from arolla.abc import abc as arolla_abc
from arolla.types.qtype import casting
from arolla.types.qtype import clib

# Returns BYTES qvalue.
boolean = clib.boolean

# Returns BYTES qvalue.
bytes_ = clib.bytes

# Returns FLOAT32 qvalue.
float32 = clib.float32

# Returns FLOAT64 qvalue.
float64 = clib.float64

# Returns INT32 qvalue.
int32 = clib.int32

# Returns INT64 qvalue.
int64 = clib.int64

# Returns TEXT qvalue.
text = clib.text

# Returns UINT64 qvalue.
uint64 = clib.uint64

# Returns UNIT qvalue.
unit = clib.unit

# Returns WEAK_FLOAT qvalue.
weak_float = clib.weak_float

# LINT.IfChange
BOOLEAN = boolean(True).qtype
BYTES = bytes_(b'').qtype
FLOAT32 = float32(0).qtype
FLOAT64 = float64(0).qtype
INT32 = int32(0).qtype
INT64 = int64(0).qtype
TEXT = clib.text('').qtype
UINT64 = uint64(0).qtype
UNIT = unit().qtype
WEAK_FLOAT = weak_float(0).qtype

SCALAR_SHAPE = clib.SCALAR_SHAPE
SCALAR_TO_SCALAR_EDGE = clib.SCALAR_TO_SCALAR_EDGE
# LINT.ThenChange(//depot/py/arolla/types/types.py)

INTEGRAL_QTYPES = (
    INT32,
    INT64,
)

FLOATING_POINT_QTYPES = (
    FLOAT32,
    FLOAT64,
    WEAK_FLOAT,
)

NUMERIC_QTYPES = INTEGRAL_QTYPES + FLOATING_POINT_QTYPES
ORDERED_QTYPES = NUMERIC_QTYPES + (BOOLEAN, BYTES, TEXT)
SCALAR_QTYPES = NUMERIC_QTYPES + (UNIT, BOOLEAN, BYTES, TEXT, UINT64)


# Precompiled 'qtype.get_scalar_qtype' operator.
_compiled_expr_get_scalar_qtype: Callable[
    [arolla_abc.QType], arolla_abc.QType
] = arolla_abc.CompiledExpr(
    arolla_abc.unsafe_parse_sexpr(
        ('qtype.get_scalar_qtype', arolla_abc.leaf('x'))
    ),
    dict(x=arolla_abc.QTYPE),
)


def get_scalar_qtype(qtype: arolla_abc.QType) -> arolla_abc.QType:
  """Returns scalar qtype corresponding to the given `qtype`.

  Examples:

    get_scalar_qtype:
      INT32            -> INT32
      OPTIONAL_FLOAT32 -> FLOAT32
      ARRAY_BYTES      -> BYTES
      TUPLE            -> QTypeError

  Args:
    qtype: A qtype.

  Returns:
    The scalar qtype corresponding to the given `qtype`.

  Raises:
    QTypeError: When there is no corresponding scalar qtype.
  """
  if not isinstance(qtype, arolla_abc.QType):
    raise TypeError(
        f'expected QType, got qtype: {arolla_abc.get_type_name(type(qtype))}'
    )
  result = _compiled_expr_get_scalar_qtype(qtype)
  if result == arolla_abc.NOTHING:
    raise casting.QTypeError(f'no scalar type for: {qtype}')
  return result


def is_scalar_qtype(qtype: arolla_abc.QType) -> bool:
  """Returns True if the argument is a scalar qtype."""
  return casting.get_shape_qtype_or_nothing(qtype) == SCALAR_SHAPE


def false() -> arolla_abc.AnyQValue:
  """Returns BOOLEAN qvalue False."""
  return boolean(False)


def true() -> arolla_abc.AnyQValue:
  """Returns BOOLEAN qvalue True."""
  return boolean(True)
