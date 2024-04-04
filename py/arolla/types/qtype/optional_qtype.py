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

"""(Private) Optional qtypes and corresponding factory functions.

Please avoid using this module directly. Use arolla (preferrably) or
arolla.types.types instead.
"""

from arolla.abc import abc as arolla_abc
from arolla.types.qtype import casting
from arolla.types.qtype import clib
from arolla.types.qtype import scalar_qtype

# Returns OPTIONAL_BOOLEAN qvalue.
optional_boolean = clib.optional_boolean

# Returns OPTIONAL_BYTES qvalue.
optional_bytes = clib.optional_bytes

# Returns OPTIONAL_FLOAT32 qvalue.
optional_float32 = clib.optional_float32

# Returns OPTIONAL_FLOAT64 qvalue.
optional_float64 = clib.optional_float64

# Returns OPTIONAL_INT32 qvalue.
optional_int32 = clib.optional_int32

# Returns OPTIONAL_INT64 qvalue.
optional_int64 = clib.optional_int64

# Returns OPTIONAL_TEXT qvalue.
optional_text = clib.optional_text

# Returns OPTIONAL_UINT64 qvalue.
optional_uint64 = clib.optional_uint64

# Returns OPTIONAL_UNIT qvalue.
optional_unit = clib.optional_unit

# Returns OPTIONAL_WEAK_FLOAT qvalue.
optional_weak_float = clib.optional_weak_float

# LINT.IfChange
OPTIONAL_BOOLEAN = optional_boolean(None).qtype
OPTIONAL_BYTES = optional_bytes(None).qtype
OPTIONAL_FLOAT32 = optional_float32(None).qtype
OPTIONAL_FLOAT64 = optional_float64(None).qtype
OPTIONAL_INT32 = optional_int32(None).qtype
OPTIONAL_INT64 = optional_int64(None).qtype
OPTIONAL_TEXT = optional_text(None).qtype
OPTIONAL_UINT64 = optional_uint64(None).qtype
OPTIONAL_UNIT = optional_unit(None).qtype
OPTIONAL_WEAK_FLOAT = optional_weak_float(None).qtype

OPTIONAL_SCALAR_SHAPE = clib.OPTIONAL_SCALAR_SHAPE
# LINT.ThenChange(//depot/py/arolla/rl.py)


OPTIONAL_QTYPES = (
    OPTIONAL_UNIT,
    OPTIONAL_BOOLEAN,
    OPTIONAL_BYTES,
    OPTIONAL_TEXT,
    OPTIONAL_INT32,
    OPTIONAL_INT64,
    OPTIONAL_UINT64,
    OPTIONAL_FLOAT32,
    OPTIONAL_FLOAT64,
    OPTIONAL_WEAK_FLOAT,
)


def is_optional_qtype(qtype: arolla_abc.QType) -> bool:
  """Indicates whether the given qtype is optional."""
  return casting.get_shape_qtype_or_nothing(qtype) == OPTIONAL_SCALAR_SHAPE


def make_optional_qtype(qtype: arolla_abc.QType) -> arolla_abc.QType:
  """Returns the optional qtype for a given value qtype."""
  if not (scalar_qtype.is_scalar_qtype(qtype) or is_optional_qtype(qtype)):
    raise casting.QTypeError(f'expected a scalar qtype, got {qtype=}')
  return casting.broadcast_qtype([OPTIONAL_UNIT], qtype)


def missing_unit() -> arolla_abc.AnyQValue:
  """Returns a missing OPTIONAL_UNIT."""
  return optional_unit(None)


def present_unit() -> arolla_abc.AnyQValue:
  """Returns a present OPTIONAL_UNIT."""
  return optional_unit(True)


# Aliases
missing = missing_unit
present = present_unit
