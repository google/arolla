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

"""QType and QValue abstractions.

This module provides no concrete QTypes or QValue specializations.
"""

from typing import Any, Self

from arolla.abc import clib

# A fingerprint type.
Fingerprint = clib.Fingerprint

# Base class of all Arolla values in Python.
#
# QValue is immutable. It provides only basic functionality. Subclasses of this
# class might have further specialization.
#
# class QValue:
#
#   Methods defined here:
#
#     _arolla_init_(self, /)
#       This method gets automatically invoked from C++ code during the object
#       initialization **instead of** __init__(self, *args, **kwargs).
#       Subclasses can override this method.
#
#     __bool__(self, /)
#       The default implementation raises TypeError.
#       Subclasses can override this method.
#
#     __hash__(self, /)
#       Raises TypeError.
#       Subclasses can override this method.
#
#     __eq__(self, value, /)
#       Raises TypeError.
#       Subclasses can override this method.
#
#     __repr__(self, /)
#       Returns a string representation of the value.
#
#     __reduce__(self, /)
#       Implements pickle-friendly serialization.
#
#       WARNING: no persistence guarantee, for long-term storage use rl.s11n
#       instead.
#
#   Data descriptors defined here:
#
#     fingerprint
#       Unique identifier of the value.
#
#     qtype
#       QType of the stored value.
#
# (implementation: py/arolla/abc/py_qvalue.cc)
#
QValue = clib.QValue

# QType describes the memory layout of Arolla values.
#
# A final subclass of QValue.
#
# class QType(QValue)
#
#   Method resolution order:
#     QType
#     QValue
#     builtins.object
#
#   Methods defined here:
#
#     __bool__(self, /)
#       Returns True.
#
#     __eq__(self, value, /)
#       Returns True, iff the value represents the same qtype.
#
#     __hash__(self, /)
#       hash(self.fingerprint)
#
#   Data descriptors defined here:
#
#     name
#       Type name.
#
#     value_qtype
#       QType of values for a container type, otherwise None.
#
# (implementation: py/arolla/abc/py_qtype.cc)
#
QType = clib.QType


# Upper-bound type for QValue specializations.
AnyQValue = Any  # b/163076379

# `NOTHING` is an uninhabited type. In other words, it's a type with no values.
NOTHING = clib.NOTHING

# `QTYPE` is a qtype for qtypes.
QTYPE = NOTHING.qtype

# Returns a tuple with field qtypes.
get_field_qtypes = clib.get_field_qtypes

# Returns `unspecified` value.
unspecified = clib.unspecified

UNSPECIFIED = unspecified().qtype


def register_qvalue_specialization(
    key: str | QType, qvalue_class: type[QValue]
) -> None:
  """Registers a specialised QValue subclass for a particular Key or QType.

  A subsequent call with the same key overrides the previous specialisation.

  Key based specialisations have priority over qtype based ones.

  Args:
    key: A string key or QType for specialization.
    qvalue_class: A subclass of QValue.
  """
  if isinstance(key, QType):
    clib.register_qvalue_specialization_by_qtype(key, qvalue_class)
  elif isinstance(key, str):
    clib.register_qvalue_specialization_by_key(key, qvalue_class)
  else:
    raise TypeError(f'expected str or QType, got {key!r}')


def remove_qvalue_specialization(key: str | QType) -> None:
  """Removes QValue specialization for a particular key or QType."""
  if isinstance(key, QType):
    clib.remove_qvalue_specialization_by_qtype(key)
  elif isinstance(key, str):
    clib.remove_qvalue_specialization_by_key(key)
  else:
    raise TypeError(f'expected str or QType, got {key!r}')


class Unspecified(QValue):
  """QValue specialization for `unspecified` value.

  The `unspecified` is to serve as a default value for a parameter in situations
  where the actual default value must be determined based on other parameters.
  """

  __slots__ = ()

  def __new__(cls) -> Self:
    return unspecified()

  def __eq__(self, other) -> bool:
    if isinstance(other, QValue) and other.qtype == UNSPECIFIED:
      return True
    return NotImplemented

  def __ne__(self, other) -> bool:
    if isinstance(other, QValue) and other.qtype == UNSPECIFIED:
      return False
    return NotImplemented

  def py_value(self) -> None:
    return None


register_qvalue_specialization(UNSPECIFIED, Unspecified)
