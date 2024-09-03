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

"""(Private) QValue specialisations for optional types."""

from __future__ import annotations

from typing import Any, Callable, TypeVar

from arolla.abc import abc as arolla_abc
from arolla.types.qtype import optional_qtypes
from arolla.types.qtype import scalar_qtypes
from arolla.types.qtype import scalar_utils
from arolla.types.qvalue import qvalue_mixins


_T = TypeVar('_T')


class OptionalScalar(qvalue_mixins.PresenceQValueMixin, arolla_abc.QValue):
  """Base class for optional type specializations."""

  __slots__ = ('_py_value',)

  _py_value: Any

  def py_value(self, /) -> Any:
    """Returns a python scalar value or None."""
    raise NotImplementedError

  def _init_py_value(self, py_value_fn: Callable[[Any], _T], /) -> _T:
    result = py_value_fn(self)
    self._py_value = result
    return result

  def __hash__(self, /) -> int:
    return hash(self.py_value())

  @property
  def is_present(self, /) -> bool:
    return self.py_value() is not None


class OptionalUnit(OptionalScalar):
  """QValue specialisation for OptionalUnit qtype."""

  __slots__ = ()

  def py_value(self, /) -> bool | None:
    try:
      return self._py_value
    except AttributeError:
      pass
    return self._init_py_value(scalar_utils.py_unit)

  def __bool__(self) -> bool:
    return self.py_value() is not None


class OptionalBoolean(OptionalScalar):
  """QValue specialisation for OPTIONAL_BOOLEAN."""

  __slots__ = ()

  def py_value(self, /) -> bool | None:
    try:
      return self._py_value
    except AttributeError:
      pass
    return self._init_py_value(scalar_utils.py_boolean)

  def __bool__(self) -> bool:
    result = self.py_value()
    if result is None:
      raise scalar_utils.MissingOptionalError
    return result


class OptionalBytes(OptionalScalar):
  """QValue specialisation for OPTIONAL_BYTES."""

  __slots__ = ()

  def py_value(self, /) -> bytes | None:
    try:
      return self._py_value
    except AttributeError:
      pass
    return self._init_py_value(scalar_utils.py_bytes)

  def __bytes__(self) -> bytes:
    result = self.py_value()
    if result is None:
      raise scalar_utils.MissingOptionalError
    return result


class OptionalText(OptionalScalar):
  """QValue specialisation for OPTIONAL_TEXT."""

  __slots__ = ()

  def py_value(self, /) -> str | None:
    try:
      return self._py_value
    except AttributeError:
      pass
    return self._init_py_value(scalar_utils.py_text)


class OptionalInt(qvalue_mixins.IntegralArithmeticQValueMixin, OptionalScalar):
  """QValue specialisation for OPTIONAL_INTs."""

  __slots__ = ()

  def py_value(self, /) -> int | None:
    try:
      return self._py_value
    except AttributeError:
      pass
    return self._init_py_value(scalar_utils.py_index)

  def __index__(self) -> int:
    result = self.py_value()
    if result is None:
      raise scalar_utils.MissingOptionalError
    return result


class OptionalUInt(OptionalScalar):
  """QValue specialisation for OPTIONAL_UINTs."""

  __slots__ = ()

  def py_value(self, /) -> int | None:
    try:
      return self._py_value
    except AttributeError:
      pass
    return self._init_py_value(scalar_utils.py_index)

  def __index__(self) -> int:
    result = self.py_value()
    if result is None:
      raise scalar_utils.MissingOptionalError
    return result


class OptionalFloat(
    qvalue_mixins.FloatingPointArithmeticQValueMixin, OptionalScalar
):
  """QValue specialisation for OPTIONAL_FLOAT."""

  __slots__ = ()

  def py_value(self, /) -> float | None:
    try:
      return self._py_value
    except AttributeError:
      pass
    return self._init_py_value(scalar_utils.py_float)

  def __float__(self) -> float:
    result = self.py_value()
    if result is None:
      raise scalar_utils.MissingOptionalError
    return result

  def __int__(self) -> int:
    return self.__float__().__int__()


class OptionalScalarShape(arolla_abc.QValue):
  """QValue specialisation for OPTIONAL_SCALAR_SHAPE qtype."""

  __slots__ = ()

  def __new__(cls) -> OptionalScalarShape:
    """Constructs a optional scalar shape instance."""
    return arolla_abc.invoke_op('qtype._const_optional_scalar_shape')


def _register_qvalue_specializations():
  """Registers qvalue specializations for optional types."""
  arolla_abc.register_qvalue_specialization(
      optional_qtypes.OPTIONAL_UNIT, OptionalUnit
  )
  arolla_abc.register_qvalue_specialization(
      optional_qtypes.OPTIONAL_BOOLEAN, OptionalBoolean
  )
  arolla_abc.register_qvalue_specialization(
      optional_qtypes.OPTIONAL_BYTES, OptionalBytes
  )
  arolla_abc.register_qvalue_specialization(
      optional_qtypes.OPTIONAL_TEXT, OptionalText
  )
  for qtype in scalar_qtypes.INTEGRAL_QTYPES:
    arolla_abc.register_qvalue_specialization(
        optional_qtypes.make_optional_qtype(qtype), OptionalInt
    )
  arolla_abc.register_qvalue_specialization(
      optional_qtypes.OPTIONAL_UINT64, OptionalUInt
  )
  for qtype in scalar_qtypes.FLOATING_POINT_QTYPES:
    arolla_abc.register_qvalue_specialization(
        optional_qtypes.make_optional_qtype(qtype), OptionalFloat
    )
  arolla_abc.register_qvalue_specialization(
      optional_qtypes.OPTIONAL_SCALAR_SHAPE, OptionalScalarShape
  )


_register_qvalue_specializations()
