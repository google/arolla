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

"""(Private) QValue specialisations for scalar types."""

from __future__ import annotations

from typing import Any, Callable, TypeVar

from arolla.abc import abc as arolla_abc
from arolla.types.qtype import scalar_qtypes
from arolla.types.qtype import scalar_utils
from arolla.types.qvalue import qvalue_mixins


_T = TypeVar('_T')


class Scalar(qvalue_mixins.PresenceQValueMixin, arolla_abc.QValue):
  """Base class for scalar type specializations."""

  __slots__ = ('_py_value',)

  _py_value: Any

  def py_value(self, /) -> Any:
    """Returns a python scalar value."""
    raise NotImplementedError

  def _init_py_value(self, py_value_fn: Callable[[Any], _T | None], /) -> _T:
    result = py_value_fn(self)
    assert result is not None
    self._py_value = result
    return result

  def __hash__(self, /) -> int:
    return hash(self.py_value())


class Unit(Scalar):
  """QValue specialization for UNIT qtype."""

  __slots__ = ()

  def py_value(self, /) -> bool:
    try:
      return self._py_value
    except AttributeError:
      pass
    return self._init_py_value(scalar_utils.py_unit)


class Boolean(Scalar):
  """QValue specialization for boolean qtype."""

  __slots__ = ()

  def py_value(self, /) -> bool:
    try:
      return self._py_value
    except AttributeError:
      pass
    return self._init_py_value(scalar_utils.py_boolean)

  def __bool__(self, /) -> bool:
    return self.py_value()


class Bytes(Scalar):
  """QValue specialization for bytes qtype."""

  __slots__ = ()

  def py_value(self, /) -> bytes:
    try:
      return self._py_value
    except AttributeError:
      pass
    return self._init_py_value(scalar_utils.py_bytes)

  def __bytes__(self, /) -> bytes:
    return self.py_value()


class Text(Scalar):
  """QValue specialization for text qtype."""

  __slots__ = ()

  def py_value(self, /) -> str:
    try:
      return self._py_value
    except AttributeError:
      pass
    return self._init_py_value(scalar_utils.py_text)


class Int(qvalue_mixins.IntegralArithmeticQValueMixin, Scalar):
  """QValue specialization for integral qtypes."""

  __slots__ = ()

  def py_value(self, /) -> int:
    try:
      return self._py_value
    except AttributeError:
      pass
    return self._init_py_value(scalar_utils.py_index)

  def __index__(self, /) -> int:
    return self.py_value()


class UInt(Scalar):
  """QValue specialization for unsigned integral qtypes."""

  __slots__ = ()

  def py_value(self, /) -> int:
    try:
      return self._py_value
    except AttributeError:
      pass
    return self._init_py_value(scalar_utils.py_index)

  def __index__(self, /) -> int:
    return self.py_value()


class Float(qvalue_mixins.FloatingPointArithmeticQValueMixin, Scalar):
  """QValue specialization for floating point qtypes."""

  __slots__ = ()

  def py_value(self, /) -> float:
    try:
      return self._py_value
    except AttributeError:
      pass
    return self._init_py_value(scalar_utils.py_float)

  def __float__(self, /) -> float:
    return self.py_value()

  def __int__(self, /) -> int:
    return self.py_value().__int__()


class ScalarShape(arolla_abc.QValue):
  """QValue specialisation for SCALAR_SHAPE qtype."""

  __slots__ = ()

  def __new__(cls) -> ScalarShape:
    """Constructs a scalar shape instance."""
    return arolla_abc.invoke_op('qtype._const_scalar_shape')


_scalar_to_scalar_edge_expr = arolla_abc.unsafe_parse_sexpr(
    ('edge.from_sizes_or_shape', ('qtype._const_scalar_shape',))
)


class ScalarToScalarEdge(arolla_abc.QValue):
  """QValue specialisation for ScalarToScalarEdge qtype."""

  __slots__ = ()

  def __new__(cls) -> ScalarToScalarEdge:
    """Constructs a scalar-to-scalar-edge instance."""
    return arolla_abc.eval_expr(_scalar_to_scalar_edge_expr)


def _register_qvalue_specializations():
  """Registers qvalue specializations for scalar types."""
  arolla_abc.register_qvalue_specialization(scalar_qtypes.UNIT, Unit)
  arolla_abc.register_qvalue_specialization(scalar_qtypes.BOOLEAN, Boolean)
  arolla_abc.register_qvalue_specialization(scalar_qtypes.BYTES, Bytes)
  arolla_abc.register_qvalue_specialization(scalar_qtypes.TEXT, Text)
  for qtype in scalar_qtypes.INTEGRAL_QTYPES:
    arolla_abc.register_qvalue_specialization(qtype, Int)
  arolla_abc.register_qvalue_specialization(scalar_qtypes.UINT64, UInt)
  for qtype in scalar_qtypes.FLOATING_POINT_QTYPES:
    arolla_abc.register_qvalue_specialization(qtype, Float)
  arolla_abc.register_qvalue_specialization(
      scalar_qtypes.SCALAR_SHAPE, ScalarShape
  )
  arolla_abc.register_qvalue_specialization(
      scalar_qtypes.SCALAR_TO_SCALAR_EDGE, ScalarToScalarEdge
  )


_register_qvalue_specializations()
