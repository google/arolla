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

"""Mixins for QValue specialisations.

Please avoid using this module directly. Use arolla.rl (preferrably) or
arolla.types.types instead.
"""

from arolla.abc import abc as arolla_abc
from arolla.types.qtype import boxing as rl_boxing

# Methods of qvalue-mixin classes expect that 'self' is QValue, but
# pytype cannot deduce it. For that reason we have to suppress the check.
#
# pytype: disable=wrong-arg-types

# IMPORTANT NOTE: If a mixin overrides a base class method, such mixin should
# come first in the inheritance list.
#
# Example:
#
#   class Scalar(PresenceQValueMixin, QValue):
#                ^^^
#                PresenceQValueMixin.__eq__ overrides QValue.__eq__
#


_as_qvalue = rl_boxing.as_qvalue
_invoke_op = arolla_abc.invoke_op


def _invoke_binary_op(op, qvalue, other) -> arolla_abc.AnyQValue:
  try:
    return _invoke_op(op, (qvalue, _as_qvalue(other)))
  except TypeError:
    return NotImplemented


def _invoke_binary_rop(op, other, qvalue) -> arolla_abc.AnyQValue:
  try:
    return _invoke_op(op, (_as_qvalue(other), qvalue))
  except TypeError:
    return NotImplemented


class PresenceQValueMixin:
  """Mixin with presence operations."""

  __slots__ = ()

  def __lt__(self, other) -> arolla_abc.AnyQValue:
    return _invoke_binary_op('core.less', self, other)

  def __le__(self, other) -> arolla_abc.AnyQValue:
    return _invoke_binary_op('core.less_equal', self, other)

  def __ge__(self, other) -> arolla_abc.AnyQValue:
    return _invoke_binary_op('core.greater_equal', self, other)

  def __gt__(self, other) -> arolla_abc.AnyQValue:
    return _invoke_binary_op('core.greater', self, other)

  def __and__(self, other) -> arolla_abc.AnyQValue:
    return _invoke_binary_op('core.presence_and', self, other)

  # NOTE: __rand__ is currently redundant for a general case, because
  # it works only for types based on unit based types. And there is autoboxing
  # for units.
  def __rand__(self, other) -> arolla_abc.AnyQValue:
    return _invoke_binary_rop('core.presence_and', other, self)

  def __or__(self, other) -> arolla_abc.AnyQValue:
    return _invoke_binary_op('core.presence_or', self, other)

  def __ror__(self, other) -> arolla_abc.AnyQValue:
    return _invoke_binary_rop('core.presence_or', other, self)

  def __invert__(self) -> arolla_abc.AnyQValue:
    return _invoke_op('core.presence_not', (self,))

  # Required behaviour for __eq__ (and __ne__):
  #   qvalue == qvalue => rl.eval(M.core.equal(...))
  #   qvalue == expr => M.core.equal(...)
  #   qvalue == object => TypeError
  #
  # NOTE: Methods __eq__ and __ne__ are special. When these methods return
  # NotImplemented, it makes Python switch to the object identity check.
  #
  # The implication is that, generally, QValue.__eq__ may not return
  # NotImplemented (otherwise, `qvalue == object` is False). For that reason,
  # we handle comparison with Expr manually.
  #
  # TODO: Introduce an extension point to QValue.__eq__.
  def __eq__(self, other) -> arolla_abc.AnyQValue | arolla_abc.Expr:  # pytype: disable=signature-mismatch
    if isinstance(other, arolla_abc.Expr):
      return NotImplemented  # fallback to Expr.__eq__
    return _invoke_op('core.equal', (self, _as_qvalue(other)))

  def __ne__(self, other) -> arolla_abc.AnyQValue | arolla_abc.Expr:  # pytype: disable=signature-mismatch
    if isinstance(other, arolla_abc.Expr):
      return NotImplemented  # fallback to Expr.__ne__
    return _invoke_op('core.not_equal', (self, _as_qvalue(other)))


class IntegralArithmeticQValueMixin:
  """A mixin with arithmetic operatorions."""

  __slots__ = ()

  def __pos__(self) -> arolla_abc.AnyQValue:
    return _invoke_op('math.pos', (self,))

  def __neg__(self) -> arolla_abc.AnyQValue:
    return _invoke_op('math.neg', (self,))

  def __add__(self, other) -> arolla_abc.AnyQValue:
    return _invoke_binary_op('math.add', self, other)

  def __radd__(self, other) -> arolla_abc.AnyQValue:
    return _invoke_binary_rop('math.add', other, self)

  def __sub__(self, other) -> arolla_abc.AnyQValue:
    return _invoke_binary_op('math.subtract', self, other)

  def __rsub__(self, other) -> arolla_abc.AnyQValue:
    return _invoke_binary_rop('math.subtract', other, self)

  def __mul__(self, other) -> arolla_abc.AnyQValue:
    return _invoke_binary_op('math.multiply', self, other)

  def __rmul__(self, other) -> arolla_abc.AnyQValue:
    return _invoke_binary_rop('math.multiply', other, self)

  def __floordiv__(self, other) -> arolla_abc.AnyQValue:
    return _invoke_binary_op('math.floordiv', self, other)

  def __rfloordiv__(self, other) -> arolla_abc.AnyQValue:
    return _invoke_binary_rop('math.floordiv', other, self)

  def __mod__(self, other) -> arolla_abc.AnyQValue:
    return _invoke_binary_op('math.mod', self, other)

  def __rmod__(self, other) -> arolla_abc.AnyQValue:
    return _invoke_binary_rop('math.mod', other, self)


class FloatingPointArithmeticQValueMixin(IntegralArithmeticQValueMixin):
  """A mixin with floating-point arithmetic operatorions."""

  __slots__ = ()

  def __truediv__(self, other) -> arolla_abc.AnyQValue:
    return _invoke_binary_op('math.divide', self, other)

  def __rtruediv__(self, other) -> arolla_abc.AnyQValue:
    return _invoke_binary_rop('math.divide', other, self)

  def __pow__(self, other) -> arolla_abc.AnyQValue:
    return _invoke_binary_op('math.pow', self, other)

  def __rpow__(self, other) -> arolla_abc.AnyQValue:
    return _invoke_binary_rop('math.pow', other, self)


# pytype: enable=wrong-arg-types
