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

"""Default expr-view for Arolla expressions."""

from arolla.abc import abc as arolla_abc


_aux_bind_op = arolla_abc.aux_bind_op


class DefaultExprView(arolla_abc.ExprView):
  """Default ExprView for the standard arolla expressions.

  This class is defining behavior of infix operations for every Expression.
  """

  def __lt__(self, other) -> arolla_abc.Expr:
    return _aux_bind_op('core.less', self, other)

  def __le__(self, other) -> arolla_abc.Expr:
    return _aux_bind_op('core.less_equal', self, other)

  def __eq__(self, other) -> arolla_abc.Expr:
    return _aux_bind_op('core.equal', self, other)

  def __ne__(self, other) -> arolla_abc.Expr:
    return _aux_bind_op('core.not_equal', self, other)

  def __ge__(self, other) -> arolla_abc.Expr:
    return _aux_bind_op('core.greater_equal', self, other)

  def __gt__(self, other) -> arolla_abc.Expr:
    return _aux_bind_op('core.greater', self, other)

  def __pos__(self) -> arolla_abc.Expr:
    return _aux_bind_op('math.pos', self)

  def __neg__(self) -> arolla_abc.Expr:
    return _aux_bind_op('math.neg', self)

  def __add__(self, other) -> arolla_abc.Expr:
    return _aux_bind_op('math.add', self, other)

  def __radd__(self, other) -> arolla_abc.Expr:
    return _aux_bind_op('math.add', other, self)

  def __sub__(self, other) -> arolla_abc.Expr:
    return _aux_bind_op('math.subtract', self, other)

  def __rsub__(self, other) -> arolla_abc.Expr:
    return _aux_bind_op('math.subtract', other, self)

  def __mul__(self, other) -> arolla_abc.Expr:
    return _aux_bind_op('math.multiply', self, other)

  def __rmul__(self, other) -> arolla_abc.Expr:
    return _aux_bind_op('math.multiply', other, self)

  def __truediv__(self, other) -> arolla_abc.Expr:
    return _aux_bind_op('math.divide', self, other)

  def __rtruediv__(self, other) -> arolla_abc.Expr:
    return _aux_bind_op('math.divide', other, self)

  def __floordiv__(self, other) -> arolla_abc.Expr:
    return _aux_bind_op('math.floordiv', self, other)

  def __rfloordiv__(self, other) -> arolla_abc.Expr:
    return _aux_bind_op('math.floordiv', other, self)

  def __mod__(self, other) -> arolla_abc.Expr:
    return _aux_bind_op('math.mod', self, other)

  def __rmod__(self, other) -> arolla_abc.Expr:
    return _aux_bind_op('math.mod', other, self)

  def __pow__(self, other) -> arolla_abc.Expr:
    return _aux_bind_op('math.pow', self, other)

  def __rpow__(self, other) -> arolla_abc.Expr:
    return _aux_bind_op('math.pow', other, self)

  def __invert__(self) -> arolla_abc.Expr:
    return _aux_bind_op('core.presence_not', self)

  def __and__(self, other) -> arolla_abc.Expr:
    return _aux_bind_op('core.presence_and', self, other)

  def __rand__(self, other) -> arolla_abc.Expr:
    return _aux_bind_op('core.presence_and', other, self)

  def __or__(self, other) -> arolla_abc.Expr:
    return _aux_bind_op('core.presence_or', self, other)

  def __ror__(self, other) -> arolla_abc.Expr:
    return _aux_bind_op('core.presence_or', other, self)


arolla_abc.unsafe_set_default_expr_view(DefaultExprView)
