# Copyright 2025 Google LLC
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

"""QValue specialization for an arolla operator."""

from __future__ import annotations

import inspect
from typing import Any

from arolla.abc import aux_binding_policy as abc_aux_binding_policy
from arolla.abc import clib
from arolla.abc import expr as abc_expr
from arolla.abc import qtype as abc_qtype


# QType for Operator.
OPERATOR = clib.unsafe_make_registered_operator('op-name').qtype

_aux_bind_op = clib.aux_bind_op
_aux_eval_op = clib.aux_eval_op


# Proxy for aux_bind_op() exposing the operator's signature.
class _OperatorAuxBindOp:
  __slots__ = ('_op',)

  def __init__(self, op: Operator):
    self._op = op

  @property
  def __signature__(self) -> inspect.Signature:
    return abc_aux_binding_policy.aux_inspect_signature(self._op)

  def __call__(self, *args: Any, **kwargs: Any) -> abc_expr.Expr:
    return _aux_bind_op(self._op, *args, **kwargs)


# Non-data descriptor for _OperatorAuxBindOp.
#
# General information about descriptors and descriptor protocol available here:
#   https://docs.python.org/3/howto/descriptor.html
class _OperatorAuxBindOpDescriptor:
  __slots__ = ()

  def __get__(self, op: Operator | None, optype: type[Operator]):
    del optype
    if op is None:
      return self
    return _OperatorAuxBindOp(op)

  def __call__(self, *args: Any, **kwargs: Any) -> abc_expr.Expr:
    # A stub method to help pytype recognize Operator as a callable.
    raise NotImplementedError('must be never called')


class Operator(abc_qtype.QValue):
  """QValue specialization for Operator."""

  __slots__ = ()

  def __bool__(self) -> bool:
    return True

  def __hash__(self) -> int:
    return self._fingerprint_hash

  def __eq__(self, other) -> bool:
    if isinstance(other, Operator):
      return self.fingerprint == other.fingerprint
    return NotImplemented

  def __ne__(self, other) -> bool:
    if isinstance(other, Operator):
      return self.fingerprint != other.fingerprint
    return NotImplemented

  def getdoc(self):
    return clib.get_operator_doc(self)

  display_name = property(clib.get_operator_name)

  __signature__ = property(abc_aux_binding_policy.aux_inspect_signature)

  __call__ = _OperatorAuxBindOpDescriptor()


class RegisteredOperator(Operator):
  """QValue specialization for RegisteredOperator."""

  __slots__ = ()

  def __new__(cls, name: str) -> RegisteredOperator:
    return abc_expr.lookup_operator(name)


abc_qtype.register_qvalue_specialization(OPERATOR, Operator)
abc_qtype.register_qvalue_specialization(
    '::arolla::expr::RegisteredOperator', RegisteredOperator
)
