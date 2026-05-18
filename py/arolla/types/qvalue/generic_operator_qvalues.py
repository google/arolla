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

"""(Private) QValue specializations for GenericOperator."""

from __future__ import annotations

from arolla.abc import abc as arolla_abc
from arolla.types.qtype import boxing
from arolla.types.qvalue import clib


class GenericOperator(arolla_abc.Operator):
  """QValue specialization for GenericOperator."""

  __slots__ = ()

  def __new__(
      cls,
      name: str,
      *,
      signature: arolla_abc.MakeOperatorSignatureArg,
      doc: str = '',
  ) -> GenericOperator:
    """Returns a new generic operator instance.

    A generic operator works as a frontend to a namespace with overloads stored
    in the operator registry. The overloads have associated overload conditions
    (that must be mutually exclusive) based on which the overload selection
    happens.

    Args:
      name: An operator name.
      signature: An operator signature.
      doc: An operator doc-string.
    """
    signature = arolla_abc.make_operator_signature(signature)
    return clib.make_generic_operator(name, signature, doc)


class GenericOperatorOverload(arolla_abc.Operator):
  """QValue specialization for GenericOperatorOverload."""

  __slots__ = ()

  def __new__(
      cls,
      base_operator: arolla_abc.Operator,
      overload_condition_expr: arolla_abc.Expr,
      *,
      case_name: str | None = None,
      signature: arolla_abc.MakeOperatorSignatureArg | None = None,
  ) -> GenericOperatorOverload:
    """Creates an overload for a generic operator."""
    if case_name is None:
      case_name = base_operator.display_name
    if signature is None:
      signature = arolla_abc.get_operator_signature(base_operator)
    else:
      signature = arolla_abc.make_operator_signature(signature)
    return clib.make_generic_operator_overload(
        case_name,
        signature,
        boxing.as_expr(overload_condition_expr),
        base_operator,
    )


arolla_abc.register_qvalue_specialization(
    '::arolla::operator_loader::GenericOperator', GenericOperator
)
arolla_abc.register_qvalue_specialization(
    '::arolla::operator_loader::GenericOperatorOverload',
    GenericOperatorOverload,
)
