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

"""(Private) QValue specialisations for DispatchOperator."""

from __future__ import annotations

import dataclasses
import functools

from arolla.abc import abc as arolla_abc
from arolla.types.qtype import boxing
from arolla.types.qvalue import clib
from arolla.types.qvalue import lambda_operator_qvalues
from arolla.types.qvalue import overload_operator_helpers


OperatorOrExprOrValue = arolla_abc.QValue | arolla_abc.Expr


def _wrap_operator(
    signature: arolla_abc.Signature, value: OperatorOrExprOrValue
) -> arolla_abc.Operator:
  """Returns operator."""
  if isinstance(value, arolla_abc.Operator):
    return value
  return lambda_operator_qvalues.LambdaOperator(
      signature, boxing.as_expr(value)
  )


@dataclasses.dataclass
class DispatchCase:
  """Container for dispatch cases."""

  op: OperatorOrExprOrValue
  condition: arolla_abc.Expr


class DispatchOperator(arolla_abc.Operator):
  """QValue specialization for DispatchOperator."""

  __slots__ = ()

  def __new__(
      cls,
      signature: arolla_abc.MakeOperatorSignatureArg,
      /,
      *,
      default: OperatorOrExprOrValue | None = None,
      name: str = 'anonymous.dispatch_operator',
      **dispatch_cases: DispatchCase,
  ) -> DispatchOperator:
    """Creates a dispatch operator from a given list of overloads.

    Dispatch operator is an adapter for a list of overloads with constraints
    provided in terms of one explicit signature. For each input it checks that
    there is at most one overload with the passing constraint and apply the
    corresponding overload. If default overload is provided, it is applied when
    all the constraints fail.

    Example:
      size_op = DispatchOperator(
          'x',
          seq_case=DispatchCase(
              M.seq.size,
              condition=M.qtype.is_sequence_qtype(P.x)
          ),
          default=M.array.size
      )

    Args:
      signature: operator signature.
      default: (optional) default operator.
      name: operator name.
      **dispatch_cases: dispatch cases.

    Returns:
      Constructed operator.
    """
    operator_signature = arolla_abc.make_operator_signature(signature)
    overload_operator_helpers.check_signature_of_overload(operator_signature)

    prepared_overloads = []
    required_params_to_dispatch = set()
    for case_name, case in dispatch_cases.items():
      case_op = _wrap_operator(operator_signature, case.op)
      prepared_condition_expr, used_param_ids = (
          overload_operator_helpers.substitute_placeholders_in_condition_expr(
              operator_signature,
              boxing.as_expr(case.condition),
          )
      )
      required_params_to_dispatch.update(used_param_ids)
      prepared_overloads.append((case_name, case_op, prepared_condition_expr))

    if default is not None:
      default_op = _wrap_operator(operator_signature, default)
      or_op = arolla_abc.lookup_operator('core.presence_or')
      not_op = arolla_abc.lookup_operator('core.presence_not')
      default_condition = not_op(
          functools.reduce(or_op, (o[2] for o in prepared_overloads))
      )
      prepared_overloads.append(('default', default_op, default_condition))

    dispatch_readiness_condition = (
        overload_operator_helpers.get_overload_condition_readiness_expr(
            operator_signature, required_params_to_dispatch
        )
    )

    return clib.make_dispatch_operator(
        name,
        operator_signature,
        prepared_overloads,
        dispatch_readiness_condition,
    )


arolla_abc.register_qvalue_specialization(
    '::arolla::operator_loader::DispatchOperator', DispatchOperator
)
