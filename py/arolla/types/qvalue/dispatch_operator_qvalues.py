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

from arolla.abc import abc as arolla_abc
from arolla.types.qtype import boxing
from arolla.types.qvalue import clib
from arolla.types.qvalue import lambda_operator_qvalues

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
      doc: str = '',
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
      doc: operator doc-string.
      **dispatch_cases: dispatch cases.

    Returns:
      Constructed operator.
    """
    operator_signature = arolla_abc.make_operator_signature(signature)

    overloads = []
    for case_name, case in dispatch_cases.items():
      case_op = _wrap_operator(operator_signature, case.op)
      condition_expr = boxing.as_expr(case.condition)
      overloads.append((case_name, case_op, condition_expr))

    default_op = None
    if default is not None:
      default_op = _wrap_operator(operator_signature, default)

    return clib.make_dispatch_operator(
        name,
        operator_signature,
        doc,
        overloads,
        default_op,
    )


arolla_abc.register_qvalue_specialization(
    '::arolla::operator_loader::DispatchOperator', DispatchOperator
)
