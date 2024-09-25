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

"""(Private) QValue specialisations for RestrictedLambdaOperator."""

from __future__ import annotations

from arolla.abc import abc as arolla_abc
from arolla.types.qvalue import clib
from arolla.types.qvalue import helpers as arolla_types_qvalue_helpers


class RestrictedLambdaOperator(arolla_abc.Operator):
  """QValue specialization for RestrictedLambdaOperator."""

  __slots__ = ()

  def __new__(
      cls,
      *args,
      qtype_constraints: arolla_types_qvalue_helpers.QTypeConstraints = (),
      name: str = 'anonymous.restricted_lambda',
      doc: str = '',
  ) -> RestrictedLambdaOperator:
    """Constructs a restricted lambda operator.

    Supported calls:

     * RestrictedLambdaOperator(lambda_body_expr)

       An explicit signature is not required if `lambda_body_expr` contains one
       or fewer parameters.

       Example:
         sqr_op = RestrictedLambdaOperator(
             P.x * P.x,
             qtype_constraints=[
               (
                   M.qtype.is_floating_point_qtype(P.x),
                   'expected floating point, got x:{x}'
               ),
             ],
             name='Sqr',
             doc='Returns squared `x`.')

     * RestrictedLambdaOperator(signature, lambda_body_expr)

       `signature` parameter needs to be compatible with
       `arolla_abc.make_operator_signature`.

       Examples:

         hypot_op = RestrictedLambdaOperator(
             'x, y',
             (P.x**2.0 + P.y**2.0)**0.5,
             qtype_constraints=[
               (
                   M.qtype.is_floating_point_qtype(P.x),
                   'expected floating point, got x:{x}'
               ),
               (
                   M.qtype.is_floating_point_qtype(P.y),
                   'expected floating point, got y:{y}'
               ),
               (
                   M.qtype.common_qtype(P.x, P.y) != arolla.NOTHING,
                   'types x:{x} and y:{y} are not compatible',
               ),
             ],
         )

    Args:
      *args: signature and/or lambda body expression.
      qtype_constraints: list of [predicate_expr, error_message] pairs. If a
        qtype constraint is not fulfilled, the corresponding error_message is
        used. Placeholders, like {arg_name}, get replaced with the actual type
        names during the error message formatting.
      name: operator name.
      doc: operator doc-string.

    Returns:
      Constructed operator.
    """
    prepared_args = arolla_types_qvalue_helpers.prepare_lambda_operator_args(
        *args
    )
    prepared_qtype_constraints = (
        arolla_types_qvalue_helpers.prepare_qtype_constraints(qtype_constraints)
    )
    return clib.make_restricted_lambda_operator(
        name, *prepared_args, doc, prepared_qtype_constraints
    )

  @property
  def lambda_body(self) -> arolla_abc.Expr:
    """Returns body of the lambda operator."""
    return clib.get_lambda_body(self)


arolla_abc.register_qvalue_specialization(
    '::arolla::operator_loader::RestrictedLambdaOperator',
    RestrictedLambdaOperator,
)
