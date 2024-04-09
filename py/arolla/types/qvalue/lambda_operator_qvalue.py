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

"""(Private) QValue specialisations for LambdaOperator.

Please avoid using this module directly. Use arolla (preferrably) or
arolla.types.types instead.
"""

from typing import Self

from arolla.abc import abc as rl_abc
from arolla.types.qvalue import clib
from arolla.types.qvalue import helpers as rl_types_qvalue_helpers


class LambdaOperator(rl_abc.Operator):
  """QValue specialization for LambdaOperator."""

  __slots__ = ()

  def __new__(
      cls, *args, name: str = 'anonymous.lambda', doc: str = ''
  ) -> Self:
    """Constructs a lambda operator.

    Supported calls:

     * LambdaOperator(lambda_body_expr)

       An explicit signature is not required if `lambda_body_expr` contains one
       or less parameters.

       Example:
         sqr_op = LambdaOperator(
             P.x*P.x, name='Sqr', doc='Returns squared `x`.')

     * LambdaOperator(signature, lambda_body_expr)

       `signature` parameter needs to be compatible with
       `rl_abc.make_operator_signature`.

       Examples:

         hypot_op = LambdaOperator('x, y', (P.x**2.0 + P.y**2.0)**0.5)

         log_op   = LambdaOperator(
             ('x, base=', math.exp(1.0)), M.math.log(P.x) / M.math.log(P.base))

    Args:
      *args: signature and/or lambda body expression.
      name: operator name.
      doc: operator doc-string.

    Returns:
      Constructed operator.
    """
    prepared_args = rl_types_qvalue_helpers.prepare_lambda_operator_args(*args)
    return rl_abc.make_lambda(*prepared_args, name=name, doc=doc)

  @property
  def lambda_body(self) -> rl_abc.Expr:
    """Returns body of the lambda operator."""
    return clib.get_lambda_body(self)


rl_abc.register_qvalue_specialization(
    '::arolla::expr::LambdaOperator', LambdaOperator
)
