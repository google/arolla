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

"""(Private) Various helpers for arolla.type.qvalue."""

from typing import Iterable

from arolla.abc import abc as arolla_abc
from arolla.types.qtype import boxing

# A single qtype constraint.
QTypeConstraint = tuple[arolla_abc.Expr, str]

# QType constraints.
QTypeConstraints = Iterable[QTypeConstraint]


def prepare_lambda_operator_args(
    *args,
) -> tuple[arolla_abc.Signature, arolla_abc.Expr]:
  """Returns signature and lambda_body_expr for LambdaOperator.

  Supported calls:

   * prepare_lambda_operator_args(lambda_body_expr)

     An explicit signature is not required if `lambda_body_expr` contains one
     or fewer parameters.

     Example:
       clif_signature, expr = prepare_lambda_operator_args(P.x * P.x)

   * prepare_lambda_operator_args(signature, lambda_body_expr)

     `signature` parameter needs to be compatible with
     `arolla_abc.make_operator_signature`.

     Examples:

       ... = prepare_lambda_operator_args('x, y', (P.x**2.0 + P.y**2.0)**0.5)

       ... = prepare_lambda_operator_args(
           ('x, base=', math.exp(1.0)), M.math.log(P.x) / M.math.log(P.base))

  Args:
    *args: signature and/or lambda body expression.

  Returns:
    Constructed clif_signature, lambda_body_expr.
  """
  if not args or len(args) > 2:
    if not args:
      raise TypeError('missing 1 required positional argument')
    raise TypeError(
        'takes from 1 to 2 positional arguments but {} were given'.format(
            len(args)
        )
    )
  if len(args) == 1:
    lambda_body_expr = boxing.as_expr(args[0])
    placeholder_keys = arolla_abc.get_placeholder_keys(lambda_body_expr)
    if len(placeholder_keys) > 1:
      raise ValueError('please provide explicit operator signature')
    signature = arolla_abc.make_operator_signature(', '.join(placeholder_keys))
  else:
    lambda_body_expr = boxing.as_expr(args[1])
    signature = arolla_abc.make_operator_signature(
        args[0], as_qvalue=boxing.as_qvalue
    )
  return signature, lambda_body_expr
