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

"""Operator declaration helpers."""

from typing import Any

from arolla.abc import abc as arolla_abc
from arolla.types import types as arolla_types


def make_lambda(
    *args: Any,
    qtype_constraints: arolla_types.QTypeConstraints = (),
    name: str = "anonymous.lambda",
    doc: str = "",
) -> arolla_abc.Operator:
  """Returns a lambda operator.

  This function creates a lambda operator based on the provided signature and
  lambda body expression. Depending on whether `qtype_constraints` are provided,
  it returns either a lambda operator or a restricted lambda operator.

  Args:
    *args: The signature and/or the lambda body expression.
    qtype_constraints: A list of `[predicate_expr, error_message]` pairs that
      define constraints on the types of the arguments. If a constraint is not
      fulfilled, the corresponding error_message is used. Placeholders, such as
      `{arg_name}`, are replaced with the actual type names during error message
      formatting.
    name: The name of the operator. Defaults to "anonymous.lambda".
    doc: The docstring for the operator.

  Returns:
    A lambda operator (or a restricted lambda operator, depending on
    the presence of `qtype_constraints`).
  """
  if not qtype_constraints:
    return arolla_types.LambdaOperator(*args, name=name, doc=doc)
  return arolla_types.RestrictedLambdaOperator(
      *args, qtype_constraints=qtype_constraints, name=name, doc=doc
  )


# NOTE: LambdaOperator doesn't raise a warning because of the "unused" prefix
# in the parameter name.
suppress_unused_parameter_warning = make_lambda(
    "x, *unused",
    arolla_abc.placeholder("x"),
    name="suppress_unused_parameter_warning",
    doc=(
        "Returns its first argument and ignores the rest.\n\nIt's a helper"
        ' operator that suppresses "unused lambda parameter" warning.'
    ),
)
