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

"""(Private) QValue specializations for GenericOperator.

Please avoid using this module directly. Use arolla.rl (preferrably) or
arolla.types.types instead.
"""

import functools
from typing import Self

from arolla.abc import abc as arolla_abc
from arolla.types.qtype import boxing
from arolla.types.qvalue import clib
from arolla.types.qvalue import overload_operator_helpers


def _prepare_generic_overload_condition_expr(
    signature: arolla_abc.Signature, condition_expr: arolla_abc.Expr
) -> arolla_abc.Expr:
  """Returns a prepared overload condition expression.

  The overload condition is similar to the qtype constraint (see
  BackendOperator). A condition can reference the parameters' qtypes using
  placeholders, such as P.param_name, and must return present() or missing() to
  indicate whether the condition is met. The distinction from qtype_constraints
  is that an unmet condition does not imply an error.

  A condition can also use L.input_tuple_qtype, which represents types of all
  inputs using a single tuple_qtype value, where the unknown qtypes represented
  with NOTHING.

  The prepared overload expression contains no placeholders, and only use
  L.input_tuple_qtype. The prepared overload expression also
  contains checks for length and presence of elements in L.input_tuple_qtype.

  Args:
    signature: An operator signature.
    condition_expr: An overload condition.
  """
  overload_operator_helpers.check_signature_of_overload(signature)
  conditions = (
      overload_operator_helpers.get_input_tuple_length_validation_exprs(
          signature
      )
  )
  prepared_condition_expr, used_param_ids = (
      overload_operator_helpers.substitute_placeholders_in_condition_expr(
          signature, condition_expr
      )
  )
  conditions.append(prepared_condition_expr)
  conditions.append(
      overload_operator_helpers.get_overload_condition_readiness_expr(
          signature, used_param_ids
      )
  )
  presence_and = arolla_abc.lookup_operator('core.presence_and')
  return functools.reduce(presence_and, conditions)


class GenericOperator(arolla_abc.Operator):
  """QValue specialization for GenericOperator."""

  __slots__ = ()

  def __new__(
      cls,
      name: str,
      *,
      signature: arolla_abc.MakeOperatorSignatureArg,
      doc: str = '',
  ) -> Self:
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
  ) -> Self:
    """Creates an overload for a generic operator."""
    prepared_overload_condition_expr = _prepare_generic_overload_condition_expr(
        arolla_abc.get_operator_signature(base_operator),
        boxing.as_expr(overload_condition_expr),
    )
    return clib.make_generic_operator_overload(
        base_operator,
        prepared_overload_condition_expr,
    )


arolla_abc.register_qvalue_specialization(
    '::arolla::operator_loader::GenericOperator', GenericOperator
)
arolla_abc.register_qvalue_specialization(
    '::arolla::operator_loader::GenericOperatorOverload',
    GenericOperatorOverload,
)
