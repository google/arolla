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

"""(Private) QValue specialisations for BackendOperator."""

from __future__ import annotations

from arolla.abc import abc as arolla_abc
from arolla.types.qtype import boxing
from arolla.types.qvalue import clib
from arolla.types.qvalue import helpers as arolla_types_qvalue_helpers


class BackendOperator(arolla_abc.Operator):
  """QValue specialization for BackendOperator."""

  __slots__ = ()

  def __new__(
      cls,
      name: str,
      signature: arolla_abc.MakeOperatorSignatureArg,
      *,
      doc: str = '',
      qtype_constraints: arolla_types_qvalue_helpers.QTypeConstraints = (),
      qtype_inference_expr: arolla_abc.QType | arolla_abc.Expr,
  ) -> BackendOperator:
    """Constructs a backend operator.

    Args:
      name: backend operator name.
      signature: operator signature.
      doc: operator doc-string.
      qtype_constraints: an optional list of (predicate_expr, error_message)
        pairs. If a qtype constraint is not fulfilled, the corresponding
        error_message is used. Placeholders, like {arg_name}, get replaced with
        the actual type names during the error message formatting.
      qtype_inference_expr: expression that computes operator's output qtype; an
        argument qtype can be referenced as P.arg_name.

    Returns:
      Constructed operator.
    """
    prepared_signature = arolla_abc.make_operator_signature(
        signature, as_qvalue=boxing.as_qvalue
    )
    prepared_qtype_constraints = (
        arolla_types_qvalue_helpers.prepare_qtype_constraints(qtype_constraints)
    )
    prepared_qtype_inference_expr = boxing.as_expr(qtype_inference_expr)
    return clib.make_backend_operator(
        name,
        prepared_signature,
        doc,
        prepared_qtype_constraints,
        prepared_qtype_inference_expr,
    )


arolla_abc.register_qvalue_specialization(
    '::arolla::operator_loader::BackendOperator', BackendOperator
)
