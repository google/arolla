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

"""(Private) QValue specialisations for DummyOperator."""

from __future__ import annotations

from arolla.abc import abc as arolla_abc
from arolla.types.qtype import boxing
from arolla.types.qvalue import clib


class DummyOperator(arolla_abc.Operator):
  """QValue specialization for DummyOperator."""

  __slots__ = ()

  def __new__(
      cls,
      name: str,
      signature: arolla_abc.MakeOperatorSignatureArg,
      *,
      doc: str = '',
      result_qtype: arolla_abc.QType,
  ) -> DummyOperator:
    """Constructs a dummy operator.

    Args:
      name: operator name and dummy identifier. This value will be used as
        `operator_key`.
      signature: operator signature.
      doc: operator doc-string.
      result_qtype: The result qtype.

    Returns:
      Constructed operator.
    """
    prepared_signature = arolla_abc.make_operator_signature(
        signature, as_qvalue=boxing.as_qvalue
    )
    return clib.make_dummy_operator(name, prepared_signature, doc, result_qtype)


arolla_abc.register_qvalue_specialization(
    '::arolla::operator_loader::DummyOperator', DummyOperator
)
