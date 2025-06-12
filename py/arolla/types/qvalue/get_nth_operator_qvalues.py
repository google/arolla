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

"""(Private) QValue specialisations for GetNthOperator."""

from __future__ import annotations

from arolla.abc import abc as arolla_abc
from arolla.types.qvalue import clib


class GetNthOperator(arolla_abc.Operator):
  """QValue specialization for LambdaOperator."""

  __slots__ = ()

  def __new__(cls, index: int) -> GetNthOperator:
    """Constructs a get_nth[index] operator.

    Args:
      index: The field index.

    Returns:
      Constructed operator.
    """
    return clib.make_get_nth_operator(index)

  @property
  def index(self) -> int:
    """Returns the index value."""
    return clib.get_nth_operator_index(self)


arolla_abc.register_qvalue_specialization(
    '::arolla::expr::GetNthOperator', GetNthOperator
)
