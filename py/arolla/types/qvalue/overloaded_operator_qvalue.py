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

"""(Private) QValue specialisations for OverloadedOperator.

Please avoid using this module directly. Use arolla (preferrably) or
arolla.types.types instead.
"""

from typing import Self

from arolla.abc import abc as rl_abc
from arolla.types.qvalue import clib


class OverloadedOperator(rl_abc.Operator):
  """QValue specialization for OverloadedOperator."""

  __slots__ = ()

  def __new__(
      cls,
      *base_operators: rl_abc.Operator,
      name: str = 'anonymous.overloaded_operator',
  ) -> Self:
    """Creates an overloaded operator from a given list of base operators.

    Overloaded operator is an adapter for a list of base operators. For each
    inputs it takes the first operator in the list that fits the case and
    apply it.

    Args:
      *base_operators: base operators.
      name: operator name.

    Returns:
      Constructed operator.
    """
    for op in base_operators:
      if op.qtype != rl_abc.OPERATOR:
        raise TypeError(f'expected operator, got {op.qtype}')
    return clib.make_overloaded_operator(name, base_operators)


rl_abc.register_qvalue_specialization(
    '::arolla::expr::OverloadedOperator', OverloadedOperator
)
