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

"""(Private) QValue specialisations for sequence types.

Please avoid using this module directly. Use arolla.rl (preferrably) or
arolla.types.types instead.
"""

from typing import Any, Self

from arolla.abc import abc as arolla_abc
from arolla.types.qtype import boxing as rl_boxing
from arolla.types.qtype import scalar_qtype as rl_scalar_qtype
from arolla.types.qtype import sequence_qtype as rl_sequence_qtype


class Sequence(arolla_abc.QValue):
  """QValue specialization for sequence qtype."""

  __slots__ = ('_size',)

  _size: int

  def __new__(
      cls, *values: Any, value_qtype: arolla_abc.QType | None = None
  ) -> Self:
    """Constructs a sequence qvalue.

    Args:
      *values: Values.
      value_qtype: Value qtype.

    Returns:
      A value qtype.
    """
    return rl_sequence_qtype.make_sequence_qvalue(
        list(map(rl_boxing.as_qvalue, values)), value_qtype=value_qtype
    )

  @property
  def size(self) -> int:
    """Number of elements."""
    try:
      return self._size
    except AttributeError:
      self._size = int(arolla_abc.invoke_op('seq.size', (self,)))
      return self._size

  @property
  def value_qtype(self) -> arolla_abc.QType:
    """QType of values."""
    return self.qtype.value_qtype

  def __len__(self) -> int:
    return self.size

  def __getitem__(self, i) -> arolla_abc.AnyQValue:
    try:
      i = i.__index__()
    except AttributeError:
      raise TypeError(
          f'non-index type: {arolla_abc.get_type_name(type(i))}'
      ) from None
    size = self.size
    if not -size <= i < size:
      raise IndexError(f'index out of range: {i}')
    if i < 0:
      i += size
    return arolla_abc.invoke_op('seq.at', (self, rl_scalar_qtype.int64(i)))


# Register qvalue specializations for tuple types.
arolla_abc.register_qvalue_specialization('::arolla::SequenceQType', Sequence)
