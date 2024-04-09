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

"""(Private) Helpers for array qvalue specialisations."""

from typing import Any, SupportsIndex
from arolla.abc import abc as arolla_abc
from arolla.types.qtype import array_qtype


class BasicArray(arolla_abc.QValue):
  """Base class for qvalue specialisation for array types."""

  __slots__ = ('_size', '_present_count', '_value_qtype')

  _size: int
  _present_count: int
  _value_qtype: arolla_abc.QType

  @property
  def size(self) -> int:
    """Number of elements, including missing ones."""
    try:
      return self._size
    except AttributeError:
      self._size = int(arolla_abc.invoke_op('array.size', (self,)))
      return self._size

  @property
  def present_count(self) -> int:
    """Number of present elements, excluding missing ones."""
    try:
      return self._present_count
    except AttributeError:
      self._present_count = int(
          arolla_abc.invoke_op('array.count', (self, arolla_abc.unspecified()))
      )
      return self._present_count

  @property
  def value_qtype(self) -> arolla_abc.QType:
    """QType of values."""
    try:
      return self._value_qtype
    except AttributeError:
      self._value_qtype = self.qtype.value_qtype
      return self._value_qtype

  def __len__(self) -> int:
    return self.size

  def __getitem__(self, i: SupportsIndex) -> arolla_abc.AnyQValue:
    return array_qtype.get_array_item(self, i)

  def py_value(self) -> list[Any]:
    """Returns list of values with None for missing values."""
    return array_qtype.get_array_py_value(self)
