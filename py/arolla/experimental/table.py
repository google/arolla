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

"""A simple mutable table abstraction."""

from typing import Any as _AnyType
from typing import Optional

from arolla import arolla
from arolla.experimental import dataset as arolla_dataset


def _array_range(n: int):
  """Returns a array with range(0, n) values."""
  return arolla.abc.invoke_op('array.iota', (arolla.types.ArrayShape(n),))


class Table(arolla_dataset.DataSet):
  """A simple mutable table abstraction."""

  def _init_row_count(self, row_count: int) -> None:
    """Initializes row_count."""
    if not isinstance(row_count, int):
      raise TypeError(
          'expected int, got {}'.format(
              arolla.abc.get_type_name(type(row_count))
          )
      )
    super().__setattr__('id', _array_range(row_count))
    self._row_count = row_count

  def __init__(self, *, row_count: int | None = None, **columns: _AnyType):
    """Constructor.

    Args:
      row_count: Number of rows in the table.
      **columns: Table data.
    """
    super().__init__()
    super().__setattr__('id', _array_range(0))
    self._row_count = None
    if row_count is not None:
      self._init_row_count(row_count)
    for key, value in columns.items():
      setattr(self, key, value)

  def row_count(self) -> Optional[int]:
    """Returns the number of rows in the table."""
    return self._row_count

  def __setattr__(self, key: str, value: _AnyType) -> None:
    """Assigns a column."""
    if key.startswith('_'):
      return super().__setattr__(key, value)
    if key == 'id':
      raise AttributeError("cannot overwrite 'id'")
    if not isinstance(value, arolla.QValue):
      try:
        value = arolla.array(value)
      except TypeError:
        raise TypeError(
            'expected an array, got {}'.format(
                arolla.abc.get_type_name(type(value))
            )
        ) from None
    if not arolla.is_array_qtype(value.qtype):
      raise TypeError(f'expected an array, got {value.qtype}')
    if self._row_count is None:
      self._init_row_count(len(value))
    elif self._row_count != len(value):
      raise ValueError(
          f'expected len(array) = {self._row_count}, got {len(value)}'
      )
    super().__setattr__(key, value)

  def __delattr__(self, key: str) -> None:
    """Deletes a column."""
    if key == 'id':
      raise AttributeError("cannot delete 'id'")
    super().__delattr__(key)
