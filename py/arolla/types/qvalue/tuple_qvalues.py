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

"""(Private) QValue specialisations for tuple types."""

from __future__ import annotations

import collections
import functools
from typing import Any, SupportsIndex, Type

from arolla.abc import abc as arolla_abc
from arolla.types.qtype import tuple_qtypes


class Tuple(arolla_abc.QValue):
  """QValue specialization for Tuple qtypes."""

  __slots__ = ('_field_count',)

  _field_count: int

  @property
  def field_count(self) -> int:
    try:
      return self._field_count
    except AttributeError:
      self._field_count = int(
          arolla_abc.invoke_op('qtype.get_field_count', (self.qtype,))
      )
      return self._field_count

  def __len__(self) -> int:
    return self.field_count

  def __getitem__(self, key: SupportsIndex | slice) -> arolla_abc.AnyQValue:
    if isinstance(key, slice):
      return self._internal_slice(key)
    return self._internal_getitem(key)

  def _internal_slice(self, key: slice) -> Tuple:
    indices = range(self.field_count).__getitem__(key)
    return tuple_qtypes.make_tuple_qvalue(
        *(tuple_qtypes.get_nth(self, i) for i in indices)
    )

  def _internal_getitem(self, key: SupportsIndex) -> arolla_abc.AnyQValue:
    try:
      i = key.__index__()
    except AttributeError as ex:
      raise TypeError(
          f'non-index type: {arolla_abc.get_type_name(type(key))}'
      ) from ex
    field_count = self.field_count
    if i < -self.field_count or i >= self.field_count:
      raise IndexError(f'index out of range: {i}')
    if i < 0:
      i += field_count
    return tuple_qtypes.get_nth(self, i)

  def py_value(self):
    """Returns a python tuple with py_values."""
    return tuple(
        tuple_qtypes.get_nth(self, i).py_value()
        for i in range(self.field_count)
    )


@functools.lru_cache
def _named_tuple_cls_from_qtype(qtype: arolla_abc.QType) -> Type[Any]:
  """Returns collections.namedtuple class with the same fields as in qtype."""
  return collections.namedtuple(
      'NamedTuple', tuple_qtypes.get_namedtuple_field_names(qtype)
  )


arolla_abc.cache_clear_callbacks.add(
    _named_tuple_cls_from_qtype.cache_clear
)  # subscribe the lru_cache for cleaning


class NamedTuple(arolla_abc.QValue):
  """QValue specialization for NamedTuple qtypes."""

  __slots__ = ()

  def keys(self) -> list[str]:
    """Returns the field names in the order as they appear in the tuple."""
    return tuple_qtypes.get_namedtuple_field_names(self.qtype)

  def __contains__(self, field: str) -> bool:
    return (
        tuple_qtypes.get_namedtuple_field_index(self.qtype, field) is not None
    )

  def __getitem__(self, field: str) -> arolla_abc.AnyQValue:
    if not isinstance(field, str):
      raise TypeError(
          'non-str type: {}, {}'.format(
              arolla_abc.get_type_name(type(field)), field
          )
      )
    field_index = tuple_qtypes.get_namedtuple_field_index(self.qtype, field)
    if field_index is None:
      raise KeyError(f'`{field}` is not found in `{self.qtype}`')
    return tuple_qtypes.get_nth(self, field_index)

  def as_dict(self):
    """Returns a dict with the field values."""
    keys = self.keys()
    return {keys[i]: tuple_qtypes.get_nth(self, i) for i in range(len(keys))}

  def py_value(self):
    """Returns a python tuple with py_values."""
    cls = _named_tuple_cls_from_qtype(self.qtype)
    return cls._make(
        tuple_qtypes.get_nth(self, i).py_value()
        for i in range(len(cls._fields))
    )


# Register qvalue specializations for tuple types.
arolla_abc.register_qvalue_specialization('::arolla::TupleQType', Tuple)
arolla_abc.register_qvalue_specialization(
    '::arolla::NamedTupleQType', NamedTuple
)
