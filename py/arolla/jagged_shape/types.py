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

"""Python API for the JaggedShape types."""

from __future__ import annotations

import abc
from typing import Any, Generic, SupportsIndex, TypeVar

from arolla import arolla
from arolla.jagged_shape.operators import operators_clib as _

Edge = TypeVar('Edge', bound=arolla.QValue)


JAGGED_ARRAY_SHAPE = arolla.abc.invoke_op(
    'jagged.make_jagged_shape_qtype', (arolla.ARRAY_EDGE,)
)
JAGGED_DENSE_ARRAY_SHAPE = arolla.abc.invoke_op(
    'jagged.make_jagged_shape_qtype', (arolla.DENSE_ARRAY_EDGE,)
)


def is_jagged_shape_qtype(qtype: arolla.QType) -> bool:
  """Returns True iff qtype is a jagged shape qtype."""
  return bool(arolla.abc.invoke_op('jagged.is_jagged_shape_qtype', (qtype,)))


class _JaggedShape(arolla.QValue, Generic[Edge], metaclass=abc.ABCMeta):
  """Base QValue specialization for jagged shape qtypes."""

  __slots__ = ()

  def __new__(cls):
    raise NotImplementedError('please use the from_* classmethods instead')

  def rank(self) -> int:
    return int(arolla.abc.invoke_op('jagged.rank', (self,)))

  @classmethod
  @abc.abstractmethod
  def from_edges(cls, *edges: Edge) -> _JaggedShape[Edge]:
    raise NotImplementedError

  def edges(self) -> list[Edge]:
    return list(arolla.abc.invoke_op('jagged.edges', (self,)))

  def __getitem__(self, value: Any) -> Edge | _JaggedShape[Edge]:
    if isinstance(value, SupportsIndex):
      return arolla.abc.invoke_op('jagged.edge_at', (self, arolla.int64(value)))
    elif isinstance(value, slice):
      if value.start or value.step not in {None, 1}:
        raise ValueError(
            'jagged shape slicing must start at 0 (or None) and have a step'
            f' size of 1 (or None), got: (start, stop, step)={value}'
        )
      stop = self.rank() if value.stop is None else value.stop
      return arolla.abc.invoke_op(
          'jagged.remove_dims', (self, arolla.int64(stop))
      )
    else:
      raise TypeError(f'unsupported type: {type(value).__name__}')

  def __eq__(self, other: Any) -> bool:
    if not isinstance(other, type(self)):
      return NotImplemented
    return bool(arolla.abc.invoke_op('jagged.equal', (self, other)))

  def __ne__(self, other: Any) -> bool:
    if not isinstance(other, type(self)):
      return NotImplemented
    return not bool(arolla.abc.invoke_op('jagged.equal', (self, other)))


class JaggedArrayShape(_JaggedShape[arolla.types.ArrayEdge]):
  """QValue specialization for JAGGED_ARRAY_SHAPE qtypes."""

  @classmethod
  def from_edges(cls, *edges: arolla.types.ArrayEdge) -> JaggedArrayShape:
    return arolla.abc.invoke_op('jagged.array_shape_from_edges', edges)


class JaggedDenseArrayShape(_JaggedShape[arolla.types.DenseArrayEdge]):
  """QValue specialization for JAGGED_DENSE_ARRAY_SHAPE qtypes."""

  @classmethod
  def from_edges(
      cls, *edges: arolla.types.DenseArrayEdge
  ) -> JaggedDenseArrayShape:
    return arolla.abc.invoke_op('jagged.dense_array_shape_from_edges', edges)


arolla.abc.register_qvalue_specialization(JAGGED_ARRAY_SHAPE, JaggedArrayShape)
arolla.abc.register_qvalue_specialization(
    JAGGED_DENSE_ARRAY_SHAPE, JaggedDenseArrayShape
)
