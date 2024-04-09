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

"""(Private) QValue specialisations for array types.

Please avoid using this module directly. Use arolla (preferrably) or
arolla.types.types instead.
"""

import functools
from typing import Any, Self, SupportsIndex, TypeVar

from arolla.abc import abc as rl_abc
from arolla.types.qtype import array_qtype as rl_array_qtype
from arolla.types.qtype import boxing as rl_boxing
from arolla.types.qtype import dense_array_qtype as rl_dense_array_qtype
from arolla.types.qtype import scalar_qtype as rl_scalar_qtype
from arolla.types.qvalue import basic_array_helpers as rl_basic_array_helpers
from arolla.types.qvalue import qvalue_mixins as rl_qvalue_mixins


# We purposfully do _not_ use typing.Sequence, as neither
# rl_basic_array_helpers.BasicArray nor np.ndarray implement it.
IntSequence = Any
TSequence = TypeVar('TSequence')

_present_values_as_dense_array_expr = rl_abc.unsafe_parse_sexpr(
    ('array.as_dense_array', ('array.present_values', rl_abc.leaf('array')))
)
_present_indices_as_dense_array_expr = rl_abc.unsafe_parse_sexpr(
    ('array.as_dense_array', ('array.present_indices', rl_abc.leaf('array')))
)


class Array(
    rl_qvalue_mixins.PresenceQValueMixin, rl_basic_array_helpers.BasicArray
):
  """QValue specialization for array qtype.

  Array is an immutable array with support of missed values. Implemented
  on top of DenseArray. It efficiently represents very sparse data and
  constants, but has bigger fixed overhead than DenseArray.

  The implementation of Arrays: //arolla/array/array.h
  """

  __slots__ = ()

  def to_ids_and_values(self) -> tuple[list[int], list[Any]]:
    """Returns aligned lists of ids and values."""
    values = rl_abc.invoke_op('array.present_values', (self,))
    indices = rl_abc.invoke_op('array.present_indices', (self,))
    return indices.py_value(), values.py_value()

  def to_ids_and_values_as_numpy_arrays(self):
    """Returns aligned lists of ids and values as numpy arrays."""
    values = rl_dense_array_qtype.numpy_ndarray_from_dense_array(
        rl_abc.eval_expr(_present_values_as_dense_array_expr, dict(array=self))
    )
    indices = rl_dense_array_qtype.numpy_ndarray_from_dense_array(
        rl_abc.eval_expr(_present_indices_as_dense_array_expr, dict(array=self))
    )
    return indices, values


class ArrayInt(rl_qvalue_mixins.IntegralArithmeticQValueMixin, Array):
  """QValue specialisation for ARRAY_INTs."""

  __slots__ = ()


class ArrayFloat(rl_qvalue_mixins.FloatingPointArithmeticQValueMixin, Array):
  """QValue specialisation for ARRAY_FLOATs."""

  __slots__ = ()


class ArrayEdge(rl_abc.QValue):
  """QValue specialization for ARRAY_EDGE qtype."""

  __slots__ = ()

  def __new__(cls):
    raise NotImplementedError('please use ArrayEdge.from_* instead')

  @classmethod
  def from_sizes(cls, sizes: IntSequence) -> Self:
    """Returns a ArrayEdge from group sizes."""
    return rl_abc.invoke_op(
        'edge.from_sizes', (rl_array_qtype.array_int64(sizes),)
    )

  @classmethod
  def from_split_points(cls, split_points: IntSequence) -> Self:
    """Returns a DenseArrayEdge from group split points."""
    return rl_abc.invoke_op(
        'edge.from_split_points', (rl_array_qtype.array_int64(split_points),)
    )

  @classmethod
  def from_mapping(
      cls, mapping: IntSequence, parent_size: SupportsIndex
  ) -> Self:
    """Returns a ArrayEdge from a mapping of each child to a parent.

    Args:
      mapping: Maps each child element i to a parent value mapping[i].
      parent_size: The number of parents.
    """
    return rl_abc.invoke_op(
        'edge.from_mapping',
        (
            rl_array_qtype.array_int64(mapping),
            rl_scalar_qtype.int64(parent_size),
        ),
    )

  @classmethod
  def from_keys(cls, child_keys: TSequence, parent_keys: TSequence) -> Self:
    """Returns a ArrayEdge from child_keys to parent_keys.

    Args:
      child_keys: Maps each child element i to an index j, where child_keys[i]
        == parent_keys[j].
      parent_keys: The ids of the parents.
    """
    return rl_abc.invoke_op(
        'edge.from_keys',
        (rl_boxing.array(child_keys), rl_boxing.array(parent_keys)),
    )

  @property
  def parent_size(self) -> int:
    return int(rl_abc.invoke_op('edge.parent_size', (self,)))

  @property
  def child_size(self) -> int:
    return int(rl_abc.invoke_op('edge.child_size', (self,)))

  def mapping(self) -> rl_abc.AnyQValue:
    return rl_abc.invoke_op('edge.mapping', (self,))


# Note: We use lazy evaluation, just in case the corresponding operators are
# unavailable (or not yet available) in the current environment.
_empty_array_empty_shape_fn = functools.cache(
    functools.partial(rl_abc.invoke_op, 'qtype._const_empty_array_shape')
)


class ArrayShape(rl_abc.QValue):
  """QValue specialization for ARRAY_SHAPE qtype."""

  __slots__ = ()

  def __new__(cls, size: SupportsIndex) -> Self:
    """Returns a dense array shape of the given size."""
    return rl_abc.invoke_op(
        'array.resize_array_shape',
        (_empty_array_empty_shape_fn(), rl_scalar_qtype.int64(size)),
    )

  @property
  def size(self) -> int:
    return int(rl_abc.invoke_op('array.array_shape_size', (self,)))


class ArrayToScalarEdge(rl_abc.QValue):
  """QValue specialization for ARRAY_TO_SCALAR_EDGE qtype."""

  __slots__ = ()

  def __new__(cls, size: SupportsIndex) -> Self:
    return rl_abc.invoke_op('edge.from_shape', (ArrayShape(size),))

  @property
  def child_size(self) -> int:
    return int(rl_abc.invoke_op('edge.child_size', (self,)))


def _register_qvalue_specializations():
  """Registers qvalue specializations for array types."""
  for qtype in rl_scalar_qtype.SCALAR_QTYPES:
    if qtype in rl_scalar_qtype.INTEGRAL_QTYPES:
      rl_abc.register_qvalue_specialization(
          rl_array_qtype.make_array_qtype(qtype), ArrayInt
      )
    elif qtype in rl_scalar_qtype.FLOATING_POINT_QTYPES:
      rl_abc.register_qvalue_specialization(
          rl_array_qtype.make_array_qtype(qtype), ArrayFloat
      )
    else:
      rl_abc.register_qvalue_specialization(
          rl_array_qtype.make_array_qtype(qtype), Array
      )
  rl_abc.register_qvalue_specialization(rl_array_qtype.ARRAY_UINT64, ArrayInt)
  rl_abc.register_qvalue_specialization(rl_array_qtype.ARRAY_EDGE, ArrayEdge)
  rl_abc.register_qvalue_specialization(rl_array_qtype.ARRAY_SHAPE, ArrayShape)
  rl_abc.register_qvalue_specialization(
      rl_array_qtype.ARRAY_TO_SCALAR_EDGE, ArrayToScalarEdge
  )


_register_qvalue_specializations()
