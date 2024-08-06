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

"""(Private) QValue specialisations for dense_array types.

Please avoid using this module directly. Use arolla.rl (preferrably) or
arolla.types.types instead.
"""

import functools
from typing import Any, Self, SupportsIndex, TypeVar

from arolla.abc import abc as arolla_abc
from arolla.types.qtype import boxing as rl_boxing
from arolla.types.qtype import dense_array_qtype as rl_dense_array_qtype
from arolla.types.qtype import scalar_qtype as rl_scalar_qtype
from arolla.types.qvalue import basic_array_helpers as rl_basic_array_helpers
from arolla.types.qvalue import qvalue_mixins as rl_qvalue_mixins

# We purposfully do _not_ use typing.Sequence, as neither rl_array_helpers.Array
# nor np.ndarray implement it.
IntSequence = Any
TSequence = TypeVar('TSequence')


class DenseArray(
    rl_qvalue_mixins.PresenceQValueMixin, rl_basic_array_helpers.BasicArray
):
  """QValue specialization for dense_array qtype.

  DenseArray supports missed values. Analogue of std::vector<OptionalValue<T>>.
  It is implemented on top of Buffer<T>, so values are immutable.

  Additional information on DenseArray:
      //arolla/dense_array/README.md
  """

  __slots__ = ()


class DenseArrayInt(rl_qvalue_mixins.IntegralArithmeticQValueMixin, DenseArray):
  """QValue specialisation for DENSE_ARRAY_INTs."""

  __slots__ = ()


class DenseArrayFloat(
    rl_qvalue_mixins.FloatingPointArithmeticQValueMixin, DenseArray
):
  """QValue specialisation for DENSE_ARRAY_FLOATs."""

  __slots__ = ()


class DenseArrayEdge(arolla_abc.QValue):
  """QValue specialization for DENSE_ARRAY_EDGE qtype."""

  __slots__ = ()

  def __new__(cls):
    raise NotImplementedError('please use DenseArrayEdge.from_* instead')

  @classmethod
  def from_sizes(cls, sizes: IntSequence) -> Self:
    """Returns a DenseArrayEdge from group sizes."""
    return arolla_abc.invoke_op(
        'edge.from_sizes', (rl_dense_array_qtype.dense_array_int64(sizes),)
    )

  @classmethod
  def from_split_points(cls, split_points: IntSequence) -> Self:
    """Returns a DenseArrayEdge from group split points."""
    return arolla_abc.invoke_op(
        'edge.from_split_points',
        (rl_dense_array_qtype.dense_array_int64(split_points),),
    )

  @classmethod
  def from_mapping(
      cls, mapping: IntSequence, parent_size: SupportsIndex
  ) -> Self:
    """Returns a DenseArrayEdge from a mapping of each child to a parent.

    Args:
      mapping: Maps each child element i to a parent value mapping[i].
      parent_size: The number of parents.
    """
    return arolla_abc.invoke_op(
        'edge.from_mapping',
        (
            rl_dense_array_qtype.dense_array_int64(mapping),
            rl_scalar_qtype.int64(parent_size),
        ),
    )

  @classmethod
  def from_keys(cls, child_keys: TSequence, parent_keys: TSequence) -> Self:
    """Returns a DenseArrayEdge from child_keys to parent_keys.

    Args:
      child_keys: Maps each child element i to an index j, where child_keys[i]
        == parent_keys[j].
      parent_keys: The ids of the parents.
    """
    return arolla_abc.invoke_op(
        'edge.from_keys',
        (rl_boxing.dense_array(child_keys), rl_boxing.dense_array(parent_keys)),
    )

  @property
  def parent_size(self) -> int:
    return int(arolla_abc.invoke_op('edge.parent_size', (self,)))

  @property
  def child_size(self) -> int:
    return int(arolla_abc.invoke_op('edge.child_size', (self,)))

  def mapping(self) -> arolla_abc.AnyQValue:
    return arolla_abc.invoke_op('edge.mapping', (self,))


# Note: We use lazy evaluation, just in case the corresponding operators are
# unavailable (or not yet available) in the current environment.
_empty_dense_array_empty_shape_fn = functools.cache(
    functools.partial(
        arolla_abc.invoke_op, 'qtype._const_empty_dense_array_shape'
    )
)


class DenseArrayShape(arolla_abc.QValue):
  """QValue specialization for DENSE_ARRAY_SHAPE qtype."""

  __slots__ = ()

  def __new__(cls, size: SupportsIndex) -> Self:
    """Returns a dense array shape of the given size."""
    return arolla_abc.invoke_op(
        'array.make_dense_array_shape', (rl_scalar_qtype.int64(size),)
    )

  @property
  def size(self) -> int:
    return int(arolla_abc.invoke_op('array.array_shape_size', (self,)))


class DenseArrayToScalarEdge(arolla_abc.QValue):
  """QValue specialization for DENSE_ARRAY_TO_SCALAR_EDGE qtype."""

  __slots__ = ()

  def __new__(cls, size: SupportsIndex) -> Self:
    return arolla_abc.invoke_op('edge.from_shape', (DenseArrayShape(size),))

  @property
  def child_size(self) -> int:
    return int(arolla_abc.invoke_op('edge.child_size', (self,)))


def _register_qvalue_specializations():
  """Registers qvalue specializations for dense_array types."""
  for qtype in rl_scalar_qtype.SCALAR_QTYPES:
    if qtype in rl_scalar_qtype.INTEGRAL_QTYPES:
      arolla_abc.register_qvalue_specialization(
          rl_dense_array_qtype.make_dense_array_qtype(qtype), DenseArrayInt
      )
    elif qtype in rl_scalar_qtype.FLOATING_POINT_QTYPES:
      arolla_abc.register_qvalue_specialization(
          rl_dense_array_qtype.make_dense_array_qtype(qtype), DenseArrayFloat
      )
    else:
      arolla_abc.register_qvalue_specialization(
          rl_dense_array_qtype.make_dense_array_qtype(qtype), DenseArray
      )
  arolla_abc.register_qvalue_specialization(
      rl_dense_array_qtype.DENSE_ARRAY_UINT64, DenseArrayInt
  )
  arolla_abc.register_qvalue_specialization(
      rl_dense_array_qtype.DENSE_ARRAY_EDGE, DenseArrayEdge
  )
  arolla_abc.register_qvalue_specialization(
      rl_dense_array_qtype.DENSE_ARRAY_SHAPE, DenseArrayShape
  )
  arolla_abc.register_qvalue_specialization(
      rl_dense_array_qtype.DENSE_ARRAY_TO_SCALAR_EDGE, DenseArrayToScalarEdge
  )


_register_qvalue_specializations()
