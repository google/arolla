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

"""Tests for M.edge.as_dense_array_edge operator."""

import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.testing import testing

M = arolla.M


def _mapping_edge(array_factory, parent_size, child_size):
  mapping = array_factory(
      list(itertools.islice(itertools.cycle(range(parent_size)), child_size))
  )
  return arolla.eval(M.edge.from_mapping(mapping, parent_size))


def _split_points_edge(array_factory, parent_size, child_size):
  sizes = []
  if parent_size != 0:
    sizes = [child_size] + [0] * (parent_size - 1)
  return arolla.eval(M.edge.from_sizes(array_factory(sizes)))


def _to_scalar_edge(array_factory, child_size):
  return arolla.eval(M.edge.to_scalar(array_factory([None] * child_size)))


QTYPE_SIGNATURES = (
    (arolla.types.DENSE_ARRAY_EDGE, arolla.types.DENSE_ARRAY_EDGE),
    (
        arolla.types.DENSE_ARRAY_TO_SCALAR_EDGE,
        arolla.types.DENSE_ARRAY_TO_SCALAR_EDGE,
    ),
    (arolla.ARRAY_EDGE, arolla.types.DENSE_ARRAY_EDGE),
    (arolla.ARRAY_TO_SCALAR_EDGE, arolla.types.DENSE_ARRAY_TO_SCALAR_EDGE),
)

_SIZES = (0, 1, 10)
_ARRAY_FACTORIES = (arolla.dense_array_int64, arolla.array_int64)


class EdgeAsDenseArrayEdgeTest(parameterized.TestCase):

  def testQTypeSignatures(self):
    arolla.testing.assert_qtype_signatures(
        M.edge.as_dense_array_edge, QTYPE_SIGNATURES
    )

  @parameterized.parameters(
      [
          (
              _mapping_edge(array, p, c),
              _mapping_edge(arolla.dense_array_int64, p, c),
          )
          for p, c, array in itertools.product(_SIZES, _SIZES, _ARRAY_FACTORIES)
      ]
      + [
          (
              _split_points_edge(array, p, c),
              _split_points_edge(arolla.dense_array_int64, p, c),
          )
          for p, c, array in itertools.product(_SIZES, _SIZES, _ARRAY_FACTORIES)
      ]
      + [
          (
              _to_scalar_edge(array, c),
              _to_scalar_edge(arolla.dense_array_int64, c),
          )
          for c, array in itertools.product(_SIZES, _ARRAY_FACTORIES)
      ]
  )
  def testValue(self, arg, expected_output):
    testing.assert_qvalue_equal_by_fingerprint(
        arolla.eval(M.edge.as_dense_array_edge(arg)), expected_output
    )

  def testError(self):
    with self.assertRaisesRegex(
        ValueError, 'unsupported argument type SCALAR_TO_SCALAR_EDGE'
    ):
      _ = M.edge.as_dense_array_edge(arolla.types.ScalarToScalarEdge())


if __name__ == '__main__':
  absltest.main()
