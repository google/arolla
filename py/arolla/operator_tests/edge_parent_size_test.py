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

"""Tests for M.edge.parent_size."""

import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

M = arolla.M


def _dense_array_edge(parent_size, child_size):
  return arolla.types.DenseArrayEdge.from_mapping(
      [None] * child_size, parent_size
  )


def _array_edge(parent_size, child_size):
  return arolla.types.ArrayEdge.from_mapping([None] * child_size, parent_size)


QTYPE_SIGNATURES = (
    (arolla.types.DENSE_ARRAY_EDGE, arolla.INT64),
    (arolla.ARRAY_EDGE, arolla.INT64),
)

_SIZES = (0, 1, 10)


class ArrayParentSize(parameterized.TestCase):

  def testQTypeSignatures(self):
    arolla.testing.assert_qtype_signatures(M.edge.parent_size, QTYPE_SIGNATURES)

  @parameterized.parameters(
      [
          (_dense_array_edge(p, c), p)
          for p, c in itertools.product(_SIZES, _SIZES)
      ]
      + [(_array_edge(p, c), p) for p, c in itertools.product(_SIZES, _SIZES)]
  )
  def testValue(self, arg, expected_output):
    self.assertEqual(arolla.eval(M.edge.parent_size(arg)), expected_output)

  @parameterized.parameters(
      [arolla.types.DenseArrayToScalarEdge(c) for c in _SIZES]
      + [arolla.types.ArrayToScalarEdge(c) for c in _SIZES]
      + [arolla.types.ScalarToScalarEdge()]
  )
  def testError(self, arg):
    with self.assertRaisesRegex(
        ValueError, f'edge: {arg.qtype} is not supported'
    ):
      _ = M.edge.parent_size(arg)


if __name__ == '__main__':
  absltest.main()
