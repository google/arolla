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

"""Tests for M.edge.child_size."""

import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

M = arolla.M

QTYPE_SIGNATURES = (
    (arolla.types.DENSE_ARRAY_EDGE, arolla.INT64),
    (arolla.types.DENSE_ARRAY_TO_SCALAR_EDGE, arolla.INT64),
    (arolla.types.ARRAY_EDGE, arolla.INT64),
    (arolla.types.ARRAY_TO_SCALAR_EDGE, arolla.INT64),
)

_SIZES = (0, 1, 10)


class ArrayChildSize(parameterized.TestCase):

  def testQTypeSignatures(self):
    arolla.testing.assert_qtype_signatures(M.edge.child_size, QTYPE_SIGNATURES)

  @parameterized.parameters(
      [
          (arolla.types.DenseArrayEdge.from_mapping([None] * c, p), c)
          for p, c in itertools.product(_SIZES, _SIZES)
      ]
      + [
          (arolla.types.ArrayEdge.from_mapping([None] * c, p), c)
          for p, c in itertools.product(_SIZES, _SIZES)
      ]
      + [(arolla.types.DenseArrayToScalarEdge(c), c) for c in _SIZES]
      + [(arolla.types.ArrayToScalarEdge(c), c) for c in _SIZES]
  )
  def testValue(self, arg, expected_output):
    self.assertEqual(arolla.eval(M.edge.child_size(arg)), expected_output)

  def testError(self):
    with self.assertRaisesRegex(
        ValueError, 'edge: SCALAR_TO_SCALAR_EDGE is not supported'
    ):
      _ = M.edge.child_size(arolla.types.ScalarToScalarEdge())


if __name__ == '__main__':
  absltest.main()
