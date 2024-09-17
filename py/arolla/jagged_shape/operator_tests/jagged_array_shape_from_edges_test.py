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

"""Tests for M.jagged.array_shape_from_edges operator."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.jagged_shape import jagged_shape

M = arolla.OperatorsContainer(jagged_shape)
L = arolla.L

# Test data: tuple(
#     edges,
#     ...
# )
TEST_DATA = (
    (),
    (arolla.types.ArrayEdge.from_sizes([2]),),
    (
        arolla.types.ArrayEdge.from_sizes([2]),
        arolla.types.ArrayEdge.from_sizes([2, 1]),
    ),
    (arolla.types.ArrayEdge.from_mapping([0, 0, 0], parent_size=1),),
)


class JaggedArrayShapeFromEdgesTest(parameterized.TestCase):

  @parameterized.parameters(*TEST_DATA)
  def test_eval(self, *edges):
    self.assertEqual(
        arolla.eval(M.jagged.array_shape_from_edges(*edges)).qtype,
        jagged_shape.JAGGED_ARRAY_SHAPE,
    )

  def test_no_common_edge_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "arguments do not have a common type - expected_type: ARRAY_EDGE,"
            " *edges: (DENSE_ARRAY_EDGE)"
        ),
    ):
      M.jagged.array_shape_from_edges(
          arolla.types.DenseArrayEdge.from_sizes([2, 1])
      )

  def test_invalid_edge_error(self):
    with self.assertRaisesRegex(ValueError, "incompatible dimensions"):
      arolla.eval(
          M.jagged.array_shape_from_edges(
              arolla.types.ArrayEdge.from_sizes([1, 2])
          )
      )


if __name__ == "__main__":
  absltest.main()
