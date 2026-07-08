# Copyright 2025 Google LLC
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

"""Tests for M.jagged.shape_from_edges operator."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.jagged_shape import jagged_shape

M = arolla.M | jagged_shape.M  # pyrefly: ignore[unsupported-operation]
L = arolla.L

# Test data: tuple(
#     (*edges, expected_qtype),
#     ...
# )
TEST_DATA = (
    (arolla.types.ArrayEdge.from_sizes([2]), jagged_shape.JAGGED_ARRAY_SHAPE),
    (
        arolla.types.ArrayEdge.from_sizes([2]),
        arolla.types.ArrayEdge.from_sizes([2, 1]),
        jagged_shape.JAGGED_ARRAY_SHAPE,
    ),
    (
        arolla.types.ArrayEdge.from_mapping([0, 0, 0], parent_size=1),
        jagged_shape.JAGGED_ARRAY_SHAPE,
    ),
    (
        arolla.types.DenseArrayEdge.from_sizes([2]),
        jagged_shape.JAGGED_DENSE_ARRAY_SHAPE,
    ),
    (
        arolla.types.DenseArrayEdge.from_sizes([2]),
        arolla.types.DenseArrayEdge.from_sizes([2, 1]),
        jagged_shape.JAGGED_DENSE_ARRAY_SHAPE,
    ),
    (
        arolla.types.DenseArrayEdge.from_mapping([0, 0, 0], parent_size=1),
        jagged_shape.JAGGED_DENSE_ARRAY_SHAPE,
    ),
)


class JaggedShapeFromEdgesTest(parameterized.TestCase):

  @parameterized.parameters(*TEST_DATA)
  def test_eval(self, *edges_and_res):
    edges, expected_qtype = edges_and_res[:-1], edges_and_res[-1]
    self.assertEqual(
        arolla.eval(M.jagged.shape_from_edges(*edges)).qtype, expected_qtype  # pyrefly: ignore[missing-attribute]
    )

  def test_no_common_edge_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "arguments do not have a common type - expected_type:"
            " ARRAY_EDGE, *edges: (ARRAY_EDGE, DENSE_ARRAY_EDGE)"
        ),
    ):
      M.jagged.shape_from_edges(  # pyrefly: ignore[missing-attribute]
          arolla.types.ArrayEdge.from_sizes([2]),
          arolla.types.DenseArrayEdge.from_sizes([2, 1]),
      )

  def test_not_an_array_edge_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape("no suitable overload operator")
    ) as cm:
      M.jagged.shape_from_edges(arolla.types.ArrayToScalarEdge(3))  # pyrefly: ignore[missing-attribute]
    self.assertTrue(
        arolla.testing.any_note_regex(
            re.escape("Input qtypes: edge: ARRAY_TO_SCALAR_EDGE, *edges: ().")
        )(cm.exception)
    )
    self.assertTrue(
        arolla.testing.any_note_regex(
            re.escape("In generic operator: 'jagged.shape_from_edges'.")
        )(cm.exception)
    )

  def test_invalid_edge_error(self):
    with self.assertRaisesRegex(ValueError, "incompatible dimensions"):
      arolla.eval(
          M.jagged.shape_from_edges(arolla.types.ArrayEdge.from_sizes([1, 2]))  # pyrefly: ignore[missing-attribute]
      )


if __name__ == "__main__":
  absltest.main()
