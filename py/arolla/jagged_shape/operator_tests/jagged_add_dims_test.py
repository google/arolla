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

"""Tests for M.jagged.add_dims operator."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.jagged_shape import jagged_shape

M = arolla.M | jagged_shape.M  # pyrefly: ignore[unsupported-operation]
L = arolla.L


def gen_test_data():
  # Test data: tuple(
  #     (input_shape, *edges, expected_shape),
  #     ...
  # )
  sizess = ([2], [2, 1], [2, 0, 1], [1, 2, 1], [3, 7, 1, 7])

  # ArrayShape.
  edges = [arolla.types.ArrayEdge.from_sizes(sizes) for sizes in sizess]
  shape = jagged_shape.JaggedArrayShape.from_edges(*edges)
  for i in range(len(edges) + 1):
    input_shape = jagged_shape.JaggedArrayShape.from_edges(*edges[:i])
    yield input_shape, *edges[i:], shape

  # DenseArrayShape.
  edges = [arolla.types.DenseArrayEdge.from_sizes(sizes) for sizes in sizess]
  shape = jagged_shape.JaggedDenseArrayShape.from_edges(*edges)
  for i in range(len(edges) + 1):
    input_shape = jagged_shape.JaggedDenseArrayShape.from_edges(*edges[:i])
    yield input_shape, *edges[i:], shape


TEST_DATA = tuple(gen_test_data())


class JaggedShapeFromEdgesTest(parameterized.TestCase):

  @parameterized.parameters(*TEST_DATA)
  def test_eval(self, *args_and_expected):
    args, expected_value = args_and_expected[:-1], args_and_expected[-1]
    actual_value = arolla.eval(M.jagged.add_dims(*args))  # pyrefly: ignore[missing-attribute]
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        actual_value, expected_value
    )

  def test_no_common_edge_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "arguments do not have a common type - expected_type:"
            " ARRAY_EDGE, *edges: (ARRAY_EDGE, DENSE_ARRAY_EDGE)"
        ),
    ):
      M.jagged.add_dims(  # pyrefly: ignore[missing-attribute]
          jagged_shape.JaggedArrayShape.from_edges(),
          arolla.types.ArrayEdge.from_sizes([2]),
          arolla.types.DenseArrayEdge.from_sizes([2, 1]),
      )

  def test_not_an_array_edge_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape("no suitable overload operator")
    ) as cm:
      M.jagged.add_dims(arolla.types.ArrayToScalarEdge(3))  # pyrefly: ignore[missing-attribute]
    self.assertTrue(
        arolla.testing.any_note_regex(
            re.escape("Input qtypes: shape: ARRAY_TO_SCALAR_EDGE")
        )(cm.exception)
    )
    self.assertTrue(
        arolla.testing.any_note_regex(
            re.escape("In generic operator: 'jagged.add_dims'.")
        )(cm.exception)
    )

  def test_invalid_edge_error(self):
    shape = jagged_shape.JaggedArrayShape.from_edges(
        arolla.types.ArrayEdge.from_sizes([2])
    )
    with self.assertRaisesRegex(ValueError, "incompatible dimensions"):
      arolla.eval(
          M.jagged.add_dims(shape, arolla.types.ArrayEdge.from_sizes([1]))  # pyrefly: ignore[missing-attribute]
      )


if __name__ == "__main__":
  absltest.main()
