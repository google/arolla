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

"""Tests for M.jagged.flatten operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.jagged_shape import jagged_shape
from arolla.operator_tests import pointwise_test_utils

M = arolla.OperatorsContainer(jagged_shape)
L = arolla.L


def gen_test_data_for_range(
    shape_factory, edge_factory, shape_sizes, from_dim, to_dim, res_sizes
):
  shape = shape_factory(*(edge_factory(s) for s in shape_sizes))
  res = shape_factory(*(edge_factory(s) for s in res_sizes))
  if from_dim is None:
    yield shape, res
    return
  if to_dim is None:
    yield shape, arolla.int32(from_dim), res
    yield shape, arolla.int64(from_dim), res
  elif to_dim == arolla.unspecified():
    yield shape, arolla.int32(from_dim), to_dim, res
    yield shape, arolla.int64(from_dim), to_dim, res
  else:
    yield shape, arolla.int32(from_dim), arolla.int32(to_dim), res
    yield shape, arolla.int32(from_dim), arolla.int64(to_dim), res
    yield shape, arolla.int64(from_dim), arolla.int32(to_dim), res
    yield shape, arolla.int64(from_dim), arolla.int64(to_dim), res


# Test data: tuple(
#     (shape, result),
#     (shape, from_dim, result),
#     (shape, from_dim, to_dim, result),
#     ...
# )
def gen_test_data():
  shape_sizes = ([2], [2, 1], [2, 1, 2])
  for case in (
      ((), None, None, ([1],)),
      (shape_sizes, None, None, ([5],)),
      (shape_sizes, 1, None, ([2], [3, 2])),
      (shape_sizes, -1, None, ([2], [2, 1], [2, 1, 2])),
      (shape_sizes, -2, None, ([2], [3, 2])),
      (shape_sizes, 1, arolla.unspecified(), ([2], [3, 2])),
      (shape_sizes, 0, 2, ([3], [2, 1, 2])),
      (shape_sizes, 1, 1, ([2], [1, 1], [2, 1], [2, 1, 2])),
      (shape_sizes, 2, 2, ([2], [2, 1], [1, 1, 1], [2, 1, 2])),
      (shape_sizes, 1, 0, ([2], [1, 1], [2, 1], [2, 1, 2])),
      (shape_sizes, -2, 5, ([2], [3, 2])),
      (shape_sizes, -5, -1, ([3], [2, 1, 2])),
      (([1],) * 100, 5, 17, ([1],) * (100 - (17 - 5) + 1)),
  ):
    yield from gen_test_data_for_range(
        jagged_shape.JaggedArrayShape.from_edges,
        arolla.types.ArrayEdge.from_sizes,
        *case
    )
    yield from gen_test_data_for_range(
        jagged_shape.JaggedDenseArrayShape.from_edges,
        arolla.types.DenseArrayEdge.from_sizes,
        *case
    )


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = frozenset(
    tuple(arg.qtype for arg in args) for args in TEST_DATA
)


class JaggedFlattenTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    possible_qtypes = pointwise_test_utils.DETECT_SIGNATURES_DEFAULT_QTYPES + (
        jagged_shape.JAGGED_ARRAY_SHAPE,
        jagged_shape.JAGGED_DENSE_ARRAY_SHAPE,
    )
    self.assertCountEqual(
        pointwise_test_utils.detect_qtype_signatures(
            M.jagged.flatten, possible_qtypes=possible_qtypes
        ),
        QTYPE_SIGNATURES,
    )

  @parameterized.parameters(*TEST_DATA)
  def test_eval(self, *args_and_expected):
    args, expected_value = args_and_expected[:-1], args_and_expected[-1]
    actual_value = arolla.eval(M.jagged.flatten(*args))
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        actual_value, expected_value
    )

  def test_non_shape_error(self):
    with self.assertRaisesRegex(
        ValueError, 'expected a jagged shape, got: shape: QTYPE'
    ):
      M.jagged.flatten(arolla.INT32, 1, 1)


if __name__ == '__main__':
  absltest.main()
