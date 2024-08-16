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

"""Tests for M.edge.sizes."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base

M = arolla.M

# [sizes, ...]
# where edge.sizes(edge.from_sizes(sizes)) = sizes.
EDGE_FROM_SIZES_TEST_DATA = (
    [],
    [0, 0],
    [0, 1, 0, 0, 2],
    [10, 10, 10],
)

# [(mapping, parent_size, expected), ...)
# where edge.sizes(edge.from_mapping(mapping, parent_size)) = expected.
EDGE_FROM_MAPPING_TEST_DATA = (
    ([0, 0, 0, 1, 1, 1, 1, 3, 3], 5, [3, 4, 0, 2, 0]),
    ([0, 2, 4, 2, 1, 2, 1], 5, [1, 2, 3, 0, 1]),
    ([None, 2, 4, 2, None, 2, 1], 5, [0, 1, 3, 0, 1]),
)

# [(arg, expected), ...]
# where edge.sizes(edge.to_scalar(arg)) = expected.
EDGE_TO_SCALAR_TEST_DATA = (
    ([], 0),
    ([0.0], 1),
    ([1.0, None, 2.0], 3),
)


def gen_cases():
  """Yields test cases.

  Yields: (*arg_qvalues, result_qvalue)
  """
  for array_fn in (arolla.array, arolla.dense_array):
    for sizes in EDGE_FROM_SIZES_TEST_DATA:
      sizes = array_fn(sizes, arolla.INT64)
      edge = arolla.abc.invoke_op('edge.from_sizes', (sizes,))
      yield (edge, sizes)
    for mapping, parent_size, expected in EDGE_FROM_MAPPING_TEST_DATA:
      mapping = array_fn(mapping)
      parent_size = arolla.int64(parent_size)
      expected = array_fn(expected, arolla.INT64)
      edge = arolla.abc.invoke_op('edge.from_mapping', (mapping, parent_size))
      yield (edge, expected)
    for arg, expected in EDGE_TO_SCALAR_TEST_DATA:
      edge = arolla.abc.invoke_op(
          'edge.to_scalar', (array_fn(arg, arolla.FLOAT64),)
      )
      expected = arolla.int64(expected)
      yield (edge, expected)

  # scalar-to-scalar edge test case
  edge = arolla.abc.invoke_op('edge.to_scalar', (arolla.float64(3.1415),))
  size = arolla.int64(1)
  yield (edge, size)


TEST_CASES = tuple(gen_cases())
QTYPE_SIGNATURES = frozenset(
    tuple(x.qtype for x in test_case) for test_case in TEST_CASES
)


class EdgeSizesTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.edge.sizes, QTYPE_SIGNATURES)

  @parameterized.parameters(*TEST_CASES)
  def test_eval(self, edge, expected):
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.edge.sizes(edge)), expected
    )


if __name__ == '__main__':
  absltest.main()
