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

"""Tests for edge.compose."""
import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import utils

M = arolla.M


def gen_test_cases():
  for _, array_fn in utils.ARRAY_FACTORIES:
    # Two edges.
    yield (
        M.edge.from_mapping(array_fn([2, 1, 0]), 3),  # pyrefly: ignore[missing-attribute]
        M.edge.from_mapping(array_fn([0, 1, 2, 0, 1, 2]), 3),  # pyrefly: ignore[missing-attribute]
        M.edge.from_mapping(array_fn([2, 1, 0, 2, 1, 0]), 3),  # pyrefly: ignore[missing-attribute]
    )
    yield (
        M.edge.from_mapping(array_fn([4, 3, 4]), 7),  # pyrefly: ignore[missing-attribute]
        M.edge.from_mapping(array_fn([0, 1, 2, 0, 1, 2]), 3),  # pyrefly: ignore[missing-attribute]
        M.edge.from_mapping(array_fn([4, 3, 4, 4, 3, 4]), 7),  # pyrefly: ignore[missing-attribute]
    )
    yield (
        M.edge.from_mapping(array_fn([None, 3, 4]), 7),  # pyrefly: ignore[missing-attribute]
        M.edge.from_mapping(array_fn([0, 1, 2, 0, 1, 2]), 3),  # pyrefly: ignore[missing-attribute]
        M.edge.from_mapping(array_fn([None, 3, 4, None, 3, 4]), 7),  # pyrefly: ignore[missing-attribute]
    )
    yield (
        M.edge.from_mapping(array_fn([4, 3, 4]), 7),  # pyrefly: ignore[missing-attribute]
        M.edge.from_mapping(array_fn([0, 1, 2, None, 1, 2]), 3),  # pyrefly: ignore[missing-attribute]
        M.edge.from_mapping(array_fn([4, 3, 4, None, 3, 4]), 7),  # pyrefly: ignore[missing-attribute]
    )
    yield (
        M.edge.from_sizes(array_fn([0, 0, 2, 0, 1, 0, 0])),  # pyrefly: ignore[missing-attribute]
        M.edge.from_mapping(array_fn([0, 1, 2, 0, 1, 2]), 3),  # pyrefly: ignore[missing-attribute]
        M.edge.from_mapping(array_fn([2, 2, 4, 2, 2, 4]), 7),  # pyrefly: ignore[missing-attribute]
    )
    yield (
        M.edge.from_mapping(array_fn([5, 4, 3]), 7),  # pyrefly: ignore[missing-attribute]
        M.edge.from_sizes(array_fn([2, 2, 2])),  # pyrefly: ignore[missing-attribute]
        M.edge.from_mapping(array_fn([5, 5, 4, 4, 3, 3]), 7),  # pyrefly: ignore[missing-attribute]
    )
    yield (
        M.edge.from_sizes(array_fn([0, 0, 2, 0, 1, 0, 0])),  # pyrefly: ignore[missing-attribute]
        M.edge.from_sizes(array_fn([2, 2, 2])),  # pyrefly: ignore[missing-attribute]
        M.edge.from_mapping(array_fn([2, 2, 2, 2, 4, 4]), 7),  # pyrefly: ignore[missing-attribute]
    )
    yield (
        M.edge.to_single(array_fn(range(3))),  # pyrefly: ignore[missing-attribute]
        M.edge.from_mapping(array_fn([0, 1, None, 0, 1, 2]), 3),  # pyrefly: ignore[missing-attribute]
        M.edge.from_mapping(array_fn([0, 0, None, 0, 0, 0]), 1),  # pyrefly: ignore[missing-attribute]
    )
    yield (
        M.edge.from_mapping(array_fn([], arolla.INT64), 7),  # pyrefly: ignore[missing-attribute]
        M.edge.from_mapping(array_fn([None, None, None], arolla.INT64), 0),  # pyrefly: ignore[missing-attribute]
        M.edge.from_mapping(array_fn([None, None, None], arolla.INT64), 7),  # pyrefly: ignore[missing-attribute]
    )
    # One edge.
    yield (
        M.edge.from_mapping(array_fn([2, 1, 0]), 3),  # pyrefly: ignore[missing-attribute]
        M.edge.from_mapping(array_fn([2, 1, 0]), 3),  # pyrefly: ignore[missing-attribute]
    )
    # >2 edges.
    yield (
        M.edge.from_sizes(array_fn([1, 2])),  # pyrefly: ignore[missing-attribute]
        M.edge.from_sizes(array_fn([1, 1, 2])),  # pyrefly: ignore[missing-attribute]
        M.edge.from_sizes(array_fn([3, 1, 7, 1])),  # pyrefly: ignore[missing-attribute]
        M.edge.from_sizes(array_fn([3, 9])),  # pyrefly: ignore[missing-attribute]
    )


TEST_CASES = tuple(gen_test_cases())
QTYPE_SIGNATURES = frozenset(
    tuple(arolla.eval(e).qtype for e in test_case) for test_case in TEST_CASES
)


class EdgeComposeTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  @parameterized.parameters(*TEST_CASES)
  def test_eval(self, *edges_and_expected):
    edges, expected = edges_and_expected[:-1], edges_and_expected[-1]
    expected = arolla.eval(expected)
    actual = self.eval(M.edge.compose(*edges))  # pyrefly: ignore[missing-attribute]
    arolla.testing.assert_qvalue_allequal(actual.mapping(), expected.mapping())
    self.assertEqual(actual.parent_size, expected.parent_size)

  def test_incompatible_edge_shape_qtype_error(self):
    self.require_self_eval_is_called = False
    a = arolla.eval(M.edge.from_mapping(arolla.dense_array([1, 0, 2]), 3))  # pyrefly: ignore[missing-attribute]
    b = arolla.eval(M.edge.from_mapping(arolla.array([0, 1, 2]), 3))  # pyrefly: ignore[missing-attribute]
    c = arolla.eval(M.edge.from_mapping(arolla.dense_array([2, 1, 0]), 3))  # pyrefly: ignore[missing-attribute]
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'arguments do not have a common type - edge: DENSE_ARRAY_EDGE,'
            ' *edges: (ARRAY_EDGE, DENSE_ARRAY_EDGE)'
        ),
    ):
      _ = M.edge.compose(a, b, c)  # pyrefly: ignore[missing-attribute]

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_incompatible_size_error(self, array_factory):
    a = arolla.eval(M.edge.from_mapping(array_factory([1, 0, 2, 0, 1]), 3))  # pyrefly: ignore[missing-attribute]
    b = arolla.eval(M.edge.from_mapping(array_factory([0, 1, 2, 0, 1, 2]), 3))  # pyrefly: ignore[missing-attribute]
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'incompatible edges: edges[0].child_size (5) !='
            ' edges[1].parent_size (3)'
        ),
    ):
      _ = self.eval(M.edge.compose(a, b))  # pyrefly: ignore[missing-attribute]


if __name__ == '__main__':
  absltest.main()
