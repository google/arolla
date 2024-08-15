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

"""Tests for edge.group_by2."""
import collections

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import utils

M = arolla.M

TestCase = collections.namedtuple(
    'TestCase', ['grouping', 'a_to_c', 'a_to_b', 'b_to_c']
)


def gen_test_cases():
  for _, array_fn in utils.ARRAY_FACTORIES:
    for qtype in arolla.types.NUMERIC_QTYPES:
      yield TestCase(
          grouping=array_fn([5, -7, 0, 5, -7, 0], qtype),
          a_to_c=arolla.unspecified(),
          a_to_b=M.edge.from_mapping(array_fn([0, 1, 2, 0, 1, 2]), 3),
          b_to_c=M.edge.from_mapping(array_fn([0, 0, 0]), 1),
      )
      yield TestCase(
          grouping=array_fn([5, 5, 0, 5, 5, 0], qtype),
          a_to_c=M.edge.from_sizes(array_fn([3, 3])),
          a_to_b=M.edge.from_mapping(array_fn([0, 0, 1, 2, 2, 3]), 4),
          b_to_c=M.edge.from_mapping(array_fn([0, 0, 1, 1]), 2),
      )
      yield TestCase(
          grouping=array_fn([5, 5, 5, 5, 5, 5], qtype),
          a_to_c=M.edge.from_mapping(array_fn([2, 1, 0, 2, 1, 0]), 3),
          a_to_b=M.edge.from_mapping(array_fn([0, 1, 2, 0, 1, 2]), 3),
          b_to_c=M.edge.from_mapping(array_fn([2, 1, 0]), 3),
      )
      yield TestCase(
          grouping=array_fn([5, -7, 0, 5, -7, 0], qtype),
          a_to_c=M.edge.from_mapping(array_fn([4, 3, 4, 4, 3, 4]), 7),
          a_to_b=M.edge.from_mapping(array_fn([0, 1, 2, 0, 1, 2]), 3),
          b_to_c=M.edge.from_mapping(array_fn([4, 3, 4]), 7),
      )
      yield TestCase(
          grouping=array_fn([None, -7, 0, None, -7, 0], qtype),
          a_to_c=M.edge.from_mapping(array_fn([3, 3, 4, 1, 3, 4]), 7),
          a_to_b=M.edge.from_mapping(array_fn([None, 0, 1, None, 0, 1]), 2),
          b_to_c=M.edge.from_mapping(array_fn([3, 4]), 7),
      )
      yield TestCase(
          grouping=array_fn([5, -7, 0, 5, -7, 0], qtype),
          a_to_c=M.edge.from_mapping(array_fn([4, 3, 4, None, 3, 4]), 7),
          a_to_b=M.edge.from_mapping(array_fn([0, 1, 2, None, 1, 2]), 3),
          b_to_c=M.edge.from_mapping(array_fn([4, 3, 4]), 7),
      )
      yield TestCase(
          grouping=array_fn([5, -7, 0, 5, -7, 0], qtype),
          a_to_c=M.edge.from_mapping(array_fn([2, 2, 4, 2, 2, 4]), 7),
          a_to_b=M.edge.from_mapping(array_fn([0, 1, 2, 0, 1, 2]), 3),
          b_to_c=M.edge.from_mapping(array_fn([2, 2, 4]), 7),
      )
      yield TestCase(
          grouping=M.core.make_tuple(
              array_fn([5, 5, 0, 5, 5, 0], qtype),
              array_fn(['a', 'b', 'a', 'a', 'b', 'a']),
          ),
          a_to_c=M.edge.from_mapping(array_fn([4, 3, 4, 4, 3, 4]), 7),
          a_to_b=M.edge.from_mapping(array_fn([0, 1, 2, 0, 1, 2]), 3),
          b_to_c=M.edge.from_mapping(array_fn([4, 3, 4]), 7),
      )

    yield TestCase(
        grouping=array_fn(['x', 'x', 'x2', 'x2', 'X', 'X']),
        a_to_c=M.edge.from_sizes(array_fn([0, 0, 0, 2, 2, 2, 0])),
        a_to_b=M.edge.from_mapping(array_fn([0, 0, 1, 1, 2, 2]), 3),
        b_to_c=M.edge.from_mapping(array_fn([3, 4, 5]), 7),
    )
    yield TestCase(
        grouping=array_fn([b'', b'', b' ', b' ', b'\n', b'\n']),
        a_to_c=M.edge.from_sizes(array_fn([0, 0, 0, 2, 2, 2, 0])),
        a_to_b=M.edge.from_mapping(array_fn([0, 0, 1, 1, 2, 2]), 3),
        b_to_c=M.edge.from_mapping(array_fn([3, 4, 5]), 7),
    )
    yield TestCase(
        grouping=array_fn([None, True, False, None, True, False]),
        a_to_c=M.edge.from_mapping(array_fn([None, 3, 4, None, 3, 4]), 7),
        a_to_b=M.edge.from_mapping(array_fn([None, 0, 1, None, 0, 1]), 2),
        b_to_c=M.edge.from_mapping(array_fn([3, 4]), 7),
    )


TEST_CASES = tuple(gen_test_cases())
QTYPE_SIGNATURES = frozenset(
    tuple(arolla.eval(e).qtype for e in test_case) for test_case in TEST_CASES
)


class EdgeGroupBy2Test(parameterized.TestCase, backend_test_base.SelfEvalMixin):
  @parameterized.parameters(*TEST_CASES)
  def test_eval(self, grouping, a_to_c, expected_a_to_b, expected_b_to_c):
    a_to_b, b_to_c = self.eval(M.edge.group_by2(grouping, over=a_to_c))

    with self.subTest('edge1'):
      expected = arolla.eval(expected_a_to_b)
      arolla.testing.assert_qvalue_allequal(
          a_to_b.mapping(), expected.mapping()
      )
      self.assertEqual(a_to_b.parent_size, expected.parent_size)

    with self.subTest('edge2'):
      expected = arolla.eval(expected_b_to_c)
      arolla.testing.assert_qvalue_allequal(
          b_to_c.mapping(), expected.mapping()
      )
      self.assertEqual(b_to_c.parent_size, expected.parent_size)


if __name__ == '__main__':
  absltest.main()
