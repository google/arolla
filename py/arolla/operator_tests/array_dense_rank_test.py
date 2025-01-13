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

"""Tests for array.dense_rank operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import utils

M = arolla.M
NAN = float('nan')


def gen_test_cases(array_factory):
  xs0 = [
      array_factory([7, 8, None, 7, 7, 8], value_qtype=qtype)
      for qtype in arolla.types.NUMERIC_QTYPES
  ] + [
      array_factory(['a', 'b', None, 'a', 'a', 'b'], arolla.TEXT),
      array_factory([b'a', b'b', None, b'a', b'a', b'b'], arolla.BYTES),
      array_factory([False, True, None, False, False, True], arolla.BOOLEAN),
  ]
  xs1 = [
      array_factory(
          [None, arolla.unit(), None, None, arolla.unit(), arolla.unit()]
      ),
      array_factory([None, False, None, None, False, False], arolla.BOOLEAN),
  ]

  inputs = [xs0, xs1]
  expectations = [
      array_factory([0, 1, None, 0, 0, 1], arolla.INT64),
      array_factory([None, 0, None, None, 0, 0], arolla.INT64),
  ]

  for xs, expected in zip(inputs, expectations):
    for x in xs:
      yield (x, expected)
      edges = [
          arolla.eval(M.edge.to_single(x)),
          arolla.eval(M.edge.to_scalar(x)),
          arolla.unspecified(),
      ]
      for over in edges:
        yield (x, over, expected)
        descending = arolla.boolean(False)
        yield (x, over, descending, expected)


TEST_CASES = [
    *gen_test_cases(arolla.array),
    *gen_test_cases(arolla.dense_array),
]


class ArrayDenseRankTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    expected_qtype_signatures = frozenset(
        tuple(arg.qtype for arg in test_case) for test_case in TEST_CASES
    )
    arolla.testing.assert_qtype_signatures(
        M.array.dense_rank, expected_qtype_signatures
    )

  @parameterized.parameters(TEST_CASES)
  def test_generated_cases(self, *test_case):
    *args, expected = test_case
    actual = self.eval(M.array.dense_rank(*args))
    arolla.testing.assert_qvalue_allequal(actual, expected)

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_floating_with_nan(self, array_factory):
    values = [3, 8, 8, NAN, None, 2, None, NAN, 1, 8]
    edge = arolla.eval(M.edge.from_sizes(array_factory([3, 3, 0, 4])))
    expected_ascending = array_factory(
        [0, 1, 1, 1, None, 0, None, 2, 0, 1], arolla.INT64
    )
    expected_descending = array_factory(
        [1, 0, 0, 1, None, 0, None, 2, 1, 0], arolla.INT64
    )
    for dtype in arolla.types.FLOATING_POINT_QTYPES:
      with self.subTest(str(dtype)):
        a = array_factory(values, dtype)
        arolla.testing.assert_qvalue_allequal(
            self.eval(M.array.dense_rank(a, over=edge)),
            expected_ascending,
        )
        arolla.testing.assert_qvalue_allequal(
            self.eval(M.array.dense_rank(a, over=edge, descending=True)),
            expected_descending,
        )


if __name__ == '__main__':
  absltest.main()
