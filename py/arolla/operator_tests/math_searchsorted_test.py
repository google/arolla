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

"""Tests for math.searchsorted."""

from typing import Any

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M

NAN = float('nan')
INF = float('inf')

ARRAY_FACTORIES = [arolla.dense_array, arolla.array]

# Format: Tuple[haystack, needle, right, results, haystack_qtypes]
TEST_DATA = [
    (
        [0.0, 1.0, 2.0, 3.0],
        [-INF, 2.0, 0.0, 1.5, INF, NAN, None],
        False,
        [0, 2, 0, 2, 4, 0, None],
        arolla.types.FLOATING_POINT_QTYPES,
    ),
    (
        [0.0, 1.0, 2.0, 3.0],
        [-INF, 2.0, 0.0, 1.5, INF, NAN, None],
        True,
        [0, 3, 1, 2, 4, 4, None],
        arolla.types.FLOATING_POINT_QTYPES,
    ),
    (
        [0, 2, 4, 6],
        [-10, 4, 0, 3, 10, None],
        False,
        [0, 2, 0, 2, 4, None],
        arolla.types.NUMERIC_QTYPES,
    ),
    (
        [0, 2, 4, 6],
        [-10, 4, 0, 3, 10, None],
        True,
        [0, 3, 1, 2, 4, None],
        arolla.types.NUMERIC_QTYPES,
    ),
    (
        [2**60, 2**60 + 1, 2**60 + 2, 2**60 + 3, 2**60 + 4],
        [2**60, 2**60 + 1, 2**60 + 2, 2**60 + 3, 2**60 + 4],
        False,
        [0, 1, 2, 3, 4],
        (arolla.types.INT64,),
    ),
]


def gen_cases(
    test_data: list[
        tuple[list[Any], list[Any], bool, list[int | None], tuple[Any, ...]]
    ]
):
  """Generates (haystack, needle, right expected_result) test cases.

  Args:
    test_data: Tuple[haystack, needle, rigth, results, haystack_qtypes].

  Yields:
    Tuple[haystack_qvalue, needle_qvalue, right_qvalue, expected_result_qvalue]
  """
  for haystack, needle, right, expected_result, value_qtypes in test_data:
    for haystack_value_qtype in value_qtypes:
      for needle_value_qtype in value_qtypes:
        try:
          _ = arolla.types.common_qtype(
              haystack_value_qtype, needle_value_qtype
          )
        except arolla.types.QTypeError:
          continue
        for haystack_array_factory in ARRAY_FACTORIES:
          haystack_qvalue = haystack_array_factory(
              haystack, value_qtype=haystack_value_qtype
          )
          for needle_qvalue, expected_qvalue in pointwise_test_utils.gen_cases(
              tuple(zip(needle, expected_result)),
              *pointwise_test_utils.lift_qtypes(
                  (needle_value_qtype, arolla.INT64)
              )
          ):
            if not right:
              yield (haystack_qvalue, needle_qvalue, None, expected_qvalue)
              yield (
                  haystack_qvalue,
                  needle_qvalue,
                  arolla.optional_boolean(None),
                  expected_qvalue,
              )
            yield (
                haystack_qvalue,
                needle_qvalue,
                arolla.optional_boolean(right),
                expected_qvalue,
            )
            yield (
                haystack_qvalue,
                needle_qvalue,
                arolla.boolean(right),
                expected_qvalue,
            )


def get_signature(haystack_qvalue, needle_qvalue, right, expected_qvalue):
  if right is None:
    return (haystack_qvalue.qtype, needle_qvalue.qtype, expected_qvalue.qtype)
  return (
      haystack_qvalue.qtype,
      needle_qvalue.qtype,
      right.qtype,
      expected_qvalue.qtype,
  )


TEST_CASES = tuple(gen_cases(TEST_DATA))
TEST_SIGNATURES = frozenset(
    get_signature(*test_case) for test_case in TEST_CASES
)


class MathSearchsortedTest(parameterized.TestCase):

  def testQTypeSignatures(self):
    arolla.testing.assert_qtype_signatures(M.math.searchsorted, TEST_SIGNATURES)

  @parameterized.parameters(list(TEST_CASES))
  def test_searchsorted(self, haystack, needle, right, expected):
    if right is None:
      arolla.testing.assert_qvalue_allequal(
          arolla.eval(M.math.searchsorted(haystack, needle)), expected
      )
    else:
      arolla.testing.assert_qvalue_allequal(
          arolla.eval(M.math.searchsorted(haystack, needle, right)), expected
      )


if __name__ == '__main__':
  absltest.main()
