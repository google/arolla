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

"""Tests for M.array.concat."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M


def gen_test_cases():
  for array_fn in (arolla.array, arolla.dense_array):
    for value_qtype in arolla.types.SCALAR_QTYPES:
      typed_array_fn = lambda values: array_fn(values, value_qtype=value_qtype)  # pylint: disable=cell-var-from-loop"
      yield (
          typed_array_fn([]),
          typed_array_fn([]),
          typed_array_fn([]),
      )
      yield (
          typed_array_fn([]),
          typed_array_fn([None] * 1),
          typed_array_fn([None] * 1),
      )
      yield (
          typed_array_fn([None] * 1),
          typed_array_fn([None] * 2),
          typed_array_fn([None] * 3),
      )
    test_data = (
        ([arolla.unit(), None], [None, arolla.unit()]),
        ([True, True], [None, False]),
        ([b'foo', b'bar'], [None, b'xoo']),
        (['foo', 'bar'], [None, 'xoo']),
        ([-1, -3], [None, 1000]),
        ([1.7, 2.7], [3.14, 2.6]),
    )
    yield from (
        (array_fn(lhs), array_fn(rhs), array_fn(lhs + rhs))
        for lhs, rhs in test_data
    )


TEST_CASES = tuple(gen_test_cases())

QTYPE_SIGNATURES = frozenset(
    tuple(x.qtype for x in case) for case in TEST_CASES
)


class ArrayConcat(parameterized.TestCase):

  def testQTypeSignatures(self):
    self.assertCountEqual(
        pointwise_test_utils.detect_qtype_signatures(M.array.concat),
        QTYPE_SIGNATURES,
    )

  @parameterized.parameters(*TEST_CASES)
  def testValues(self, lhs, rhs, expected_result):
    actual_result = arolla.eval(M.array.concat(lhs, rhs))
    arolla.testing.assert_qvalue_allequal(actual_result, expected_result)

  def testError(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected arrays of same type, got x: ARRAY_INT32 and y:'
            ' ARRAY_INT64'
        ),
    ):
      _ = M.array.concat(arolla.array_int32([]), arolla.array_int64([]))


if __name__ == '__main__':
  absltest.main()
