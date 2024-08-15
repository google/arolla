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

"""Tests for M.array.slice."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

M = arolla.M


def gen_test_cases():
  # Provide full type coverage using arrays with missing elements.
  for array_fn in (arolla.array, arolla.dense_array):
    for value_qtype in arolla.types.SCALAR_QTYPES:
      array0 = array_fn([], value_qtype=value_qtype)
      array1 = array_fn([None], value_qtype=value_qtype)
      array2 = array_fn([None, None], value_qtype=value_qtype)
      yield from (
          (array0, arolla.int32(0), arolla.int32(0), array0),
          (array1, arolla.int32(0), arolla.int64(1), array1),
          (array2, arolla.int64(0), arolla.int32(2), array2),
          (array2, arolla.int64(1), arolla.int64(-1), array1),
      )
  # Arbitrary tests.
  test_data = (
      [None, arolla.unit(), None],
      [True, None, False, None],
      [None, b'bar', None],
      [None, 'xoo', 'yoo', None],
      [-1, -3, None, 1000],
      [1.7, 5.0, 2.6],
  )
  for array_fn in (arolla.array, arolla.dense_array):
    for values in test_data:
      array = array_fn(values)
      n = len(values)
      for offset in range(n + 1):
        yield (
            array,
            arolla.int64(offset),
            arolla.int64(-1),
            array_fn(values[offset:], value_qtype=array.value_qtype),
        )
        print(
            array,
            offset,
            -1,
            array_fn(values[offset:], value_qtype=array.value_qtype),
        )
        for size in range(n - offset + 1):
          yield (
              array,
              arolla.int64(offset),
              arolla.int64(size),
              array_fn(
                  values[offset : offset + size], value_qtype=array.value_qtype
              ),
          )


TEST_CASES = tuple(gen_test_cases())

QTYPE_SIGNATURES = frozenset(
    tuple(x.qtype for x in case) for case in TEST_CASES
)


class ArraySliceTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        M.array.slice, QTYPE_SIGNATURES
    )

  @parameterized.parameters(*TEST_CASES)
  def testValues(self, array, offset, size, expected_result):
    actual_result = arolla.eval(M.array.slice(array, offset, size))
    arolla.testing.assert_qvalue_allequal(actual_result, expected_result)

  def testError(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected `offset` in [0, 5], but got 10'),
    ):
      _ = arolla.eval(M.array.slice(arolla.array(range(5)), 10, 0))
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected `size` in [0, 3], but got 5'),
    ):
      _ = arolla.eval(M.array.slice(arolla.array(range(5)), 2, 5))
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected `size` in [0, 3], but got -2'),
    ):
      _ = arolla.eval(M.array.slice(arolla.array(range(5)), 2, -2))


if __name__ == '__main__':
  absltest.main()
