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

"""Tests for M.array.as_array."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M


def gen_qtype_signatures():
  for arg in arolla.types.SCALAR_QTYPES:
    for mutator in pointwise_test_utils.ALL_QTYPE_MUTATORS:
      yield mutator(arg), arolla.types.make_array_qtype(arg)


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())

values = (None, False, True, b'', b'foo', '', 'bar', 0, 1, 1.5, float('nan'))
TEST_DATA = tuple(zip(values, values))


class ArrayAsArray(parameterized.TestCase):

  def testQTypeSignatures(self):
    self.assertEqual(
        frozenset(QTYPE_SIGNATURES),
        frozenset(
            pointwise_test_utils.detect_qtype_signatures(M.array.as_array)
        ),
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def test(self, arg, expected):
    converted = arolla.eval(M.array.as_array(arg))
    arolla.testing.assert_qvalue_allequal(converted, expected)


if __name__ == '__main__':
  absltest.main()
