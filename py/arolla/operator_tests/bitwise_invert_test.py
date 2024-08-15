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

"""Tests for M.bitwise.invert operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M


def gen_test_data():
  """Returns scalar test data for bitwise.invert operator.

  Returns: tuple(
      (arg, result),
      ...
  )
  """
  values = [0] + [2**i for i in range(63)] + [-(2**i) for i in range(64)]
  return ((None, None), *((arg, ~arg) for arg in values))


def gen_qtype_signatures():
  """Returns supported qtype signatures for bitwise.invert.

  Returns: tuple(
      (arg_qtype, result_qtype),
      ...
  )
  """
  return pointwise_test_utils.lift_qtypes(
      *zip(arolla.types.INTEGRAL_QTYPES, arolla.types.INTEGRAL_QTYPES)
  )


TEST_DATA = gen_test_data()
QTYPE_SIGNATURES = gen_qtype_signatures()


class BitwiseInvertTest(parameterized.TestCase):

  def testQTypeSignatures(self):
    self.assertEqual(
        frozenset(QTYPE_SIGNATURES),
        frozenset(
            pointwise_test_utils.detect_qtype_signatures(M.bitwise.invert)
        ),
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def testValue(self, arg, expected_value):
    actual_value = arolla.eval(M.bitwise.invert(arg))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == "__main__":
  absltest.main()
