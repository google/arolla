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

"""Tests for M.bitwise.bitwise_or operator."""

import contextlib
import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M


def gen_test_data():
  """Yields test data for bitwise.bitwise_or operator.

  Yields: (arg_1, arg_2, result)
  """
  values = (
      [None, 0]
      + [2**i for i in range(0, 63, 4)]
      + [-(2**i) for i in range(0, 64, 4)]
  )
  for arg_1, arg_2 in itertools.product(values, repeat=2):  # pytype: disable=bad-unpacking
    yield (
        arg_1,
        arg_2,
        None if arg_1 is None or arg_2 is None else arg_1 | arg_2,
    )


def gen_qtype_signatures():
  """Yields qtype signatures for bitwise.bitwise_or.

  Yields: (arg_1_qtype, arg_2_qtype, result_qtype)
  """
  qtypes = pointwise_test_utils.lift_qtypes(*arolla.types.INTEGRAL_QTYPES)
  for arg_1, arg_2 in itertools.product(qtypes, repeat=2):
    with contextlib.suppress(arolla.types.QTypeError):
      yield (arg_1, arg_2, arolla.types.common_qtype(arg_1, arg_2))


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class BitwiseBitwiseOrTest(parameterized.TestCase):

  def testQTypeSignatures(self):
    self.assertEqual(
        frozenset(QTYPE_SIGNATURES),
        frozenset(
            pointwise_test_utils.detect_qtype_signatures(M.bitwise.bitwise_or)
        ),
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def testValue(self, lhs, rhs, expected_value):
    actual_value = arolla.eval(M.bitwise.bitwise_or(lhs, rhs))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == "__main__":
  absltest.main()
