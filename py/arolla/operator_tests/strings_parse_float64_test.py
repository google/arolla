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

"""Tests for M.strings.parse_float64."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils
from arolla.operator_tests import strings_parse_float_data

M = arolla.M

_TEST_DATA = strings_parse_float_data.TEST_DATA + (
    ("12345678901234567890", 12345678901234567000.0),
    ("1e-323", 1e-323),
    ("1e-324", 0.0),
    ("1e308", 1e308),
    ("1e309", float("inf")),
)

_TEST_DATA = _TEST_DATA + tuple(
    (None if x[0] is None else x[0].encode("utf8"),) + x[1:] for x in _TEST_DATA
)

_ERROR_TEST_DATA = tuple(
    (s, s) for s in strings_parse_float_data.ERROR_TEST_DATA
) + tuple(
    (s, s.encode("utf8")) for s in strings_parse_float_data.ERROR_TEST_DATA
)


class StringsParseFloat64Test(parameterized.TestCase):

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(
          _TEST_DATA,
          *pointwise_test_utils.lift_qtypes(
              (arolla.BYTES, arolla.FLOAT64), (arolla.TEXT, arolla.FLOAT64)
          ),
      )
  )
  def testValue(self, arg, expected_value):
    actual_value = arolla.eval(M.strings.parse_float64(arg))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(
          _ERROR_TEST_DATA,
          *pointwise_test_utils.lift_qtypes(
              (None, arolla.BYTES), (None, arolla.TEXT)
          ),
      )
  )
  def testError(self, s, arg):
    with self.assertRaisesRegex(ValueError, re.escape("FLOAT64: " + s)):
      _ = arolla.eval(M.strings.parse_float64(arg))


if __name__ == "__main__":
  absltest.main()
