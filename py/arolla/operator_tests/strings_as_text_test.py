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

"""Tests for M.strings.as_text."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

M = arolla.M

# Test data: tuple((arg, expected_result), ...)
TEST_DATA = (
    (None, None),
    (arolla.unit(), "present"),
    (arolla.boolean(True), "true"),
    (b"foo", "b'foo'"),
    (b"foo\x00bar'", "b'foo\\x00\\x62\\x61r\\''"),
    ("foo", "foo"),
    ("foo\x00bar", "foo\x00bar"),
    (-1, "-1"),
    (-0.0, "-0"),
    (0, "0"),
    (1, "1"),
    (0.2, "0.2"),
    (2.3, "2.3"),
    (14.137167, "14.137167"),
    (1e30, "1e30"),
    (1e-30, "1e-30"),
    (float("inf"), "inf"),
    (-float("inf"), "-inf"),
    (float("nan"), "nan"),
)

QTYPE_SIGNATURES = pointwise_test_utils.lift_qtypes(
    *((t, arolla.types.TEXT) for t in arolla.types.SCALAR_QTYPES)
)


class StringsAsTextTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    self.assertEqual(
        frozenset(QTYPE_SIGNATURES),
        frozenset(
            pointwise_test_utils.detect_qtype_signatures(M.strings.as_text)
        ),
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def testValue(self, arg, expected_value):
    actual_value = self.eval(M.strings.as_text(arg))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == "__main__":
  absltest.main()
