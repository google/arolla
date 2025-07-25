# Copyright 2025 Google LLC
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

"""Tests for M.strings.length operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

M = arolla.M

# Test data: tuple((arg, expected_result), ...)
TEST_DATA = (
    (None, None),
    ('', 0),
    ('snake', '5'),
    ('змея', 4),
    ('ü', 1),
    ('u\u0308', 2),  # Also ü, but written using combining character.
    (b'', 0),
    (b'snake', '5'),
    ('змея'.encode(), 8),
    ('ü'.encode(), 2),
    ('u\u0308'.encode(), 3),  # Also ü, but written using combining character.
)

QTYPE_SIGNATURES = pointwise_test_utils.lift_qtypes(
    (arolla.types.TEXT, arolla.types.INT64)
) + pointwise_test_utils.lift_qtypes((arolla.types.BYTES, arolla.types.INT64))


class StringsLengthTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.strings.length, QTYPE_SIGNATURES)

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def test_eval(self, arg, expected_value):
    actual_value = self.eval(M.strings.length(arg))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == '__main__':
  absltest.main()
