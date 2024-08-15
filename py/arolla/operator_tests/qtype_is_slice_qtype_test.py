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

"""Tests for M.qtype.is_slice_qtype operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

L = arolla.L
M = arolla.M

# Test data: tuple(
#     (arg, result),
#     ...
# )
TEST_DATA = (
    (arolla.NOTHING, False),
    (arolla.INT32, False),
    (arolla.OPTIONAL_INT32, False),
    (arolla.ARRAY_INT32, False),
    (arolla.make_tuple_qtype(), False),
    (arolla.make_tuple_qtype(arolla.INT32, arolla.INT32, arolla.INT32), False),
    (
        arolla.eval(
            M.qtype.make_slice_qtype(
                arolla.INT32, arolla.ARRAY_INT32, arolla.UNSPECIFIED
            )
        ),
        True,
    ),
    (arolla.eval(M.qtype.make_slice_qtype()), True),
)

# QType signatures: tuple(
#     (arg_qtype, result_qtype),
#     ...
# )
QTYPE_SIGNATURES = ((arolla.QTYPE, arolla.OPTIONAL_UNIT),)


class QTypeIsSliceQTypeTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    self.assertCountEqual(
        pointwise_test_utils.detect_qtype_signatures(M.qtype.is_slice_qtype),
        QTYPE_SIGNATURES,
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def test_eval(self, arg, expected_value):
    actual_value = arolla.eval(M.qtype.is_slice_qtype(L.arg), arg=arg)
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)

  def test_error(self):
    with self.assertRaisesRegex(ValueError, "expected QTYPE, got x: INT32"):
      _ = M.qtype.is_slice_qtype(arolla.int32(1))


if __name__ == "__main__":
  absltest.main()
