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

"""Tests for M.qtype.get_field_qtype."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M

QTYPE_SIGNATURES = (
    (arolla.QTYPE, idx, arolla.QTYPE) for idx in arolla.types.INTEGRAL_QTYPES
)


class QTypeGetFieldQTypesTest(parameterized.TestCase):

  def testQTypeSignatures(self):
    self.assertCountEqual(
        QTYPE_SIGNATURES,
        pointwise_test_utils.detect_qtype_signatures(M.qtype.get_field_qtype),
    )

  @parameterized.parameters(
      (arolla.INT32, -1, arolla.NOTHING),
      (arolla.INT32, 0, arolla.NOTHING),
      (arolla.INT32, 1, arolla.NOTHING),
      (arolla.OPTIONAL_UNIT, 0, arolla.BOOLEAN),
      (arolla.OPTIONAL_UNIT, 1, arolla.NOTHING),
      (arolla.OPTIONAL_INT32, -1, arolla.NOTHING),
      (arolla.OPTIONAL_INT32, 0, arolla.BOOLEAN),
      (arolla.OPTIONAL_INT32, 1, arolla.INT32),
      (arolla.OPTIONAL_INT32, 2, arolla.NOTHING),
      (arolla.types.make_tuple_qtype(), 0, arolla.NOTHING),
      (
          arolla.types.make_tuple_qtype(arolla.INT32, arolla.INT64),
          0,
          arolla.INT32,
      ),
      (
          arolla.types.make_tuple_qtype(arolla.INT32, arolla.INT64),
          1,
          arolla.INT64,
      ),
      (
          arolla.types.make_tuple_qtype(
              arolla.types.make_tuple_qtype(), arolla.INT64
          ),
          0,
          arolla.types.make_tuple_qtype(),
      ),
  )
  def testValue(self, arg_qvalue, idx, expected_output_qvalue):
    actual_output_qvalue = arolla.eval(M.qtype.get_field_qtype(arg_qvalue, idx))
    arolla.testing.assert_qvalue_allequal(
        actual_output_qvalue, expected_output_qvalue
    )


if __name__ == '__main__':
  absltest.main()
