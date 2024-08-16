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

"""Tests for M.qtype.get_field_qtypes."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M

# QType signatures: tuple(
#     (arg_qtype, result_qtype),
#     ...
# )
QTYPE_SIGNATURES = (
    (arolla.QTYPE, arolla.types.make_sequence_qtype(arolla.QTYPE)),
)


class QTypeGetFieldQTypesTest(parameterized.TestCase):

  def testQTypeSignatures(self):
    self.assertCountEqual(
        QTYPE_SIGNATURES,
        pointwise_test_utils.detect_qtype_signatures(M.qtype.get_field_qtypes),
    )

  @parameterized.parameters(
      (arolla.INT32, arolla.types.Sequence(value_qtype=arolla.QTYPE)),
      (arolla.OPTIONAL_UNIT, arolla.types.Sequence(arolla.BOOLEAN)),
      (
          arolla.OPTIONAL_INT32,
          arolla.types.Sequence(arolla.BOOLEAN, arolla.INT32),
      ),
      (
          arolla.types.make_tuple_qtype(),
          arolla.types.Sequence(value_qtype=arolla.QTYPE),
      ),
      (
          arolla.types.make_tuple_qtype(arolla.INT32, arolla.INT64),
          arolla.types.Sequence(arolla.INT32, arolla.INT64),
      ),
  )
  def testValue(self, arg_qvalue, expected_output_qvalue):
    actual_output_qvalue = arolla.eval(M.qtype.get_field_qtypes(arg_qvalue))
    arolla.testing.assert_qvalue_allequal(
        actual_output_qvalue, expected_output_qvalue
    )


if __name__ == '__main__':
  absltest.main()