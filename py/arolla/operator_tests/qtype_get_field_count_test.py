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

"""Tests for M.qtype.get_field_count."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M

QTYPE_SIGNATURES = ((arolla.QTYPE, arolla.INT64),)


class QTypeGetFieldCount(parameterized.TestCase):

  def testQTypeSignatures(self):
    self.assertCountEqual(
        QTYPE_SIGNATURES,
        pointwise_test_utils.detect_qtype_signatures(M.qtype.get_field_count),
    )

  @parameterized.parameters(
      # scalars
      (arolla.UNIT, 0),
      (arolla.BOOLEAN, 0),
      (arolla.BYTES, 0),
      (arolla.TEXT, 0),
      (arolla.INT32, 0),
      (arolla.INT64, 0),
      (arolla.FLOAT32, 0),
      (arolla.FLOAT64, 0),
      # optionals
      (arolla.OPTIONAL_UNIT, 1),
      (arolla.OPTIONAL_BOOLEAN, 2),
      (arolla.OPTIONAL_BYTES, 2),
      (arolla.OPTIONAL_TEXT, 2),
      (arolla.OPTIONAL_INT32, 2),
      (arolla.OPTIONAL_INT64, 2),
      (arolla.OPTIONAL_FLOAT32, 2),
      (arolla.OPTIONAL_FLOAT64, 2),
      # tuples
      (arolla.make_tuple_qtype(), 0),
      (arolla.make_tuple_qtype(arolla.INT32), 1),
      (
          arolla.make_tuple_qtype(
              arolla.INT32,
              arolla.INT32,
              arolla.INT32,
              arolla.INT32,
              arolla.INT32,
          ),
          5,
      ),
      # dense_array
      (arolla.DENSE_ARRAY_FLOAT32, 0),
  )
  def test(self, input_qvalue, expected_value):
    output_qvalue = arolla.eval(M.qtype.get_field_count(input_qvalue))
    arolla.testing.assert_qvalue_allequal(
        output_qvalue, arolla.int64(expected_value)
    )


if __name__ == '__main__':
  absltest.main()
