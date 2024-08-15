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

"""Tests for M.qtype._get_key_to_row_dict_key_qtype operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import dict_test_utils
from arolla.operator_tests import pointwise_test_utils

M = arolla.M


def gen_test_data():
  """Yields test data for qtype._get_key_to_row_dict_key_qtype operator.

  Yields: (arg_1, result)
  """
  for qtype in dict_test_utils.DICT_KEY_QTYPES:
    yield (arolla.types.make_key_to_row_dict_qtype(qtype), qtype)

  for qtype in [
      *arolla.types.SCALAR_QTYPES,
      *arolla.types.DENSE_ARRAY_QTYPES,
      *arolla.types.ARRAY_QTYPES,
  ]:
    yield (qtype, arolla.NOTHING)


# Test data: tuple(
#     (arg, result),
#     ...
# )
TEST_DATA = tuple(gen_test_data())

# QType signatures: tuple(
#     (arg_qtype, result_qtype),
#     ...
# )
QTYPE_SIGNATURES = ((arolla.QTYPE, arolla.QTYPE),)


class QTypeGetKeyToRowDictKeyQTypeTest(parameterized.TestCase):

  def testQTypeSignatures(self):
    self.assertEqual(
        frozenset(QTYPE_SIGNATURES),
        frozenset(
            pointwise_test_utils.detect_qtype_signatures(
                M.qtype._get_key_to_row_dict_key_qtype
            )
        ),
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def testValue(self, arg, expected_value):
    actual_value = arolla.eval(M.qtype._get_key_to_row_dict_key_qtype(arg))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == "__main__":
  absltest.main()
