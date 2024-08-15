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

"""Tests for M.strings.encode."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils


def gen_test_data():
  """Yields test data for strings.encode operator.

  Yields: (arg_1, result)
  """
  for x in (
      'abcdefghijklmnopqrstuvwxyz',
      'αβγδεζηθικλμνξοπρσ/ςτυφχψω',
      'абвгдеёжзийклмнопрстуфхцчшщъыьэюя',
      'абвгґдеєжзиіїйклмнопрстуфхцчшщьюя',
      'אבגדהוזחטיךכלםמןנסעףפץצקרשתװױײ',
  ):
    yield (x, x.encode('utf8'))


# Test data: tuple(
#     (arg, result),
#     ...
# )
TEST_DATA = tuple(gen_test_data())

# QType signatures: tuple(
#     (arg_qtype, result_qtype),
#     ...
# )
QTYPE_SIGNATURES = pointwise_test_utils.lift_qtypes((arolla.TEXT, arolla.BYTES))

M = arolla.M


class StringsEncodeTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    self.assertEqual(
        frozenset(QTYPE_SIGNATURES),
        frozenset(
            pointwise_test_utils.detect_qtype_signatures(M.strings.encode)
        ),
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def testValue(self, x, expected_value):
    actual_value = self.eval(M.strings.encode(x))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == '__main__':
  absltest.main()
