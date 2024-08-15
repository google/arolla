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

"""Tests for dict.keys operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import dict_test_utils
from arolla.operator_tests import pointwise_test_utils

M = arolla.M


def gen_cases(test_data):
  """Generates (keys, key_to_row_dict_qtype) test cases."""
  for test in test_data:
    py_dict = test.dictionary
    key_to_row_dict_qtype = arolla.types.make_key_to_row_dict_qtype(
        test.key_qtype
    )
    keys = arolla.dense_array(py_dict.keys(), value_qtype=test.key_qtype)
    yield (keys, key_to_row_dict_qtype)


def _get_key_qtypes(key_to_row_dict_qtypes):
  for qtype in key_to_row_dict_qtypes:
    keys_qtype = arolla.types.get_scalar_qtype(qtype)
    yield (arolla.make_dense_array_qtype(keys_qtype), qtype)


TEST_CASES = tuple(gen_cases(dict_test_utils.TEST_DATA))
QTYPE_SIGNATURES = frozenset(
    (keys.qtype, key_to_row_dict_qtype)
    for (keys, key_to_row_dict_qtype) in TEST_CASES
)


class DictMakeKeyToRowDictTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    expected_qtypes = frozenset(
        _get_key_qtypes(dict_test_utils.KEY_TO_ROW_DICT_QTYPES)
    )
    self.assertEqual(QTYPE_SIGNATURES, expected_qtypes)
    self.assertEqual(
        expected_qtypes,
        frozenset(
            pointwise_test_utils.detect_qtype_signatures(
                M.dict._make_key_to_row_dict
            )
        ),
    )

  @parameterized.parameters(*TEST_CASES)
  def testDictMake(self, keys, key_to_row_dict_qtype):
    res = self.eval(M.dict._make_key_to_row_dict(keys))
    self.assertEqual(res.qtype, key_to_row_dict_qtype)
    arolla.testing.assert_qvalue_allequal(arolla.eval(M.dict._keys(res)), keys)


if __name__ == '__main__':
  absltest.main()
