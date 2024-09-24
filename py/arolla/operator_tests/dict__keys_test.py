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

"""Tests for dict._keys operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import dict_test_utils
from arolla.operator_tests import pointwise_test_utils

M = arolla.M


def gen_cases(test_data):
  """Generates (key_to_row_dict, expected_result) test cases."""
  for test in test_data:
    keys = arolla.dense_array(test.dictionary.keys(), test.key_qtype)
    yield arolla.eval(M.dict._make_key_to_row_dict(keys)), keys


def gen_qtype_signatures():
  """Yields qtype signatures for core._contains."""
  for key_to_row_dict_qtype in dict_test_utils.KEY_TO_ROW_DICT_QTYPES:
    key_qtype = arolla.types.get_scalar_qtype(key_to_row_dict_qtype)
    yield key_to_row_dict_qtype, arolla.make_dense_array_qtype(key_qtype)


QTYPE_SIGNATURES = frozenset(gen_qtype_signatures())


class DictKeysTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    possible_qtypes = (
        pointwise_test_utils.DETECT_SIGNATURES_DEFAULT_QTYPES
        + tuple(dict_test_utils.KEY_TO_ROW_DICT_QTYPES)
    )
    arolla.testing.assert_qtype_signatures(
        M.dict._keys, QTYPE_SIGNATURES, possible_qtypes=possible_qtypes
    )

  @parameterized.parameters(gen_cases(dict_test_utils.TEST_DATA))
  def testDictGetRow(self, key_to_row_dict, expected_result):
    actual_value = self.eval(M.dict._keys(key_to_row_dict))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_result)


if __name__ == '__main__':
  absltest.main()
