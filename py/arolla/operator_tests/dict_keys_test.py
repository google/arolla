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

M = arolla.M


def gen_cases(test_data):
  """Generates (dict, keys) test cases."""
  for test in test_data:
    dict_qvalue = arolla.types.Dict(
        test.dictionary, key_qtype=test.key_qtype, value_qtype=test.value_qtype
    )
    yield (
        dict_qvalue,
        arolla.dense_array(test.dictionary.keys(), value_qtype=test.key_qtype),
    )


def _get_keys_qtype(dict_qtype):
  field_qtypes = arolla.abc.get_field_qtypes(dict_qtype)
  keys_qtype = arolla.types.get_scalar_qtype(field_qtypes[0])
  return arolla.make_dense_array_qtype(keys_qtype)


TEST_CASES = tuple(gen_cases(dict_test_utils.TEST_DATA))
QTYPE_SIGNATURES = frozenset((d.qtype, keys.qtype) for d, keys in TEST_CASES)


class DictKeysTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    expected_qtypes = frozenset(
        (dict_qtype, _get_keys_qtype(dict_qtype))
        for dict_qtype in dict_test_utils.DICT_QTYPES
    )
    self.assertEqual(QTYPE_SIGNATURES, expected_qtypes)

  @parameterized.parameters(*TEST_CASES)
  def testDictKeys(self, dict_qvalue, keys_qvalue):
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.dict.keys(dict_qvalue)), keys_qvalue
    )


if __name__ == '__main__':
  absltest.main()
