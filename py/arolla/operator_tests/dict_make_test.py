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
  """Generates (keys, values, dict_qtype, py_dict) test cases."""
  for test in test_data:
    py_dict = test.dictionary
    dict_qtype = arolla.types.make_dict_qtype(test.key_qtype, test.value_qtype)
    for array_mod in (arolla.dense_array, arolla.array):
      keys = array_mod(py_dict.keys(), value_qtype=test.key_qtype)
      values = array_mod(py_dict.values(), value_qtype=test.value_qtype)
      yield (keys, values, dict_qtype, py_dict)


def _get_key_value_qtypes(dict_qtypes):
  for dict_qtype in dict_qtypes:
    field_qtypes = arolla.abc.get_field_qtypes(dict_qtype)
    keys_qtype = arolla.types.get_scalar_qtype(field_qtypes[0])
    values_qtype = arolla.types.get_scalar_qtype(field_qtypes[1])
    for array_qtype_mod in (
        arolla.make_dense_array_qtype,
        arolla.make_array_qtype,
    ):
      yield (
          array_qtype_mod(keys_qtype),
          array_qtype_mod(values_qtype),
          dict_qtype,
      )


TEST_CASES = tuple(gen_cases(dict_test_utils.TEST_DATA))
QTYPE_SIGNATURES = frozenset(
    (keys.qtype, values.qtype, dict_qtype)
    for (keys, values, dict_qtype, _) in TEST_CASES
)


class DictMakeTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    expected_qtypes = frozenset(
        _get_key_value_qtypes(dict_test_utils.DICT_QTYPES)
    )
    self.assertEqual(QTYPE_SIGNATURES, expected_qtypes)

  @parameterized.parameters(*TEST_CASES)
  def testDictMake(self, keys, values, dict_qtype, py_dict):
    res = self.eval(M.dict.make(keys, values))
    self.assertEqual(res.qtype, dict_qtype)
    arolla.testing.assert_qvalue_allequal(
        res.keys(), arolla.eval(M.array.as_dense_array(keys))
    )
    arolla.testing.assert_qvalue_allequal(
        res.values(), arolla.eval(M.array.as_dense_array(values))
    )

  def testGetNth(self):
    # Dicts are represented as tuple, and we rely core.get_nth working for dicts
    # in several operator implementations. We test this here rather than in
    # `core_get_nth_test.py` since dict relies on get_nth like a client.
    keys = arolla.dense_array_int32([1, 3])
    values = arolla.dense_array_float32([2.0, 4.0])
    d = arolla.eval(M.dict.make(keys, values))
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        self.eval(M.core.get_nth(d, 0)),
        arolla.eval(M.dict._make_key_to_row_dict(keys)),
    )
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.core.get_nth(d, 1)), values
    )


if __name__ == '__main__':
  absltest.main()
