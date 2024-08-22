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

"""Tests for dict_qvalues."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla.operator_tests import dict_test_utils
from arolla.types.qtype import dict_qtypes
from arolla.types.qtype import scalar_qtypes
from arolla.types.qvalue import dense_array_qvalues
from arolla.types.qvalue import dict_qvalues


class DictQValueTest(parameterized.TestCase):

  @parameterized.parameters(dict_test_utils.TEST_DATA)
  def testConstructor(
      self, dictionary, unused_missing_keys, key_qtype, value_qtype
  ):
    dict_qvalue = dict_qvalues.Dict(
        dictionary, key_qtype=key_qtype, value_qtype=value_qtype
    )
    self.assertIsInstance(dict_qvalue, dict_qvalues.Dict)
    self.assertEqual(
        dict_qvalue.qtype,
        dict_qtypes.make_dict_qtype(key_qtype, value_qtype),
    )
    # We can have unit values, where False is converted to True after being
    # wrapped into a DenseArray.
    if value_qtype == scalar_qtypes.UNIT:
      dictionary = {
          k: True if v is False else v  # pylint: disable=g-bool-id-comparison
          for k, v in dictionary.items()
      }
    self.assertEqual(dict_qvalue.py_value(), dictionary)

  @parameterized.parameters(dict_test_utils.TEST_DATA)
  def testConstructor_NoExplicitTypes(
      self,
      dictionary,
      unused_missing_keys,
      unused_key_qtype,
      unused_value_qtype,
  ):
    dict_qvalue = dict_qvalues.Dict(dictionary)
    self.assertIsInstance(dict_qvalue, dict_qvalues.Dict)
    self.assertEqual(dict_qvalue.py_value(), dictionary)

  @parameterized.parameters(dict_test_utils.TEST_DATA)
  def testKeys(self, dictionary, unused_missing_keys, key_qtype, value_qtype):
    dict_qvalue = dict_qvalues.Dict(
        dictionary, key_qtype=key_qtype, value_qtype=value_qtype
    )
    keys = dict_qvalue.keys()
    self.assertIsInstance(keys, dense_array_qvalues.DenseArray)
    self.assertEqual(keys.value_qtype, key_qtype)
    self.assertEqual(keys.py_value(), list(dictionary.keys()))

  @parameterized.parameters(dict_test_utils.TEST_DATA)
  def testValues(self, dictionary, unused_missing_keys, key_qtype, value_qtype):
    dict_qvalue = dict_qvalues.Dict(
        dictionary, key_qtype=key_qtype, value_qtype=value_qtype
    )
    values = dict_qvalue.values()
    self.assertIsInstance(values, dense_array_qvalues.DenseArray)
    self.assertEqual(values.value_qtype, value_qtype)
    # We can have unit values, where False is converted to True after being
    # wrapped into a DenseArray.
    if value_qtype == scalar_qtypes.UNIT:
      dictionary = {
          k: True if v is False else v  # pylint: disable=g-bool-id-comparison
          for k, v in dictionary.items()
      }
    self.assertEqual(values.py_value(), list(dictionary.values()))


if __name__ == '__main__':
  absltest.main()
