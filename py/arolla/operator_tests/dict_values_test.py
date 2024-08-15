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

"""Tests for dict.values operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import dict_test_utils

M = arolla.M


def gen_cases(test_data):
  """Generates (dict, values) test cases."""
  for test in test_data:
    dict_qvalue = arolla.types.Dict(
        test.dictionary, key_qtype=test.key_qtype, value_qtype=test.value_qtype
    )
    yield (
        dict_qvalue,
        arolla.dense_array(
            test.dictionary.values(), value_qtype=test.value_qtype
        ),
    )


class DictValuesTest(parameterized.TestCase):

  # TODO(b/148658339) Test detect_qtype_signatures(M.dict.get) once we have can
  # construct dict QTypes in Python.

  @parameterized.parameters(gen_cases(dict_test_utils.TEST_DATA))
  def testDictGet(self, dict_qvalue, values_qvalue):
    arolla.testing.assert_qvalue_allequal(
        arolla.eval(M.dict.values(dict_qvalue)), values_qvalue
    )


if __name__ == '__main__':
  absltest.main()
