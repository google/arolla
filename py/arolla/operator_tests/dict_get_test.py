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

"""Tests for dict.get operator."""

import contextlib

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import dict_test_utils
from arolla.operator_tests import pointwise_test_utils

M = arolla.M


def get_implicitly_castable_qtypes(qtype):
  """Yields all qtypes that can be implicitly casted to qtype."""
  assert qtype in arolla.types.SCALAR_QTYPES
  for other_qtype in arolla.types.SCALAR_QTYPES:
    with contextlib.suppress(arolla.types.QTypeError):
      if arolla.types.common_qtype(qtype, other_qtype) == qtype:
        yield other_qtype


def gen_cases(test_data):
  """Generates (dict, keys_to_query, values_to_expect) test cases."""
  for test in test_data:
    dict_qvalue = arolla.types.Dict(
        test.dictionary, key_qtype=test.key_qtype, value_qtype=test.value_qtype
    )
    rows = tuple(test.dictionary.items()) + tuple(
        (k, None) for k in test.missing_keys
    )
    qtype_sets = []
    for qtype in get_implicitly_castable_qtypes(test.key_qtype):
      qtype_sets.extend([
          (qtype, arolla.make_optional_qtype(test.value_qtype)),
          (
              arolla.make_optional_qtype(qtype),
              arolla.make_optional_qtype(test.value_qtype),
          ),
          (
              arolla.make_dense_array_qtype(qtype),
              arolla.make_dense_array_qtype(test.value_qtype),
          ),
          (
              arolla.make_array_qtype(qtype),
              arolla.make_array_qtype(test.value_qtype),
          ),
      ])
    for keys_to_query, values_to_expect in pointwise_test_utils.gen_cases(
        rows, *qtype_sets
    ):
      yield (dict_qvalue, keys_to_query, values_to_expect)


TEST_CASES = tuple(gen_cases(dict_test_utils.TEST_DATA))
QTYPE_SIGNATURES = frozenset(
    (d.qtype, keys.qtype, values.qtype) for (d, keys, values) in TEST_CASES
)


class DictGetTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    possible_qtypes = (
        pointwise_test_utils.DETECT_SIGNATURES_DEFAULT_QTYPES
        + tuple(dict_test_utils.DICT_QTYPES)
    )
    arolla.testing.assert_qtype_signatures(
        M.dict.get, QTYPE_SIGNATURES, possible_qtypes=possible_qtypes
    )

  @parameterized.parameters(*TEST_CASES)
  def testDictGet(self, dict_qvalue, keys_to_query, values_to_expect):
    actual_value = self.eval(M.dict.get(dict_qvalue, keys_to_query))
    arolla.testing.assert_qvalue_allequal(actual_value, values_to_expect)


if __name__ == '__main__':
  absltest.main()
