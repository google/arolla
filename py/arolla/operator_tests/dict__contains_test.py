# Copyright 2025 Google LLC
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

"""Tests for dict._contains operator."""

import contextlib

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import dict_test_utils
from arolla.operator_tests import pointwise_test_utils

M = arolla.M


def gen_cases(test_data):
  """Generates (dict, keys_to_query, values_to_expect) test cases."""
  for test in test_data:
    dict_qvalue = arolla.types.Dict(
        test.dictionary, key_qtype=test.key_qtype, value_qtype=test.value_qtype
    )
    key_to_row_dict = arolla.eval(arolla.M.core.get_first(dict_qvalue))
    rows = (
        *((k, True) for k in test.dictionary),
        *((k, None) for k in test.missing_keys),
    )
    qtype_sets = (
        (test.key_qtype, arolla.make_optional_qtype(arolla.UNIT)),
        (
            arolla.make_optional_qtype(test.key_qtype),
            arolla.make_optional_qtype(arolla.UNIT),
        ),
        (
            arolla.make_dense_array_qtype(test.key_qtype),
            arolla.make_dense_array_qtype(arolla.UNIT),
        ),
        (
            arolla.make_array_qtype(test.key_qtype),
            arolla.make_array_qtype(arolla.UNIT),
        ),
    )
    for keys_to_query, values_to_expect in pointwise_test_utils.gen_cases(
        rows, *qtype_sets
    ):
      yield (key_to_row_dict, keys_to_query, values_to_expect)


def gen_qtype_signatures():
  """Yields qtype signatures for core._contains."""
  # value_qtype does not matter for dict._contains.
  for key_qtype in dict_test_utils.DICT_KEY_QTYPES:
    key_to_row_dict_qtype = arolla.types.make_key_to_row_dict_qtype(key_qtype)
    for requested_key_qtype in arolla.types.SCALAR_QTYPES:
      with contextlib.suppress(arolla.types.QTypeError):
        if (
            arolla.types.common_qtype(key_qtype, requested_key_qtype)
            == key_qtype
        ):
          yield from (
              (key_to_row_dict_qtype, k, r)
              for k, r in pointwise_test_utils.lift_qtypes(
                  (requested_key_qtype, arolla.OPTIONAL_UNIT)
              )
          )


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class DictGetRowTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    possible_qtypes = (
        pointwise_test_utils.DETECT_SIGNATURES_DEFAULT_QTYPES
        + tuple(dict_test_utils.KEY_TO_ROW_DICT_QTYPES)
        + tuple(dict_test_utils.DICT_QTYPES)
    )
    arolla.testing.assert_qtype_signatures(
        M.dict._contains, QTYPE_SIGNATURES, possible_qtypes=possible_qtypes
    )

  @parameterized.parameters(gen_cases(dict_test_utils.TEST_DATA))
  def testDictGetRow(self, dict_qvalue, keys_to_query, values_to_expect):
    actual_value = self.eval(M.dict._contains(dict_qvalue, keys_to_query))
    arolla.testing.assert_qvalue_allequal(actual_value, values_to_expect)


if __name__ == '__main__':
  absltest.main()
