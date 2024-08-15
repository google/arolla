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

"""Tests for M.strings.contains_regex."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

M = arolla.M

TEST_DATA = (
    (None, 'a.c', None),
    ('ac', 'a.c', None),
    ('ac', 'a.c', None),
    ('ac', 'a.c', None),
    ('cccaaa', 'a.c', None),
    ('cccbaaa', 'a.c', None),
    ('aaaccc', 'a.c', True),
    ('aaabccc', 'a.c', True),
    ('Pride and Prejudice', 'Pride', True),
    ('Pride and Prejudice', 'pride', None),
    ('Pride and Prejudice', '(?i)pride', True),
    ('Pride and Prejudice', 'Sense', None),
)

QTYPE_SIGNATURES = (
    (arolla.TEXT, arolla.TEXT, arolla.OPTIONAL_UNIT),
    (arolla.OPTIONAL_TEXT, arolla.TEXT, arolla.OPTIONAL_UNIT),
    (arolla.ARRAY_TEXT, arolla.TEXT, arolla.ARRAY_UNIT),
    (arolla.DENSE_ARRAY_TEXT, arolla.TEXT, arolla.DENSE_ARRAY_UNIT),
)


class StringsContainsRegexTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    self.maxDiff = None
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(M.strings.contains_regex),
        QTYPE_SIGNATURES,
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def test_eval(self, arg1, arg2, expected_value):
    actual_value = self.eval(M.strings.contains_regex(arg1, arg2))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == '__main__':
  absltest.main()
