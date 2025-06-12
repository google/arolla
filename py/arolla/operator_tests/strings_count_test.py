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

"""Tests for M.strings.count operator."""

import contextlib
import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

M = arolla.M

# Test data: tuple((arg, expected_result), ...)
TEXT_AND_BYTES_TEST_DATA = (
    (None, None, None),
    ('Pride and Prejudice', None, None),
    (None, 'Sense', None),
    ('', '', 1),
    # Note that the following test passes for both str and bytes only because
    # all the caracters are ASCII.
    ('Pride and Prejudice', '', 20),
    ('Pride and Prejudice', 'Pride', 1),
    ('Pride and Pride Once More', 'Pride', 2),
    ('Pride and Prejudice', 'Sense', 0),
    ('Гордость и Предубеждение', 'Гордость', 1),
    ('Гордость и еще раз Гордость', 'Гордость', 2),
    ('Гордость и Предубеждение', 'Смысл', 0),
)

UNICODE_TEST_DATA = (
    ('Г', '', 2),
    ('Г'.encode(), '', 2),
    ('Гордость и Предубеждение', '', 25),
    ('Гордость и Предубеждение'.encode(), b'', 47),
)

_encode = lambda x: None if x is None else x.encode()
TEST_DATA = TEXT_AND_BYTES_TEST_DATA + tuple(
    (_encode(x[0]), _encode(x[1]), x[2]) for x in TEXT_AND_BYTES_TEST_DATA
) + UNICODE_TEST_DATA


def gen_qtype_signatures():
  for string_qtype in (arolla.TEXT, arolla.BYTES):
    for s, substr in itertools.product(
        pointwise_test_utils.lift_qtypes(string_qtype),
        repeat=2,
    ):
      with contextlib.suppress(arolla.types.QTypeError):
        yield s, substr, arolla.types.broadcast_qtype((s, substr), arolla.INT64)


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class StringsCountTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    self.assertEqual(
        frozenset(QTYPE_SIGNATURES),
        frozenset(
            pointwise_test_utils.detect_qtype_signatures(M.strings.count)
        ),
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def testValue(self, arg1, arg2, expected_value):
    actual_value = self.eval(M.strings.count(arg1, arg2))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == '__main__':
  absltest.main()
