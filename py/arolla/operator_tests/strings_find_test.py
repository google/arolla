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

"""Tests for M.strings.find operator."""

import contextlib
import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

M = arolla.M

# Test data: tuple((arg, expected_result), ...)
TEST_DATA = (
    (None, None, None, None),
    ('Pride and Prejudice', 'Pr', 0, 10, 0),
    ('Pride and Prejudice', 'Pr', -10, -1, 10),
    ('Pride and Prejudice', 'Pr', 0, None, 0),
    ('Pride and Prejudice', 'Pr', 3, None, 10),
    ('Pride and Prejudice', 'Pride and Prejudice', 0, None, 0),
    ('Pride and Prejudice', '', 0, None, 0),
    ('Pride and Prejudice', 'Pr', 2, 1, None),
    ('Pride and Prejudice', 'Pr', 0, 0),
    ('Pride and Prejudice', 'Pr', 0),
    ('Pride and Prejudice', 'Sense', 0, None, None),
)

_encode = lambda x: None if x is None else x.encode('utf8')

TEST_DATA = TEST_DATA + tuple(
    (_encode(x[0]), _encode(x[1])) + x[2:] for x in TEST_DATA
)


def gen_qtype_signatures():
  lifted_ints = pointwise_test_utils.lift_qtypes(*arolla.types.INTEGRAL_QTYPES)
  for string_qtype in (arolla.TEXT, arolla.BYTES):
    lifted_strings = pointwise_test_utils.lift_qtypes(string_qtype)
    for s, substr in itertools.product(lifted_strings, repeat=2):
      with contextlib.suppress(arolla.types.QTypeError):
        yield (
            s,
            substr,
            arolla.types.broadcast_qtype((s, substr), arolla.OPTIONAL_INT64),
        )
      for start in lifted_ints:
        with contextlib.suppress(arolla.types.QTypeError):
          yield (
              s,
              substr,
              start,
              arolla.types.broadcast_qtype(
                  (s, substr, start), arolla.OPTIONAL_INT64
              ),
          )
        for end in lifted_ints:
          with contextlib.suppress(arolla.types.QTypeError):
            yield (
                s,
                substr,
                start,
                end,
                arolla.types.broadcast_qtype(
                    (s, substr, start, end), arolla.OPTIONAL_INT64
                ),
            )


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class StringsFindTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.strings.find, QTYPE_SIGNATURES)

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def testValue(self, *test_case):
    *args, expected = test_case
    actual = self.eval(M.strings.find(*args))
    arolla.testing.assert_qvalue_allequal(actual, expected)

  def testTextBytes(self):
    self.require_self_eval_is_called = False
    with self.assertRaisesRegex(
        ValueError, 'incompatible types s: TEXT and substr: BYTES'
    ):
      _ = M.strings.find('text', b'bytes')


if __name__ == '__main__':
  absltest.main()
