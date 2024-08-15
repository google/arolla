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

"""Tests for M.strings.substr."""

import contextlib
import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

M = arolla.M

TEST_DATA = (
    ('PAIN', 1, 3, 'AI'),
    ('infoops', 3, 'oops'),
    ('meeting', 0, 4, 'meet'),
    ('word', 'word'),
    ('word', 10, 0, ''),
    ('word', None, 5, 'word'),
    (None, 0, 2, None),
    ('too short', 40, 50, ''),
    ('too short', 1, 50, 'oo short'),
)

_encode = lambda x: None if x is None else x.encode('utf8')

TEST_DATA = (
    TEST_DATA
    + tuple((_encode(x[0]),) + x[1:-1] + (_encode(x[-1]),) for x in TEST_DATA)
    + (
        ('кукареку', 2, 5, 'кар'),
        ('кукареку'.encode('utf-8'), 4, 10, 'кар'.encode('utf-8')),
    )
)


def gen_qtype_signatures():
  for string_qtype in (arolla.TEXT, arolla.BYTES):
    for s, start, end in itertools.product(
        pointwise_test_utils.lift_qtypes(string_qtype),
        pointwise_test_utils.lift_qtypes(*arolla.types.INTEGRAL_QTYPES),
        pointwise_test_utils.lift_qtypes(*arolla.types.INTEGRAL_QTYPES),
    ):
      with contextlib.suppress(arolla.types.QTypeError):
        broadcasted_qtype = arolla.types.broadcast_qtype(
            (s, start, end), string_qtype
        )
        if arolla.is_array_qtype(broadcasted_qtype):
          yield (s, start, end, broadcasted_qtype)
        else:
          yield (s, start, end, s)
      with contextlib.suppress(arolla.types.QTypeError):
        broadcasted_qtype = arolla.types.broadcast_qtype(
            (s, start), string_qtype
        )
        if arolla.is_array_qtype(broadcasted_qtype):
          yield (s, start, broadcasted_qtype)
        else:
          yield (s, start, s)
      yield (s, s)


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class StringsSubstrTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    self.assertEqual(
        frozenset(
            pointwise_test_utils.detect_qtype_signatures(M.strings.substr)
        ),
        frozenset(QTYPE_SIGNATURES),
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def testValue(self, *args):
    actual_value = self.eval(M.strings.substr(*args[:-1]))
    arolla.testing.assert_qvalue_allequal(actual_value, args[-1])

  def testMissingStart(self):
    actual_value = self.eval(M.strings.substr('meeting', end=4))
    arolla.testing.assert_qvalue_allequal(
        actual_value, arolla.types.text('meet')
    )


if __name__ == '__main__':
  absltest.main()
