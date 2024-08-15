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

"""Tests for M.strings.find."""

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
    (None, None, None, None, None),
    ('Pride and Prejudice', 'Pr', 0, 10, 239, 0),
    ('Pride and Prejudice', 'Pr', -10, -1, None, 10),
    ('Pride and Prejudice', 'Pr', 0, None, None, 0),
    ('Pride and Prejudice', 'Pr', 3, None, None, 10),
    ('Pride and Prejudice', 'Pride and Prejudice', 0, None, None, 0),
    ('Pride and Prejudice', '', 0, None, None, 0),
    ('Pride and Prejudice', 'Pr', 2, 1, None, None),
    ('Pride and Prejudice', 'Sense', 0, None, 239, 239),
    ('Pride and Prejudice', 'Pr', 0, None, 0),
    ('Pride and Prejudice', 'Pr', 0, 0),
    ('Pride and Prejudice', 'Pr', 0),
)

_encode = lambda x: None if x is None else x.encode('utf8')

TEST_DATA = TEST_DATA + tuple(
    (_encode(x[0]), _encode(x[1])) + x[2:] for x in TEST_DATA
)


def gen_qtype_signatures():
  for string_qtype in (arolla.TEXT, arolla.BYTES):
    lifted_strings = pointwise_test_utils.lift_qtypes(string_qtype)
    for s, substr, start, end, failure_value in itertools.product(
        lifted_strings,
        lifted_strings,
        arolla.types.INTEGRAL_QTYPES,
        arolla.types.INTEGRAL_QTYPES,
        pointwise_test_utils.lift_qtypes(*arolla.types.INTEGRAL_QTYPES),
    ):
      with contextlib.suppress(arolla.types.QTypeError):
        yield (
            s,
            substr,
            start,
            end,
            failure_value,
            arolla.types.broadcast_qtype(
                (s, substr, failure_value), arolla.INT64
            ),
        )


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class StringsFindTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False

    @arolla.optools.as_lambda_operator('find_with_fixed_offset')
    def find_with_fixed_args(s, substr, failure_value):
      return M.strings.find(s, substr, failure_value=failure_value)

    detected_qtypes = set()
    for sig in pointwise_test_utils.detect_qtype_signatures(
        find_with_fixed_args
    ):
      for start, end in itertools.product(
          arolla.types.INTEGRAL_QTYPES, repeat=2
      ):
        detected_qtypes.add(sig[:2] + (start, end) + sig[2:])

    self.assertEqual(
        frozenset(QTYPE_SIGNATURES),
        frozenset(detected_qtypes),
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def testValue(self, arg1, arg2, arg3, arg4, arg5, expected_value):
    actual_value = self.eval(M.strings.find(arg1, arg2, arg3, arg4, arg5))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)

  def testTextBytes(self):
    self.require_self_eval_is_called = False
    with self.assertRaisesRegex(ValueError, 'unsupported argument types'):
      _ = M.strings.find('text', b'bytes')


if __name__ == '__main__':
  absltest.main()
