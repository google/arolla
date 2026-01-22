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

"""Tests for M.strings.decode operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils


def gen_test_data():
  """Yields test data for strings.decode operator.

  Yields: (arg_1, result)
  """
  for x in (
      'abcdefghijklmnopqrstuvwxyz',
      'αβγδεζηθικλμνξοπρσ/ςτυφχψω',
      'абвгдеёжзийклмнопрстуфхцчшщъыьэюя',
      'абвгґдеєжзиіїйклмнопрстуфхцчшщьюя',
      'אבגדהוזחטיךכלםמןנסעףפץצקרשתװױײ',
      # \uFFFD is handled specially by some ICU macros.
      '\uFFFD',
  ):
    yield (x.encode('utf8'), x)
    yield (x.encode('utf8'), 'strict', x)
    yield (x.encode('utf8'), 'ignore', x)
    yield (x.encode('utf8'), 'replace', x)


CORRECT_TEST_DATA = tuple(gen_test_data())
INCORRECT_TEST_DATA = (
    ('unexpected_continuation_byte', b'invalid:\x80 '),
    ('invalid_start_byte_0', b'invalid:\xC0 '),
    ('invalid_start_byte_1', b'invalid:\xC1 '),
    ('invalid_start_byte_2', b'invalid:\xF5 '),
    ('incomplete_multibyte_sequence_0', b'invalid:\xC2 '),
    ('incomplete_multibyte_sequence_1', b'invalid:\xE1\x80 '),
    ('incomplete_multibyte_sequence_2', b'invalid:\xF1\x80\x80 '),
    ('invalid_continuation_byte_0', b'invalid:\xC2\x41 '),
    ('invalid_continuation_byte_1', b'invalid:\xE1\x80\x41 '),
    ('invalid_continuation_byte_2', b'invalid:\xF1\x80\x80\x41 '),
    ('overlong_encoding_0', b'invalid:\xED\xA0\x80 '),
    ('overlong_encoding_1', b'invalid:\xE0\x9F\xBF '),
    ('overlong_encoding_2', b'invalid:\xF0\x8F\xBF\xBF '),
)

QTYPE_SIGNATURES = sum(
    [
        [(i, o), (i, arolla.TEXT, o)]
        for (i, o) in pointwise_test_utils.lift_qtypes(
            (arolla.BYTES, arolla.TEXT)
        )
    ],
    start=[],
)

M = arolla.M


class StringsDecodeTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.strings.decode, QTYPE_SIGNATURES)

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(CORRECT_TEST_DATA, *QTYPE_SIGNATURES)
  )
  def test_eval_correct(self, *args):
    actual_value = self.eval(M.strings.decode(*args[:-1]))
    arolla.testing.assert_qvalue_allequal(actual_value, args[-1])

  @parameterized.named_parameters(*INCORRECT_TEST_DATA)
  def test_eval_with_strict_errors(self, x):
    with self.assertRaisesRegex(
        ValueError, 'invalid UTF-8 sequence at position 8'
    ):
      self.eval(M.strings.decode(x, 'strict'))

  @parameterized.named_parameters(*INCORRECT_TEST_DATA)
  def test_eval_with_ignore_errors(self, x):
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.strings.decode(x, 'ignore')),
        arolla.text(x.decode(errors='ignore')),
    )

  @parameterized.named_parameters(*INCORRECT_TEST_DATA)
  def test_eval_with_replace_errors(self, x):
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.strings.decode(x, 'replace')),
        arolla.text(x.decode(errors='replace')),
    )

  def test_eval_with_invalid_errors(self):
    with self.assertRaisesRegex(
        ValueError,
        r"expected errors value to be one of \('strict', 'ignore', 'replace'\),"
        r" got 'invalid'",
    ):
      self.eval(M.strings.decode(b'foo', errors='invalid'))


if __name__ == '__main__':
  absltest.main()
