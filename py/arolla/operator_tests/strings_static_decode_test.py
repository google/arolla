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

"""Tests for M.strings.static_decode."""

import re
from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla


M = arolla.M
P = arolla.P


def gen_test_cases():
  for x in (
      'abcdefghijklmnopqrstuvwxyz',
      'αβγδεζηθικλμνξοπρσ/ςτυφχψω',
      'абвгдеёжзийклмнопрстуфхцчшщъыьэюя',
      'абвгґдеєжзиіїйклмнопрстуфхцчшщьюя',
      'אבגדהוזחטיךכלםמןנסעףפץצקרשתװױײ',
      # \uFFFD is handled specially by some ICU macros.
      '\uFFFD',
  ):
    yield from (
        (arolla.bytes(x.encode('utf8')), arolla.text(x)),
        (arolla.optional_bytes(x.encode('utf8')), arolla.optional_text(x)),
    )
    yield (arolla.optional_bytes(None), arolla.optional_text(None))


TEST_CASES = tuple(gen_test_cases())


class StringsStaticDecodeTest(parameterized.TestCase):

  @parameterized.parameters(*TEST_CASES)
  def test_literal_propagation(self, *test_case):
    args = test_case[:-1]
    expected_result = test_case[-1]
    arolla.testing.assert_qvalue_allequal(
        M.strings.static_decode(*args).qvalue,
        expected_result,
    )

  @parameterized.parameters(*TEST_CASES)
  def test_lowering(self, *test_case):
    args = test_case[:-1]
    expected_result = test_case[-1]
    arolla.testing.assert_expr_equal_by_fingerprint(
        arolla.abc.to_lowest(M.strings.static_decode(*args)),
        arolla.literal(expected_result),
    )

  def test_error_bad_utf8(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('invalid UTF-8 sequence at position 3')
    ):
      _ = M.strings.static_decode(arolla.bytes(b'xyz\xc2\xc2'))

  def test_expected_scalar(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected a bytes literal, got x: ARRAY_BYTES')
    ):
      _ = M.strings.static_decode(arolla.array_bytes([]))

  def test_expected_literal(self):
    with self.assertRaisesRegex(ValueError, re.escape('`x` must be a literal')):
      _ = M.strings.static_decode(M.annotation.qtype(P.x, arolla.BYTES))


if __name__ == '__main__':
  absltest.main()
