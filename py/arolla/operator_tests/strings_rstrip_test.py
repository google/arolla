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

"""Tests for M.strings.rstrip operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

M = arolla.M

TEST_DATA = (
    (' \nrstrip\n\t ', ' \nrstrip'),
    (
        ' \nrstrip with inner whitespaces\n  ',
        ' \nrstrip with inner whitespaces',
    ),
    ('no strip', 'no strip'),
    (' \n\t ', ''),
    ('abbstripbaab', 'ab', 'abbstrip'),
    ('abbprotectabmiddleaa', 'ab', 'abbprotectabmiddle'),
    ('aabbaabab', 'ab', ''),
    ('intact', '', 'intact'),
    ('intact', 'sv', 'intact'),
    ('', 'ab', ''),
)

_encode_tuple = lambda t: tuple(
    x.encode('utf-8') if isinstance(x, str) else x for x in t
)
_tuple_with_unspecified = lambda t: any(x == arolla.UNSPECIFIED for x in t)

TEST_DATA = TEST_DATA + tuple(_encode_tuple(t) for t in TEST_DATA)


def gen_qtype_signatures():
  # We iterate on the type of the result
  for s in pointwise_test_utils.lift_qtypes(arolla.TEXT, arolla.BYTES):
    yield s, s
    yield s, arolla.UNSPECIFIED, s
    yield s, s, s

    if arolla.is_scalar_qtype(s):
      yield s, arolla.make_optional_qtype(s), s
    if arolla.is_optional_qtype(s):
      yield s, arolla.types.get_scalar_qtype(s), s

    if arolla.is_array_qtype(s):
      yield s, arolla.types.get_scalar_qtype(s), s
      yield arolla.types.get_scalar_qtype(s), s, s
      yield s, arolla.types.make_optional_qtype(
          arolla.types.get_scalar_qtype(s)
      ), s
      yield arolla.types.make_optional_qtype(
          arolla.types.get_scalar_qtype(s)
      ), s, s


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class StringsRStripTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.strings.rstrip, QTYPE_SIGNATURES)

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(
          TEST_DATA,
          *(
              signature
              for signature in QTYPE_SIGNATURES
              if not _tuple_with_unspecified(signature)
          )
      )
  )
  def test_eval(self, *args):
    actual_value = self.eval(M.strings.rstrip(*args[:-1]))
    arolla.testing.assert_qvalue_allequal(actual_value, args[-1])

  @parameterized.parameters(
      ('\u0117\u0118\u0119', '\u0117\u0119', '\u0117\u0118'),
      ('\u0117\u0119\u0118\u0119\u0117', '\u0117\u0119', '\u0117\u0119\u0118'),
      (
          '\u0117\u0118\u0119\u0118\u0119',
          '\u0117\u0119',
          '\u0117\u0118\u0119\u0118',
      ),
      ('\u0117\u0119', '\u0117\u0119', ''),
  )
  def test_eval_on_unicode_chars(self, text, chars, result):
    self.assertEqual(self.eval(M.strings.rstrip(text, chars)), result)


if __name__ == '__main__':
  absltest.main()
