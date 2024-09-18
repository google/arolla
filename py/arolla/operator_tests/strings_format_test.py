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

"""Tests for M.strings.format operator."""

import contextlib
import inspect
import itertools
import string

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

M = arolla.M


def gen_single_format_arg_test_data():
  """Yields test data for strings.format operator.

  Yields: (fmt, arg_names, arg, result)
  """
  yield ('missing value is {d}', 'd', None, None)
  yield ('float value is {d}!', 'd', 5.75, 'float value is 5.75!')
  yield (
      'int value is {d}',
      'd',
      pointwise_test_utils.IntOnly(57),
      'int value is 57',
  )
  yield ('boolean value is {d}', 'd', True, 'boolean value is true')
  yield ('boolean value is {d}', 'd', False, 'boolean value is false')
  yield (
      'text value is "{d}"',
      'd',
      'fifty seven',
      'text value is "fifty seven"',
  )
  yield (b'missing value is {d}', 'd', None, None)
  yield (b'value is {d}', 'd', b'VAL', b'value is VAL')
  yield (b'float value is {d}!', 'd', 5.75, b'float value is 5.75!')
  yield (
      b'value is {d}',
      'd',
      pointwise_test_utils.IntOnly(17),
      b'value is 17',
  )
  yield (b'value is {d}', 'd', True, b'value is true')


SINGLE_FORMAT_ARG_TEST_DATA = tuple(gen_single_format_arg_test_data())


def gen_signatures(num_format_args):
  for scalar_fmt in (arolla.BYTES, arolla.TEXT):
    for fmt in pointwise_test_utils.lift_qtypes(scalar_fmt):
      scalar_arg_types = (
          scalar_fmt,  # We don't allow to mix TEXT and BYTES.
          arolla.FLOAT32,
          arolla.FLOAT64,
          arolla.WEAK_FLOAT,
          arolla.INT32,
          arolla.INT64,
          arolla.BOOLEAN,
      )
      arg_types = pointwise_test_utils.lift_qtypes(*scalar_arg_types)

      for args in itertools.product(arg_types, repeat=num_format_args):
        with contextlib.suppress(arolla.types.QTypeError):
          yield (
              fmt,
              arolla.TEXT,  # arg_names
              *args,
              arolla.types.broadcast_qtype(args, fmt),
          )


class StringsFormatTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):
  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    actual_signatures = pointwise_test_utils.detect_qtype_signatures(
        M.strings.format, max_arity=3
    )
    arolla.testing.assert_qtype_signatures_equal(
        list(gen_signatures(num_format_args=0))
        + list(gen_signatures(num_format_args=1)),
        actual_signatures,
    )
    # sanity check that arrays are supported
    self.assertIn(
        (
            arolla.DENSE_ARRAY_TEXT,
            arolla.TEXT,
            arolla.DENSE_ARRAY_FLOAT64,
            arolla.DENSE_ARRAY_TEXT,
        ),
        actual_signatures,
    )
    self.assertIn(
        (
            arolla.ARRAY_BYTES,
            arolla.TEXT,
            arolla.ARRAY_INT64,
            arolla.ARRAY_BYTES,
        ),
        actual_signatures,
    )
    # sanity check that arg names are not lifted
    for arg_names in [arolla.ARRAY_TEXT, arolla.BYTES]:
      self.assertNotIn(
          (
              arolla.ARRAY_BYTES,
              arg_names,
              arolla.ARRAY_INT64,
              arolla.ARRAY_BYTES,
          ),
          actual_signatures,
      )
    # TEXT format do not allow bytes
    self.assertNotIn(
        (
            arolla.TEXT,
            arolla.TEXT,
            arolla.BYTES,
            arolla.TEXT,
        ),
        actual_signatures,
    )

  def testOperatorSignature(self):
    self.require_self_eval_is_called = False
    expected_signature = inspect.signature(
        lambda fmt, *args, **kwargs: None
    )
    self.assertEqual(inspect.signature(M.strings.format), expected_signature)

  @parameterized.parameters(
      *pointwise_test_utils.gen_cases(
          SINGLE_FORMAT_ARG_TEST_DATA, *gen_signatures(num_format_args=1)
      )
  )
  def testOneArgValue(self, fmt, arg_names, arg, expected):
    self.assertEqual(arg_names, 'd')
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.strings.format(fmt, d=arg)), expected
    )
    # classic signature
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.strings.format(fmt, 'd', arg)), expected
    )

  def testManyArgValue(self):
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.strings.format('a={a}, b={b}, c={c}!', a=1, b=7.0, c='x')),
        arolla.as_qvalue('a=1, b=7., c=x!'),
    )
    # classic signature
    arolla.testing.assert_qvalue_allequal(
        self.eval(
            M.strings.format('a={a}, b={b}, c={c}!', 'a,b,c', 1, 7.0, 'x')
        ),
        arolla.as_qvalue('a=1, b=7., c=x!'),
    )

  def testIgnoreExtra(self):
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.strings.format('a={a}!', a=1, b=7.0, c='x')),
        arolla.as_qvalue('a=1!'),
    )
    # classic signature
    arolla.testing.assert_qvalue_allequal(
        self.eval(
            M.strings.format('a={a}!', 'a,b,c', 1, 7.0, 'x')
        ),
        arolla.as_qvalue('a=1!'),
    )

  def testNothingToFormat(self):
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.strings.format('a=nothing!')),
        arolla.as_qvalue('a=nothing!'),
    )
    # classic signature
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.strings.format('a=nothing!', '')),
        arolla.as_qvalue('a=nothing!'),
    )

  def testNothingToFormatBytes(self):
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.strings.format(b'a=nothing!')),
        arolla.as_qvalue(b'a=nothing!'),
    )
    # classic signature
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.strings.format(b'a=nothing!', '')),
        arolla.as_qvalue(b'a=nothing!'),
    )

  @parameterized.parameters(
      (
          '{{',
          '{',
      ),
      (
          '}}',
          '}',
      ),
      (
          '{{}}',
          '{}',
      ),
  )
  def testEscaping(self, s, res):
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.strings.format(s)),
        arolla.as_qvalue(res),
    )

  @parameterized.parameters(
      '\\"',
      '\\\\\\',
      '\\"\\"\\"',
  )
  def testNoEscaping(self, s):
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.strings.format(s)),
        arolla.as_qvalue(s),
    )

  def testNothingToFormatAllPairs(self):
    for s in itertools.product(string.printable, repeat=2):
      s = ''.join(s)
      try:
        res = s.format()
      except Exception:  # pylint: disable=broad-except
        continue  # it is fine

      arolla.testing.assert_qvalue_allequal(
          self.eval(M.strings.format(s)),
          arolla.as_qvalue(res),
      )

  @parameterized.parameters(
      (
          '{}',
          'incorrect arg name',
      ),
      (
          '{0a}',
          'incorrect arg name',
      ),
      (
          '{{a}',
          'incorrect format specification',
      ),
      (
          '{a}}',
          'incorrect format specification',
      ),
      (
          '{{}',
          'incorrect format specification',
      ),
      (
          '{\\{}',
          'incorrect format specification',
      ),
      (
          '{_non_existing}',
          'argument name.*_non_existing.*is not found',
      ),
  )
  def testFormatSpecErrors(self, fmt, expected_error_regexp):
    with self.assertRaisesRegex(ValueError, expected_error_regexp):
      self.eval(M.strings.format(fmt, a=1))


if __name__ == '__main__':
  absltest.main()
