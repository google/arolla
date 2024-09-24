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


def gen_float_formats():
  """Yields a list of different float formats to test."""
  for suffix in ('f', 'g', 'e'):
    yield '{d:%s}' % suffix
    for i in range(10):
      yield '{d:.%d%s}' % (i, suffix)
    for i, j in itertools.product(range(10), repeat=2):
      yield '{d:0%d.%d%s}' % (i, j, suffix)
  for i in range(10):
    yield '{d:.%d}' % i
    for j in range(10):
      yield '{d:0%d.%d}' % (i, j)


def gen_float_format_test_data():
  """Yields float formatting test data for strings.format operator.

  Yields: (fmt, arg_name, arg, result)
  """
  for fmt in gen_float_formats():
    yield (fmt, 'd', None, None)
    # For floats we assume that `f` is at the end if nothing is specified.
    py_fmt = fmt if fmt[-2].isalpha() else fmt[:-1] + 'f}'
    d = 7.125  # fraction is 2^-3 to have exact representation in float32
    yield (fmt, 'd', d, py_fmt.format(d=d))
    # Skip integers for formats without `f` at the end.
    if fmt == py_fmt:
      d = 12
      yield (fmt, 'd', d, py_fmt.format(d=d))


def gen_int_format_test_data():
  """Yields float formatting test data for strings.format operator.

  Yields: (fmt, arg_name, arg, result)
  """
  formats = ['{d:d}']
  for i in range(10):
    formats += ['{d:%d}' % i, '{d:%dd}' % i]
    formats += ['{d:0%d}' % i, '{d:0%dd}' % i]
  for fmt in formats:
    yield (fmt, 'd', None, None)
    d = 57
    yield (fmt, 'd', pointwise_test_utils.IntOnly(d), fmt.format(d=d))


def gen_v_format_test_data():
  """Yields :v formatting test data for strings.format operator.

  Yields: (fmt, arg_name, arg, result)
  """
  fmt = '{d:v}'
  yield (fmt, 'd', None, None)
  yield (fmt, 'd', pointwise_test_utils.IntOnly(57), '57')
  yield (fmt, 'd', 5.7, '5.7')
  yield (fmt, 'd', '57', '57')
  yield (fmt.encode(), 'd', b'57', b'57')


def gen_single_format_arg_test_data():
  """Yields test data for strings.format operator.

  Yields: (fmt, arg_name, arg, result)
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
  yield from gen_float_format_test_data()
  yield from gen_int_format_test_data()
  yield from gen_v_format_test_data()


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
  def testOneArgValue(self, fmt, arg_name, arg, expected):
    self.assertEqual(arg_name, 'd')
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

  def testExamplesFromDocstring(self):
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.strings.format('Hello {n}!', n='World')),
        arolla.as_qvalue('Hello World!'),
    )
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.strings.format('{a} + {b} = {c}', a=1, b=2, c=3)),
        arolla.as_qvalue('1 + 2 = 3'),
    )
    arolla.testing.assert_qvalue_allequal(
        self.eval(
            M.strings.format('{a} + {b} = {c}', a=[1, 3], b=[2, 1], c=[3, 4])
        ),
        arolla.as_qvalue(['1 + 2 = 3', '3 + 1 = 4']),
    )
    fmt = (
        '({a:03} + {b:e}) * {c:.2f} ='
        ' {a:02d} * {c:3d} + {b:07.3f} * {c:08.4f}'
    )
    arolla.testing.assert_qvalue_allequal(
        self.eval(
            M.strings.format(
                fmt,
                a=5,
                b=5.7,
                c=75,
            )
        ),
        arolla.as_qvalue(
            '(005 + 5.700000e+00) * 75.00 = 05 *  75 + 005.700 * 075.0000'
        ),
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
          'incorrect arg',
      ),
      (
          '{0a}',
          'incorrect arg',
      ),
      (
          '{{a}',
          'incorrect format specification',
      ),
      (
          '}foo}',
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
          'incorrect arg',
      ),
      (
          '{_non_existing}',
          'argument name.*_non_existing.*is not found',
      ),
  )
  def testFormatSpecErrors(self, fmt, expected_error_regexp):
    with self.assertRaisesRegex(ValueError, expected_error_regexp):
      self.eval(M.strings.format(fmt, a=1))

  @parameterized.parameters(
      (
          '{a:whooo}',
          1,
          'failed.*type INT32.*format.*whooo',
      ),
      (
          '{a:whooo}',
          'a',
          'unsupported.*whooo.*BYTES',
      ),
      (
          '{a:01d}',
          0.57,
          'failed.*type FLOAT.*format.*01d',
      ),
  )
  def testFormatSpecUnsupportedForType(self, fmt, arg, expected_error_regexp):
    with self.assertRaisesRegex(ValueError, expected_error_regexp):
      self.eval(M.strings.format(fmt, a=arg))


if __name__ == '__main__':
  absltest.main()
