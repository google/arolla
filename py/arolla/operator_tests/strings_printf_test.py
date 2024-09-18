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

"""Tests for M.strings.printf operator."""

import contextlib
import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

M = arolla.M


def gen_signatures(max_arity):
  for scalar_fmt in (arolla.BYTES, arolla.TEXT):
    for fmt in pointwise_test_utils.lift_qtypes(scalar_fmt):
      yield (fmt, fmt)

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

      for num_format_args in range(1, max_arity):
        for args in itertools.product(arg_types, repeat=num_format_args):
          with contextlib.suppress(arolla.types.QTypeError):
            yield (fmt, *args, arolla.types.broadcast_qtype(args, fmt))


class StringsPrintfTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def setUp(self):
    super().setUp()
    # Force self.assertCountEqual to always show the diff.
    self.maxDiff = None

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    self.assertCountEqual(
        gen_signatures(max_arity=3),
        pointwise_test_utils.detect_qtype_signatures(
            M.strings.printf, max_arity=3
        ),
    )

  @parameterized.parameters(
      *pointwise_test_utils.gen_cases(
          (
              (None, None),
              ('nothing to format is ok', 'nothing to format is ok'),
              ('missing value is %f', None, None),
              # Special cases for float formatting are tested separately.
              ('float value is %f', 5.75, 'float value is 5.750000'),
              (
                  'int value is %d',
                  pointwise_test_utils.IntOnly(57),
                  'int value is 57',
              ),
              ('boolean value is %d', True, 'boolean value is 1'),
              (
                  'text value is "%s"',
                  'fifty seven',
                  'text value is "fifty seven"',
              ),
              (b'nothing to format is ok', b'nothing to format is ok'),
              (b'missing value is %f', None, None),
              # Special cases for float formatting are tested separately.
              (b'float value is %f', 5.75, b'float value is 5.750000'),
              (
                  b'int value is %d',
                  pointwise_test_utils.IntOnly(57),
                  b'int value is 57',
              ),
              (b'boolean value is %d', True, b'boolean value is 1'),
              (
                  b'bytes value is "%s"',
                  b'fifty seven',
                  b'bytes value is "fifty seven"',
              ),
              # $ means an index of an argument.
              (b'%1$d is before %0$d', 1, 2, b'2 is before 1'),
          ),
          *gen_signatures(max_arity=2)
      )
  )
  def testValue(self, *args_and_expected):
    args = args_and_expected[:-1]
    expected = args_and_expected[-1]
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.strings.printf(*args)), expected
    )

  @parameterized.parameters(
      (
          'extra format arguments are ignored: %d',
          1,
          2,
          3,
          'extra format arguments are ignored: 1',
      ),
      (
          'non-ascii characters \u00E4re supp%srted',
          '\u00F6',
          'non-ascii characters äre suppörted',
      ),
      (
          b'invalid utf-8 BYTES \xFF are %s supported',
          b'n\xF0w',
          b'invalid utf-8 BYTES \xFF are n\xF0w supported',
      ),
      ('%f', -0, '0.000000'),
      ('%f', float('inf'), 'inf'),
      ('%f', -float('inf'), '-inf'),
      ('%f', float('NaN'), 'nan'),
      #
      # Extract from absl/strings/str_format_test.cc below:
      #
      ('%1$+3.2Lf', 1.1, '+1.10'),
      # Text conversion:
      #     'c' - Character.              Eg: 'a' -> 'A', 20 -> ' '
      ('%c', arolla.int32(ord('a')), 'a'),
      ('%c', arolla.int64(ord('a')), 'a'),
      #     's' - string       Eg: 'C' -> 'C', std::string('C++') -> 'C++'
      ('%s', 'C++', 'C++'),
      ('%v', 'C++', 'C++'),
      # Integral Conversion
      #     These format integral types: char, int, long, uint64_t, etc.
      ('%d', arolla.int32(10), '10'),
      ('%d', arolla.int64(10), '10'),
      ('%v', arolla.int64(10), '10'),
      ('%v', arolla.int64(10), '10'),
      #     d,i - signed decimal          Eg: -10 -> '-10'
      ('%d', -10, '-10'),
      ('%i', -10, '-10'),
      ('%v', -10, '-10'),
      #      o  - octal                   Eg:  10 -> '12'
      ('%o', 10, '12'),
      #      u  - unsigned decimal        Eg:  10 -> '10'
      ('%u', 10, '10'),
      ('%v', 10, '10'),
      #     x/X - lower,upper case hex    Eg:  10 -> 'a'/'A'
      ('%x', 10, 'a'),
      ('%X', 10, 'A'),
      # Floating-point, with upper/lower-case output.
      #     These format floating points types: float, double, long double, etc.
      ('%.1f', arolla.float32(1), '1.0'),
      ('%.1f', arolla.float64(1), '1.0'),
      #     These also format integral types: char, int, long, uint64_t, etc.:
      ('%.1f', arolla.int32(1), '1.0'),
      ('%.1f', arolla.int64(1), '1.0'),
      #     f/F - decimal.                Eg: 123456789 -> '123456789.000000'
      ('%f', 123456789, '123456789.000000'),
      ('%F', 123456789, '123456789.000000'),
      #     e/E - exponentiated           Eg: .01 -> '1.00000e-2'/'1.00000E-2'
      ('%e', 0.01, '1.000000e-02'),
      ('%E', 0.01, '1.000000E-02'),
      #     g/G - exponentiate to fit  Eg: .01 -> '0.01', 1e10 ->'1e+10'/'1E+10'
      ('%g', 0.01, '0.01'),
      ('%g', 1e10, '1e+10'),
      ('%G', 1e10, '1E+10'),
      ('%v', 0.01, '0.01'),
      ('%v', 1e10, '1e+10'),
      #     a/A - lower,upper case hex    Eg: -3.0 -> '-0x1.8p+1'/'-0X1.8P+1'
      ('%.1a', -3.0, '-0x1.8p+1'),  # .1 to fix MSVC output
      ('%.1A', -3.0, '-0X1.8P+1'),  # .1 to fix MSVC output
      # Output widths are supported, with optional flags.
      ('%3d', 1, '  1'),
      ('%3d', 123456, '123456'),
      ('%06.2f', 1.234, '001.23'),
      ('%+d', 1, '+1'),
      ('% d', 1, ' 1'),
      ('%-4d', -1, '-1  '),
      ('%#o', 10, '012'),
      ('%#x', 15, '0xf'),
      ('%04d', 8, '0008'),
      # Posix positional substitution.
      ('%2$s, %3$s, %1$s!', 'vici', 'veni', 'vidi', 'veni, vidi, vici!'),
      # Length modifiers are ignored.
      ('%hhd', 1, '1'),
      ('%hd', 1, '1'),
      ('%ld', 1, '1'),
      ('%lld', 1, '1'),
      ('%Ld', 1, '1'),
      ('%jd', 1, '1'),
      ('%zd', 1, '1'),
      ('%td', 1, '1'),
      ('%qd', 1, '1'),
      # Bool is handled correctly depending on whether %v is used
      ('%v', True, 'true'),
      ('%v', False, 'false'),
      ('%d', True, '1'),
  )
  def testSpecialCases(self, *args_and_expected):
    args = args_and_expected[:-1]
    expected = args_and_expected[-1]
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.strings.printf(*args)), arolla.as_qvalue(expected)
    )

  @parameterized.parameters(
      (
          '%c',
          'a',
          "format specification '%c' doesn't match format arguments",
      ),
      (
          '%p',
          arolla.int64(0x7FFDEB4),
          "format specification '%p' doesn't match format arguments",
      ),
      (
          '%d %d',
          57,
          "format specification '%d %d' doesn't match format arguments",
      ),
      (
          '%d %d',
          arolla.tuple(1, 2),
          (
              'all printf args must be broadcast compatible, got fmt: TEXT,'
              ' args: tuple<tuple<INT32,INT32>>'
          ),
      ),
      (
          b'%s',
          'foo',
          # NOTE: Ideally we would givea better error message here, but it is
          # extra 30-50 lines of code that duplicate qtype_constraints of the
          # underlying operators. Maybe it's better to forward error messages
          # from them instead.
          'unsupported argument types \\(BYTES,TEXT\\)',
      ),
      (
          '%s',
          b'bar',
          # NOTE: Ideally we would givea better error message here, but it is
          # extra 30-50 lines of code that duplicate qtype_constraints of the
          # underlying operators. Maybe it's better to forward error messages
          # from them instead.
          'unsupported argument types \\(TEXT,BYTES\\)',
      ),
  )
  def testErrors(self, *args_and_expected_error):
    self.require_self_eval_is_called = False  # Not all errors happen at eval.
    args = args_and_expected_error[:-1]
    expected_error_regexp = args_and_expected_error[-1]
    with self.assertRaisesRegex(ValueError, expected_error_regexp):
      self.eval(M.strings.printf(*args))


if __name__ == '__main__':
  absltest.main()
