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

"""Tests for M.core.less_equal operator."""

import contextlib
import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

NAN = float("nan")

M = arolla.M


def gen_test_data():
  """Yields test data for core.less_equal operator.

  Yields: (arg_1, arg_2, result)
  """
  values = (None, False, True, b"", b"foo", "", "bar", 0, 1, 1.5, NAN)
  for arg_1, arg_2 in itertools.product(values, repeat=2):
    with contextlib.suppress(TypeError):
      yield (
          arg_1,
          arg_2,
          (
              True
              if arg_1 is not None and arg_2 is not None and arg_1 <= arg_2
              else None
          ),
      )
  yield (arolla.unit(), arolla.unit(), arolla.unit())
  yield (None, arolla.unit(), None)
  yield (arolla.unit(), None, None)


def gen_qtype_signatures():
  """Yields qtype signatures for core.less_equal.

  Yields: (arg_1_qtype, arg_2_qtype, result_qtype)
  """
  arg_qtypes = pointwise_test_utils.lift_qtypes(*arolla.types.ORDERED_QTYPES)
  for arg_1, arg_2 in itertools.product(arg_qtypes, repeat=2):
    with contextlib.suppress(arolla.types.QTypeError):
      yield (
          arg_1,
          arg_2,
          arolla.types.broadcast_qtype(
              [arolla.types.common_qtype(arg_1, arg_2)], arolla.OPTIONAL_UNIT
          ),
      )


def is_numeric(qtype):
  return arolla.types.get_scalar_qtype(qtype) in arolla.types.NUMERIC_QTYPES + (
      arolla.types.UINT64,
  )


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = tuple(gen_qtype_signatures())
TEST_CASES = tuple(pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES))
# We split the tests into numeric and non-numeric cases since some backends (TF)
# have limited support.
NUMERIC_TEST_CASES = tuple(
    args for args in TEST_CASES if is_numeric(args[0].qtype)
)
NON_NUMERIC_TEST_CASES = tuple(
    args for args in TEST_CASES if not is_numeric(args[0].qtype)
)


class CoreLessEqualTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.core.less_equal, QTYPE_SIGNATURES)

  @parameterized.parameters(*NUMERIC_TEST_CASES)
  def teset_numeric_values(self, lhs, rhs, expected_value):
    actual_value = self.eval(M.core.less_equal(lhs, rhs))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)

  @parameterized.parameters(*NON_NUMERIC_TEST_CASES)
  def test_non_numeric_values(self, lhs, rhs, expected_value):
    actual_value = self.eval(M.core.less_equal(lhs, rhs))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == "__main__":
  absltest.main()
