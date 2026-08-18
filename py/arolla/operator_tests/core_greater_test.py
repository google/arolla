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

"""Tests for M.core.greater operator."""

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
  """Yields test data for core.greater operator.

  Yields: (lhs, rhs, result)
  """
  for lhs, rhs in itertools.product(
      (None, False, True, 0, 1, 1.5, NAN), repeat=2
  ):
    yield (lhs, rhs, None if lhs is None or rhs is None else lhs > rhs)
  for lhs, rhs in itertools.product((None, b"", b"foo"), repeat=2):
    yield (lhs, rhs, None if lhs is None or rhs is None else lhs > rhs)
  for lhs, rhs in itertools.product((None, "", "bar"), repeat=2):
    yield (lhs, rhs, None if lhs is None or rhs is None else lhs > rhs)
  yield (arolla.unit(), arolla.unit(), None)
  yield (None, arolla.unit(), None)
  yield (arolla.unit(), None, None)


def gen_qtype_signatures():
  """Yields qtype signatures for core.greater.

  Yields: (lhs_qtype, rhs_qtype, result_qtype)
  """
  qtypes = pointwise_test_utils.lift_qtypes(*arolla.types.ORDERED_QTYPES)
  for arg_qtypes in itertools.product(qtypes, repeat=2):
    with contextlib.suppress(arolla.types.QTypeError):
      yield (
          *arg_qtypes,
          arolla.types.broadcast_qtype(
              [arolla.types.common_qtype(*arg_qtypes)], arolla.OPTIONAL_UNIT
          ),
      )


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class CoreGreaterTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.core.greater, QTYPE_SIGNATURES)

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def test_eval(self, lhs, rhs, expected_value):
    actual_value = self.eval(M.core.greater(lhs, rhs))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == "__main__":
  absltest.main()
