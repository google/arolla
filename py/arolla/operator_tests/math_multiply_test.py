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

"""Tests for M.math.multiply operator."""

import contextlib
import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

NAN = float("nan")
INF = float("inf")

M = arolla.M


def gen_test_data():
  """Yields test data for math.multiply operator.

  Yields: (arg_1, arg_2, result)
  """
  values = (None, NAN, -INF, INF, -0.0, 0, 1, 1.5)
  for arg_1, arg_2 in itertools.product(values, repeat=2):
    yield (
        arg_1,
        arg_2,
        None if arg_1 is None or arg_2 is None else arg_1 * arg_2,
    )
  # Test data for integer overflow.
  yield from (
      (
          arolla.int32(2**30 - 1),
          arolla.int32(2**30 - 1),
          arolla.int32(-(2**31) + 1),
      ),
      (
          arolla.int64(2**62 - 1),
          arolla.int64(2**62 - 1),
          arolla.int64(-(2**63) + 1),
      ),
      (arolla.int32(2**31 - 1), arolla.int32(2**31 - 1), arolla.int32(1)),
      (arolla.int64(2**63 - 1), arolla.int64(2**63 - 1), arolla.int64(1)),
  )


def gen_qtype_signatures():
  """Yields qtype signatures for math.multiply.

  Yields: (arg_1_qtype, arg_2_qtype, result_qtype)
  """
  qtypes = pointwise_test_utils.lift_qtypes(*arolla.types.NUMERIC_QTYPES)
  for arg_1, arg_2 in itertools.product(qtypes, repeat=2):
    with contextlib.suppress(arolla.types.QTypeError):
      yield (arg_1, arg_2, arolla.types.common_qtype(arg_1, arg_2))


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class MathMultiplyTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.math.multiply, QTYPE_SIGNATURES)

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def testValue(self, arg_1, arg_2, expected_result):
    result = self.eval(M.math.multiply(arg_1, arg_2))
    arolla.testing.assert_qvalue_allclose(result, expected_result)


if __name__ == "__main__":
  absltest.main()
