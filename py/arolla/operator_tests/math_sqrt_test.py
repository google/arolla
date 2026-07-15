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

"""Tests for M.math.sqrt operator."""

import contextlib

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils
import numpy as np

NAN = float("nan")
INF = float("inf")

M = arolla.M


def gen_test_data():
  """Returns test data for math.sqrt operator."""
  values = (None, NAN, -INF, INF, -0.0, -1., 0, 1, 1.5, 2, 10.5, 11)
  result = []
  for arg in values:
    with np.errstate(invalid="ignore"):
      result.append((
          arg,
          None if arg is None else np.sqrt(float(arg)),
      ))
  return tuple(result)


def gen_qtype_signatures():
  """Yields qtype signatures for math.sqrt.

  Yields: (arg_qtype, result_qtype)
  """
  for arg_qtype in pointwise_test_utils.lift_qtypes(
      *arolla.types.NUMERIC_QTYPES
  ):
    with contextlib.suppress(arolla.types.QTypeError):
      yield arg_qtype, arolla.types.common_float_qtype(arg_qtype)


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class MathSqrtQTypeTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(M.math.sqrt, QTYPE_SIGNATURES)


class MathSqrtEvalTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def test_eval(self, arg, expected_result):
    result = self.eval(M.math.sqrt(arg))
    arolla.testing.assert_qvalue_allequal(result, expected_result)


if __name__ == "__main__":
  absltest.main()
