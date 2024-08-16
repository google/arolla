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

"""Tests for M.math.sigmoid operator."""

import contextlib
import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils
import numpy
import scipy.special

NAN = float("nan")
INF = float("inf")

M = arolla.M


def py_sigmoid(x, half, slope):
  if None in (x, half, slope):
    return None
  x = numpy.float128(x)
  half = numpy.float128(half)
  slope = numpy.float128(slope)
  return float(scipy.special.expit(slope * (x - half)))


def gen_test_data():
  """Yields scalar test data for math.sigmoid operator.

  Yields: (x, half, slope, result)
  """
  data = (None, -INF, INF, NAN, -1, 0, 1)
  for x, half, slope in itertools.product(data, repeat=3):
    yield x, half, slope, py_sigmoid(x, half, slope)


def gen_qtype_signatures():
  """Yields qtype signatures for math.sigmoid.

  Yields: (x_qtype, half_qtype, slope_qtype, result_qtype) or
          (x_qtype, half_qtype, result_qtype) or
          (x_qtype, result_qtype).
  """
  qtypes = pointwise_test_utils.lift_qtypes(*arolla.types.NUMERIC_QTYPES)
  for args in itertools.chain(
      itertools.product(qtypes, repeat=1),
      itertools.product(qtypes, repeat=2),
  ):
    with contextlib.suppress(arolla.types.QTypeError):
      # TODO: Should default arguments be WEAK_FLOAT?
      yield (*args, arolla.types.common_qtype(*args, arolla.FLOAT32))
  for args in itertools.product(qtypes, repeat=3):
    with contextlib.suppress(arolla.types.QTypeError):
      yield (*args, arolla.types.common_float_qtype(*args))


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class MathSigmoidTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.math.sigmoid, QTYPE_SIGNATURES)

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(
          TEST_DATA, *filter(lambda q: len(q) == 4, QTYPE_SIGNATURES)
      )
  )
  def testAllValues(self, x, half, slope, expected_value):
    actual_value = self.eval(M.math.sigmoid(x, half=half, slope=slope))
    arolla.testing.assert_qvalue_allclose(actual_value, expected_value)

  def testDefaults(self):
    # Tests that the defaults are set to half=0.0, slope=1.0.
    x = arolla.dense_array_float32([-1.0, 0.0, 1.0, 2.0])
    actual_value = self.eval(M.math.sigmoid(x))
    arolla.testing.assert_qvalue_allclose(
        actual_value,
        arolla.dense_array_float32([0.26894143, 0.5, 0.7310586, 0.880797]),
    )


if __name__ == "__main__":
  absltest.main()
