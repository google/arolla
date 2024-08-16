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

"""Tests for math.softmax."""

import itertools
from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import utils
import numpy as np

M = arolla.M


def gen_test_data():
  values = [-1, 0, 1, None, 1, 3, None, None, -1]

  # Array-to-array edge.
  group_sizes = [3, 3, 2, 0, 1]
  array_to_array_res = (
      list(np.exp([-1, 0, 1]) / np.sum(np.exp([-1, 0, 1])))
      + [None]
      + list(np.exp([1, 3]) / np.sum(np.exp([1, 3])))
      + [None, None, 1]
  )
  # Array-to-scalar edge.
  denominator = np.sum(np.exp([-1, 0, 1, 1, 3, -1]))
  array_to_scalar_res = [
      np.exp(x) / denominator if x is not None else None for x in values
  ]

  for array_mod, value_qtype in itertools.product(
      (arolla.dense_array, arolla.array),
      arolla.types.NUMERIC_QTYPES,
  ):
    for beta_qtype in arolla.types.NUMERIC_QTYPES:
      x = array_mod(values, value_qtype=value_qtype)
      # Array-to-array edge.
      over = arolla.eval(M.edge.from_sizes(array_mod(group_sizes)))
      beta = arolla.eval(M.core.cast(1.0, beta_qtype))
      common_qtype = arolla.types.common_float_qtype(value_qtype, beta_qtype)
      res = array_mod(array_to_array_res, value_qtype=common_qtype)
      yield x, beta, over, res
      # Array-to-scalar edge.
      over = arolla.eval(M.edge.from_shape(M.core.shape_of(x)))
      res = array_mod(array_to_scalar_res, value_qtype=common_qtype)
      yield x, beta, over, res
      yield x, beta, arolla.unspecified(), res


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = frozenset(
    [
        (x.qtype, beta.qtype, over.qtype, res.qtype)
        for x, beta, over, res in TEST_DATA
    ]
    + [(x.qtype, beta.qtype, res.qtype) for x, beta, _, res in TEST_DATA]
    + [
        (x.qtype, res.qtype)
        for x, beta, _, res in TEST_DATA
        if beta.qtype == arolla.WEAK_FLOAT
    ]
)


class MathSoftmaxQTypeTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.math.softmax, QTYPE_SIGNATURES)

  @parameterized.parameters(*TEST_DATA)
  def testValue(self, x, beta, over, expected_result):
    result = self.eval(M.math.softmax(x, beta=beta, over=over))
    arolla.testing.assert_qvalue_allclose(result, expected_result)

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testBeta(self, array_factory):
    values = array_factory(
        [-1, 0, 1, None, 1, 3, None, None, -1], arolla.FLOAT32
    )
    over = arolla.eval(M.edge.from_shape(M.core.shape_of(values)))
    arolla.testing.assert_qvalue_allclose(
        self.eval(M.math.softmax(values, beta=0.5, over=over)),
        self.eval(M.math.softmax(values * 0.5, over=over)),
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testNumericalStability(self, array_factory):
    values = array_factory([100000, 1, 0.5, 0.3], arolla.FLOAT64)
    over = arolla.eval(M.edge.from_sizes(array_factory([2, 2])))
    arolla.testing.assert_qvalue_allclose(
        self.eval(M.math.softmax(values, over=over)),
        array_factory(
            [1, 0, 0.549833997312478, 0.4501660026875221], arolla.FLOAT64
        ),
    )


if __name__ == "__main__":
  absltest.main()
