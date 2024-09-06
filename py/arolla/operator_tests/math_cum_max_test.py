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

"""Tests for M.math.cum_max operator."""

import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base

NAN = float("nan")
INF = float("inf")

M = arolla.M


def gen_test_data():
  """Yields test data for math.add operator.

  Yields: (arg_1, arg_2, result)
  """
  values = [None, 1, 3, None, None, -1]

  # Array-to-array edge.
  group_sizes = [3, 2, 0, 1]
  array_to_array_res = [None, 1, 3, None, None, -1]
  # Array-to-scalar edge.
  array_to_scalar_res = [None, 1, 3, None, None, 3]

  for array_mod, value_qtype in itertools.product(
      (arolla.dense_array, arolla.array),
      arolla.types.NUMERIC_QTYPES,
  ):
    x = array_mod(values, value_qtype=value_qtype)
    # Array-to-array edge.
    over = arolla.eval(M.edge.from_sizes(array_mod(group_sizes)))
    res = array_mod(array_to_array_res, value_qtype=value_qtype)
    yield x, over, res
    # Array-to-scalar edge.
    over = arolla.eval(M.edge.from_shape(M.core.shape_of(x)))
    res = array_mod(array_to_scalar_res, value_qtype=value_qtype)
    yield x, over, res


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = frozenset(
    (arg_1.qtype, arg_2.qtype, res.qtype) for arg_1, arg_2, res in TEST_DATA
)


class MathCumMaxTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.math.cum_max, QTYPE_SIGNATURES)

  @parameterized.parameters(*TEST_DATA)
  def testValue(self, arg1, arg2, expected_result):
    result = self.eval(M.math.cum_max(arg1, arg2))
    arolla.testing.assert_qvalue_allequal(result, expected_result)

  def testFloatNanAndInf(self):
    # Tests that NAN and INFs are handled correctly. This is tricky to handle
    # for some backends and is therefore tested explicitly.
    x = arolla.array([None, 1.0, NAN, 2.0, 3.0, INF, 4.0, 5.0, INF, -INF, 6.0])
    over = arolla.types.ArrayEdge.from_sizes([4, 3, 4])
    expected = arolla.array(
        [None, 1.0, NAN, NAN, 3.0, INF, INF, 5.0, INF, INF, INF]
    )
    res = self.eval(M.math.cum_max(x, over))
    arolla.testing.assert_qvalue_allequal(res, expected)


if __name__ == "__main__":
  absltest.main()
