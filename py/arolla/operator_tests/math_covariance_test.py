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

"""Tests for math.covariance operator."""

import itertools
from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils
from arolla.operator_tests import utils

M = arolla.M


def gen_qtype_signatures():
  unspecified = arolla.UNSPECIFIED
  bool_ = arolla.BOOLEAN
  for array_qtype_fn in pointwise_test_utils.ARRAY_QTYPE_MUTATORS:
    for x_scalar_qtype in arolla.types.NUMERIC_QTYPES:
      for y_scalar_qtype in arolla.types.NUMERIC_QTYPES:
        output_scalar_qtype = arolla.types.common_float_qtype(
            x_scalar_qtype, y_scalar_qtype
        )
        x_qtype = array_qtype_fn(x_scalar_qtype)
        y_qtype = array_qtype_fn(y_scalar_qtype)
        scalar_edge_qtype = arolla.abc.infer_attr(
            M.edge.to_scalar, (x_qtype,)
        ).qtype
        edge_qtype = arolla.abc.infer_attr(M.edge.to_single, (x_qtype,)).qtype
        output_optional_qtype = arolla.make_optional_qtype(output_scalar_qtype)
        output_array_qtype = array_qtype_fn(output_scalar_qtype)
        yield from (
            # 2 args
            (x_qtype, y_qtype, output_optional_qtype),
            # 3 args
            (x_qtype, y_qtype, unspecified, output_optional_qtype),
            (x_qtype, y_qtype, scalar_edge_qtype, output_optional_qtype),
            (x_qtype, y_qtype, edge_qtype, output_array_qtype),
            # 4 args
            (x_qtype, y_qtype, unspecified, bool_, output_optional_qtype),
            (x_qtype, y_qtype, scalar_edge_qtype, bool_, output_optional_qtype),
            (x_qtype, y_qtype, edge_qtype, bool_, output_array_qtype),
        )


class MathCovarianceQTypeSignatureTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        M.math.covariance, gen_qtype_signatures()
    )


class MathCovarianceEvalTest(parameterized.TestCase):

  @parameterized.parameters(
      itertools.product(
          (arolla.array, arolla.dense_array),
          arolla.types.FLOATING_POINT_QTYPES,
          (
              (  # empty
                  [],
                  [],
                  None,
              ),
              (  # one element
                  [1],
                  [2],
                  None,
              ),
              (  # two element
                  [1, 2],
                  [2, 2],
                  0,
              ),
              (  # small all present
                  [1, 2, 3],
                  [2, 4, 6],
                  2,
              ),
              (  # small with same missing elements
                  [1, None, 2],
                  [2, None, 4],
                  1,
              ),
              (  # small with different missing elements
                  [None, None, 2],
                  [2, None, None],
                  None,
              ),
              (  # more elements
                  [1, None, 3, 4, 5, 6],
                  [2, 4, None, 8, 10, 12],
                  28 / 3,
              ),
          ),
      ),
  )
  def test_unbiased(self, array_fn, input_qtype, test_data):
    x = array_fn(test_data[0], input_qtype)
    y = array_fn(test_data[1], input_qtype)
    result_qtype = arolla.types.common_qtype(input_qtype)
    expected = test_data[2]
    with self.subTest("agg_to_scalar"):
      result = arolla.eval(M.math.covariance(x, y))
      arolla.testing.assert_qvalue_allclose(
          result, utils.optional(expected, result_qtype)
      )
    with self.subTest("agg_to_single"):
      result = arolla.eval(M.math.covariance(x, y, into=M.edge.to_single(x)))
      arolla.testing.assert_qvalue_allclose(
          result, array_fn([expected], result_qtype)
      )

  @parameterized.parameters(
      itertools.product(
          (arolla.array, arolla.dense_array),
          arolla.types.FLOATING_POINT_QTYPES,
          (
              (  # empty
                  [],
                  [],
                  None,
              ),
              (  # one element
                  [1],
                  [2],
                  0,
              ),
              (  # small all present
                  [1, 2, 3],
                  [2, 4, 6],
                  4 / 3,
              ),
              (  # small with same missing elements
                  [1, None, 2],
                  [2, None, 4],
                  1 / 2,
              ),
              (  # small with different missing elements
                  [None, None, 2],
                  [2, None, None],
                  None,
              ),
              (  # more elements
                  [1, None, 3, 4, 5, 6],
                  [2, 4, None, 8, 10, 12],
                  7,
              ),
          ),
      ),
  )
  def test_biased(self, array_fn, input_qtype, test_data):
    x = array_fn(test_data[0], input_qtype)
    y = array_fn(test_data[1], input_qtype)
    result_qtype = arolla.types.common_float_qtype(input_qtype)
    expected = test_data[2]
    with self.subTest("agg_to_scalar"):
      result = arolla.eval(M.math.covariance(x, y, unbiased=False))
      arolla.testing.assert_qvalue_allclose(
          result, utils.optional(expected, result_qtype)
      )
    with self.subTest("agg_to_single"):
      result = arolla.eval(
          M.math.covariance(x, y, into=M.edge.to_single(x), unbiased=False)
      )
      arolla.testing.assert_qvalue_allclose(
          result, array_fn([expected], result_qtype)
      )

  @parameterized.parameters(
      itertools.product(
          (arolla.array, arolla.dense_array),
          arolla.types.FLOATING_POINT_QTYPES,
      )
  )
  def test_multi_group(self, array_fn, input_qtype):
    x = array_fn([1, 2, 3, 10, 20, 30], input_qtype)
    y = array_fn([2, 4, 6, 20, 40, 60], input_qtype)
    result_qtype = arolla.types.common_float_qtype(input_qtype)
    edge = M.edge.from_split_points(array_fn([0, 3, 6]))
    result = arolla.eval(M.math.covariance(x, y, into=edge, unbiased=False))
    arolla.testing.assert_qvalue_allclose(
        result, array_fn([4 / 3, 400 / 3], result_qtype)
    )


if __name__ == "__main__":
  absltest.main()
