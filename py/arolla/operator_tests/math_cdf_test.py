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

"""Tests for math_cdf."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import utils


M = arolla.M


def gen_qtype_signatures():
  for x_type in arolla.types.NUMERIC_QTYPES:
    for array in (arolla.make_dense_array_qtype, arolla.make_array_qtype):
      if x_type == arolla.FLOAT64:
        res_type = arolla.FLOAT64
      else:
        res_type = arolla.FLOAT32
      for weights_type in [array(t) for t in arolla.types.NUMERIC_QTYPES] + [
          arolla.UNSPECIFIED
      ]:
        for edge_type in [
            arolla.eval(M.qtype.get_edge_qtype(array(x_type))),
            arolla.eval(M.qtype.get_edge_to_scalar_qtype(array(x_type))),
            arolla.UNSPECIFIED,
        ]:
          yield (
              array(x_type),
              weights_type,
              edge_type,
              array(res_type),
          )
          yield (
              array(x_type),
              weights_type,
              array(res_type),
          )
          yield (
              array(x_type),
              array(res_type),
          )


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class MathCDFTest(parameterized.TestCase):

  def testQTypeSignatures(self):
    arolla.testing.assert_qtype_signatures(M.math.cdf, QTYPE_SIGNATURES)

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_math_cdf_float(self, array_factory):
    values = array_factory([1, None, 3, 4], arolla.FLOAT32)
    edge = M.edge.from_sizes(array_factory([2, 2]))
    expected = array_factory([1, None, 0.5, 1], arolla.FLOAT32)

    arolla.testing.assert_qvalue_allequal(
        arolla.eval(M.math.cdf(values, over=edge)), expected
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_math_cdf_default_edge(self, array_factory):
    values = array_factory([1, None, 3, 4])
    expected = array_factory([1 / 3, None, 2 / 3, 1], arolla.FLOAT32)

    arolla.testing.assert_qvalue_allequal(
        arolla.eval(M.math.cdf(values)), expected
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_math_cdf_double(self, array_factory):
    values = array_factory([1, None, 3, 4], arolla.FLOAT64)
    expected = array_factory([1 / 3, None, 2 / 3, 1.0], arolla.FLOAT64)

    arolla.testing.assert_qvalue_allequal(
        arolla.eval(M.math.cdf(values)), expected
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_math_cdf_int(self, array_factory):
    values = array_factory([1, None, 3, 4], arolla.INT32)
    expected = array_factory([1 / 3, None, 2 / 3, 1.0], arolla.FLOAT32)

    arolla.testing.assert_qvalue_allequal(
        arolla.eval(M.math.cdf(values)), expected
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_math_cdf_with_weights(self, array_factory):
    arolla.testing.assert_qvalue_allclose(
        arolla.eval(
            M.math.cdf(
                array_factory([2, 3, 5], arolla.FLOAT32),
                array_factory([1, 2, 3], arolla.FLOAT32),
            )
        ),
        array_factory([0.16666667, 0.5, 1.0], arolla.FLOAT32),
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_math_cdf_equal_values_with_weights(self, array_factory):
    arolla.testing.assert_qvalue_allclose(
        arolla.eval(
            M.math.cdf(
                array_factory([2, 2, 3, 5], arolla.FLOAT32),
                array_factory([1, 2, 2, 3], arolla.FLOAT32),
            )
        ),
        array_factory([0.375, 0.375, 0.625, 1.0], arolla.FLOAT32),
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_math_cdf_with_weights_and_edge(self, array_factory):
    arolla.testing.assert_qvalue_allclose(
        arolla.eval(
            M.math.cdf(
                array_factory([2, 2, 3, 5], arolla.FLOAT32),
                array_factory([1, 2, 2, 3], arolla.FLOAT32),
                over=M.edge.from_sizes(array_factory([2, 2])),
            )
        ),
        array_factory([1.0, 1.0, 0.4, 1.0], arolla.FLOAT32),
    )


if __name__ == '__main__':
  absltest.main()
