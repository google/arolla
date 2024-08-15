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

"""Tests for the M.core.const_with_shape operator."""

import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

M = arolla.M
L = arolla.L


def gen_test_data():
  """Yields test data for core.const_with_shape.

  Yields: (arg_1, arg_2, result)
  """
  to_optional = lambda x: arolla.eval(M.core.to_optional(x))
  data = (0, 1.0, None, 'a', b'a', True)
  # Generate scalar values from the data and the possible QTypes.
  scalar_data = pointwise_test_utils.gen_cases(
      tuple((v,) for v in data),
      *(
          (q,)
          for q in itertools.chain(
              arolla.types.SCALAR_QTYPES, arolla.types.OPTIONAL_QTYPES
          )
      ),
  )
  for (scalar,) in scalar_data:
    # Scalars.
    yield (arolla.types.ScalarShape(), scalar, scalar)
    yield (arolla.types.OptionalScalarShape(), scalar, to_optional(scalar))
    # Array types.
    for size in (0, 1, 5, 100):
      array_data = [scalar] * size
      yield (
          arolla.types.ArrayShape(size),
          scalar,
          arolla.array(array_data, value_qtype=scalar.qtype),
      )
      yield (
          arolla.types.DenseArrayShape(size),
          scalar,
          arolla.dense_array(array_data, value_qtype=scalar.qtype),
      )


def gen_to_lower_expressions_noop():
  """Yields expressions and the expected lowerings for core.const_with_shape.

  We only consider those cases where the lowering removes the operator
  completely.

  Yields: (expr, expected_lowering)
  """
  float_leaf = M.annotation.qtype(L.f, arolla.FLOAT32)
  yield (
      M.core.const_with_shape(arolla.types.ScalarShape(), float_leaf),
      float_leaf,
  )
  optional_float_leaf = M.annotation.qtype(L.f, arolla.OPTIONAL_FLOAT32)
  yield (
      M.core.const_with_shape(
          arolla.types.OptionalScalarShape(), optional_float_leaf
      ),
      optional_float_leaf,
  )


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = frozenset(
    (arg1.qtype, arg2.qtype, res.qtype) for arg1, arg2, res in TEST_DATA
)
TO_LOWER_EXPR = tuple(gen_to_lower_expressions_noop())


class CoreConstWithShapeTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    self.assertCountEqual(
        QTYPE_SIGNATURES,
        pointwise_test_utils.detect_qtype_signatures(M.core.const_with_shape),
    )

  @parameterized.parameters(*TEST_DATA)
  def testValue(self, arg1, arg2, expected_result):
    result = self.eval(M.core.const_with_shape(arg1, arg2))
    arolla.testing.assert_qvalue_allequal(result, expected_result)

  @parameterized.parameters(*TO_LOWER_EXPR)
  def testLowering(self, expr, expected_lowered_expr):
    self.require_self_eval_is_called = False
    lowered_expr = arolla.abc.to_lowest(expr)
    arolla.testing.assert_expr_equal_by_fingerprint(
        lowered_expr, expected_lowered_expr
    )


if __name__ == '__main__':
  absltest.main()
