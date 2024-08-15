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

"""Tests for M.core.cast_values operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

NAN = float('nan')
INF = float('inf')

M = arolla.M
P = arolla.P

IntOnly = pointwise_test_utils.IntOnly

TEST_DATA = (
    (None, None),
    (False, False),
    (True, True),
    (b'', b''),
    (b'bar', b'bar'),
    ('', ''),
    ('foo', 'foo'),
    (False, 0),
    (True, 1),
    (0, False),
    (1, True),
    (0, 0),
    (1, 1),
    (123456789, arolla.float32(123456790)),
    (123456789, 123456789),
    (-(2**31), -(2**31)),
    (2**31 - 1, 2**31 - 1),
    (1.1, IntOnly(1)),
    (-1.1, IntOnly(-1)),
    (1.9, IntOnly(1)),
    (-1.9, IntOnly(-1)),
    (1.5, 1.5),
    (-1.5, -1.5),
    (1e200, arolla.float32(INF)),
    (1e200, arolla.float64(1e200)),
    (-INF, -INF),
    (INF, INF),
    (NAN, NAN),
)

CAST_ARG_SCALAR_QTYPES = (
    arolla.BOOLEAN,
    arolla.types.UINT64,
) + arolla.types.NUMERIC_QTYPES


class CoreCastValuesTest(parameterized.TestCase):

  @staticmethod
  def op(qtype):
    """Returns core.cast_values(..., qtype) operator."""
    return arolla.LambdaOperator(M.core.cast_values(P.arg, qtype))

  @staticmethod
  def qtype_signatures(qtype):
    """Yields qtype signatures for self.op(qtype) operator.

    NOTE: core.cast_values(..., qtype)

    Args:
      qtype: A value for qtype parameter of core.cast_values operator.
    Yields: (arg_qtype, result_qtype)
    """
    if qtype in CAST_ARG_SCALAR_QTYPES:
      for input_qtype in CAST_ARG_SCALAR_QTYPES:
        yield from zip(
            pointwise_test_utils.lift_qtypes(input_qtype),
            pointwise_test_utils.lift_qtypes(qtype),
        )
    else:
      yield from ((t, t) for t in pointwise_test_utils.lift_qtypes(qtype))

  @parameterized.parameters(*arolla.types.SCALAR_QTYPES)
  def test_qtype_signatures(self, qtype):
    op = self.op(qtype)
    qtype_signatures = self.qtype_signatures(qtype)
    self.assertCountEqual(
        qtype_signatures,
        pointwise_test_utils.detect_qtype_signatures(op),
    )

  @parameterized.parameters(*arolla.types.SCALAR_QTYPES)
  def test_value(self, qtype):
    op = self.op(qtype)
    qtype_signatures = self.qtype_signatures(qtype)
    for arg_qvalue, expected_qvalue in pointwise_test_utils.gen_cases(
        TEST_DATA, *qtype_signatures
    ):
      with self.subTest(arg_qvalue=arg_qvalue, expected_qvalue=expected_qvalue):
        actual_qvalue = arolla.eval(op(arg_qvalue))
        arolla.testing.assert_qvalue_allequal(actual_qvalue, expected_qvalue)

  def testNonQTypeErrorMsg(self):
    self.require_self_eval_is_called = False
    with self.assertRaisesRegex(
        ValueError,
        'expected a qtype, got scalar_qtype: INT32',
    ):
      M.core.cast_values(arolla.array_float32([1.0, 2.0]), arolla.int32(1))

  def testNonScalarQTypeErrorMsg(self):
    self.require_self_eval_is_called = False
    with self.assertRaisesRegex(
        ValueError,
        'expected a scalar qtype, got scalar_qtype=ARRAY_FLOAT64',
    ):
      M.core.cast_values(
          arolla.array_float32([1.0, 2.0]), arolla.types.ARRAY_FLOAT64
      )

  def testInvalidTargetQTypeErrorMsg(self):
    self.require_self_eval_is_called = False
    with self.assertRaisesRegex(
        ValueError,
        'casting from FLOAT64 to Dict<TEXT,INT32> is not allowed',
    ):
      M.core.cast_values(arolla.types.Dict({'a': 1}), arolla.types.FLOAT64)

  def testIncompatibleQTypesErrorMsg(self):
    self.require_self_eval_is_called = False
    with self.assertRaisesRegex(
        ValueError,
        'casting from FLOAT64 to TEXT is not allowed',
    ):
      M.core.cast_values(arolla.text('foo'), arolla.types.FLOAT64)


if __name__ == '__main__':
  absltest.main()
