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

"""Tests for M.core.cast operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

NAN = float('nan')
INF = float('inf')

M = arolla.M
P = arolla.P

IntOnly = pointwise_test_utils.IntOnly

# Exact limits of int32.
_min_value = -(2**31)
_max_value = 2**31 - 1

# Test data: tuple(
#     (arg, result),
#     ...
# )
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
    (_min_value, _min_value),
    (_max_value, _max_value),
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


@parameterized.parameters(
    *pointwise_test_utils.lift_qtypes(*arolla.types.SCALAR_QTYPES)
)
class CoreCastTest(parameterized.TestCase):

  @staticmethod
  def op(qtype):
    """Returns core.cast(..., qtype=qtype, implicit_only=False) operator."""
    return arolla.LambdaOperator(
        M.core.cast(P.arg, qtype=qtype, implicit_only=False)
    )

  @staticmethod
  def qtype_signatures(qtype):
    """Yields qtype signatures for self.op(qtype) operator.

    NOTE: core.cast(..., qtype=qtype, implicit_only=False)

    Args:
      qtype: A value for qtype parameter of core.cast operator.
    Yields: (arg_qtype, result_qtype)
    """
    scalar_qtype = arolla.types.get_scalar_qtype(qtype)
    if scalar_qtype in CAST_ARG_SCALAR_QTYPES:
      if arolla.is_optional_qtype(qtype):
        yield from ((arg_qtype, qtype) for arg_qtype in CAST_ARG_SCALAR_QTYPES)
      yield from (
          (arolla.types.broadcast_qtype([qtype], arg_qtype), qtype)
          for arg_qtype in CAST_ARG_SCALAR_QTYPES
      )
    else:
      if arolla.is_optional_qtype(qtype):
        yield (scalar_qtype, qtype)
      yield (qtype, qtype)

  def test_qtype_signatures(self, qtype):
    op = self.op(qtype)
    qtype_signatures = self.qtype_signatures(qtype)
    self.assertEqual(
        frozenset(qtype_signatures),
        frozenset(pointwise_test_utils.detect_qtype_signatures(op)),
    )

  def test_value(self, qtype):
    op = self.op(qtype)
    qtype_signatures = self.qtype_signatures(qtype)
    for arg_qvalue, expected_qvalue in pointwise_test_utils.gen_cases(
        TEST_DATA, *qtype_signatures
    ):
      with self.subTest(arg_qvalue=arg_qvalue, expected_qvalue=expected_qvalue):
        actual_qvalue = arolla.eval(op(arg_qvalue))
        arolla.testing.assert_qvalue_allequal(actual_qvalue, expected_qvalue)


if __name__ == '__main__':
  absltest.main()
