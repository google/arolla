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

"""Tests for M.core.to_float32 operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

NAN = float("nan")
INF = float("inf")

M = arolla.M

# Test data: tuple(
#     (input, expected_output),
#     ...
# )
TEST_DATA = (
    (None, None),
    (False, 0.0),
    (True, 1.0),
    (0, 0.0),
    (1, 1.0),
    (123456789, 123456790.0),
    (1.5, 1.5),
    (-1.5, -1.5),
    (1e200, INF),
    (-INF, -INF),
    (INF, INF),
    (NAN, NAN),
)


def gen_qtype_signatures():
  """Yields qtype signatures for core.to_float32.

  Yields: (arg_qtype, result_qtype)
  """
  for arg in (
      arolla.BOOLEAN,
      arolla.types.UINT64,
  ) + arolla.types.NUMERIC_QTYPES:
    yield from pointwise_test_utils.lift_qtypes((arg, arolla.FLOAT32))


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class CoreToFloat32Test(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.core.to_float32, QTYPE_SIGNATURES)

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def test_eval(self, arg, expected_value):
    actual_value = self.eval(M.core.to_float32(arg))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)

  @parameterized.parameters(*pointwise_test_utils.lift_qtypes(arolla.FLOAT32))
  def test_optimise_out_in_lowering(self, qtype):
    self.require_self_eval_is_called = False
    x = M.annotation.qtype(arolla.L.x, qtype)
    arolla.testing.assert_expr_equal_by_fingerprint(
        arolla.abc.to_lowest(M.core.to_float32(x)), x
    )


if __name__ == "__main__":
  absltest.main()
