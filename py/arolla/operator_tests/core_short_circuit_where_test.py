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

"""Tests for core._short_circuit_where."""

import contextlib
import itertools
import traceback

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M
L = arolla.L
P = arolla.P


def gen_qtype_signatures():
  condition_qtypes = (arolla.UNIT, arolla.OPTIONAL_UNIT)
  value_qtypes = pointwise_test_utils.lift_qtypes(*arolla.types.SCALAR_QTYPES)
  for arg_1, arg_2, arg_3 in itertools.product(
      condition_qtypes, value_qtypes, value_qtypes
  ):
    with contextlib.suppress(arolla.types.QTypeError):
      yield (arg_1, arg_2, arg_3, arolla.types.common_qtype(arg_2, arg_3))


QTYPE_SIGNATURES = frozenset(gen_qtype_signatures())


class CoreShortCircuitWhereTest(parameterized.TestCase):

  # Note: The behavior is tested in
  #   arolla/expr/eval/compile_where_operator_test.cc
  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(M.core._short_circuit_where),
        QTYPE_SIGNATURES,
    )

  def test_stack_trace(self):

    @arolla.optools.as_lambda_operator('failing_lambda')
    def failing_lambda(x):
      return M.core.with_assertion(
          x, arolla.optional_unit(None), 'another-error'
      )

    @arolla.optools.as_lambda_operator('my_where')
    def my_where(cond, x):
      return M.core._short_circuit_where(
          cond, failing_lambda(x), arolla.int32(2)
      )

    @arolla.optools.as_lambda_operator('calling_lambda')
    def calling_lambda(cond, x):
      return my_where(cond, x)

    expr = calling_lambda(L.cond, L.x)
    try:
      arolla.abc.eval_expr(
          expr,
          {'enable_expr_stack_trace': True},
          cond=arolla.optional_unit(True),
          x=arolla.int32(1),
      )
    except ValueError as e:
      ex = e
    else:
      self.fail('Expected ValueError from assertion')

    self.assertEqual(str(ex), '[FAILED_PRECONDITION] another-error')
    tb = '\n'.join(traceback.format_tb(ex.__traceback__))
    self.assertIn('calling_lambda', tb)
    self.assertIn('my_where', tb)
    self.assertIn('failing_lambda', tb)
    self.assertIn('M.core.with_assertion', tb)


if __name__ == '__main__':
  absltest.main()
