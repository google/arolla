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

from absl.testing import absltest
from arolla.expr import expr as arolla_expr
from arolla.optools import clib
from arolla.testing import testing as arolla_testing
from arolla.types import types as _

M = arolla_expr.OperatorsContainer()
P = arolla_expr.PlaceholderContainer()


class ClibTest(absltest.TestCase):

  def test_internal_run_and_record_expr_source_locations_basics(self):
    def fn():
      return M.core.presence_or(M.core.presence_and(P.x, P.y), P.z)

    sink = {}
    res = clib.internal_run_and_record_expr_source_locations(sink, fn)

    expected_expr = M.core.presence_or(M.core.presence_and(P.x, P.y), P.z)
    arolla_testing.assert_expr_equal_by_fingerprint(res, expected_expr)

    self.assertIn(P.x.fingerprint, sink)
    self.assertIn(M.core.presence_and(P.x, P.y).fingerprint, sink)
    self.assertIn(expected_expr.fingerprint, sink)

  def test_internal_run_and_record_expr_source_locations_nested(self):
    inner_sink = {}
    outer_sink = {}

    def outer_fn():
      expr_and = M.core.presence_and(P.x, P.y)

      def inner_fn():
        return M.core.presence_or(expr_and, P.z)

      res_inner = clib.internal_run_and_record_expr_source_locations(
          inner_sink, inner_fn
      )

      return M.core.equal(res_inner, P.x)

    res_outer = clib.internal_run_and_record_expr_source_locations(
        outer_sink, outer_fn
    )

    expr_and = M.core.presence_and(P.x, P.y)
    expr_or = M.core.presence_or(expr_and, P.z)
    expr_eq = M.core.equal(expr_or, P.x)
    arolla_testing.assert_expr_equal_by_fingerprint(res_outer, expr_eq)

    self.assertIn(P.x.fingerprint, outer_sink)
    self.assertIn(P.y.fingerprint, outer_sink)
    self.assertIn(expr_and.fingerprint, outer_sink)
    self.assertIn(expr_eq.fingerprint, outer_sink)
    self.assertNotIn(expr_or.fingerprint, outer_sink)

    self.assertIn(P.z.fingerprint, inner_sink)
    self.assertIn(expr_or.fingerprint, inner_sink)
    self.assertNotIn(P.x.fingerprint, inner_sink)
    self.assertNotIn(P.y.fingerprint, inner_sink)
    self.assertNotIn(expr_and.fingerprint, inner_sink)
    self.assertNotIn(expr_eq.fingerprint, inner_sink)

  def test_resolve_source_location(self):
    # yapf: disable
# 23456789012345 -- column numbers # line numbers
    def fn():                      # 0
      return M.core.presence_or(   # 1
          P.x,                     # 2
          P.y                      # 3
      )                            # 4
    # yapf: enable

    sink = {}
    res = clib.internal_run_and_record_expr_source_locations(sink, fn)

    self.assertIn(res.fingerprint, sink)
    lasti, code = sink[res.fingerprint]
    start_line, start_col, end_line, end_col = clib.resolve_source_location(
        code, lasti
    )

    self.assertEqual(start_line, fn.__code__.co_firstlineno + 1)
    self.assertEqual(start_col, 13)
    self.assertEqual(end_line, fn.__code__.co_firstlineno + 4)
    self.assertEqual(end_col, 7)

    # Test out of bounds lasti.
    with self.assertRaisesRegex(ValueError, 'lasti out of range'):
      clib.resolve_source_location(code, -1)
    with self.assertRaisesRegex(ValueError, 'lasti out of range'):
      clib.resolve_source_location(code, len(code.co_code))
    with self.assertRaisesRegex(ValueError, 'lasti out of range'):
      clib.resolve_source_location(code, len(code.co_code) + 10)


if __name__ == '__main__':
  absltest.main()
