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

"""Tests for M.core.default_if_unspecified operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

L = arolla.L
M = arolla.M


class CoreDefaultIfUnspecifiedTest(parameterized.TestCase):

  @parameterized.parameters(
      M.core.default_if_unspecified(L.x, L.y),  # pyrefly: ignore[missing-attribute]
      M.core.default_if_unspecified(L.x, arolla.unspecified()),  # pyrefly: ignore[missing-attribute]
      M.core.default_if_unspecified(L.x, arolla.int32(1)),  # pyrefly: ignore[missing-attribute]
  )
  def test_unresolved(self, expr):
    self.assertIsNone(expr.qtype)
    arolla.testing.assert_expr_equal_by_fingerprint(
        arolla.abc.to_lowest(expr), expr
    )

  @parameterized.parameters(
      M.core.default_if_unspecified(  # pyrefly: ignore[missing-attribute]
          M.annotation.qtype(L.x, arolla.INT32),  # pyrefly: ignore[not-callable]
          L.y,
      ),
      M.core.default_if_unspecified(  # pyrefly: ignore[missing-attribute]
          M.annotation.qtype(L.x, arolla.INT32),  # pyrefly: ignore[not-callable]
          M.annotation.qtype(L.y, arolla.INT32),  # pyrefly: ignore[not-callable]
      ),
      M.core.default_if_unspecified(arolla.unit(), arolla.unspecified()),  # pyrefly: ignore[missing-attribute]
      M.core.default_if_unspecified(arolla.tuple(), arolla.unspecified()),  # pyrefly: ignore[missing-attribute]
  )
  def test_resolves_to_first(self, expr):
    first = expr.node_deps[0]
    self.assertEqual(expr.qtype, first.qtype)
    arolla.testing.assert_expr_equal_by_fingerprint(
        arolla.abc.to_lowest(expr), first
    )

  @parameterized.parameters(
      M.core.default_if_unspecified(  # pyrefly: ignore[missing-attribute]
          M.annotation.qtype(L.x, arolla.UNSPECIFIED), arolla.unspecified()  # pyrefly: ignore[not-callable]
      ),
      M.core.default_if_unspecified(arolla.unspecified(), arolla.unspecified()),  # pyrefly: ignore[missing-attribute]
      M.core.default_if_unspecified(arolla.unspecified(), arolla.unit()),  # pyrefly: ignore[missing-attribute]
      M.core.default_if_unspecified(arolla.unspecified(), L.x),  # pyrefly: ignore[missing-attribute]
      M.core.default_if_unspecified(arolla.unspecified(), arolla.tuple()),  # pyrefly: ignore[missing-attribute]
  )
  def test_resolves_to_second(self, expr):
    second = expr.node_deps[1]
    self.assertEqual(expr.qtype, second.qtype)
    arolla.testing.assert_expr_equal_by_fingerprint(
        arolla.abc.to_lowest(expr), second
    )

  def test_eval(self):
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        arolla.eval(
            M.core.default_if_unspecified(arolla.unspecified(), arolla.tuple())  # pyrefly: ignore[missing-attribute]
        ),
        arolla.tuple(),
    )


if __name__ == '__main__':
  absltest.main()
