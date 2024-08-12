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

"""Tests for arolla.expr.default_view."""

from absl.testing import absltest
from arolla.expr import containers
from arolla.expr import default_view as _
from arolla.operators import operators_clib as _
from arolla.testing import testing as arolla_testing
from arolla.types import types as arolla_types


class ExprTest(absltest.TestCase):

  def test_default_view(self):
    p = containers.PlaceholderContainer()
    m = containers.OperatorsContainer()
    y = arolla_types.float32(1)
    assert_expr_equal = arolla_testing.assert_expr_equal_by_fingerprint
    assert_expr_equal(p.x < y, m.core.less(p.x, y))
    assert_expr_equal(y > p.x, m.core.less(p.x, y))
    assert_expr_equal(p.x <= y, m.core.less_equal(p.x, y))
    assert_expr_equal(y >= p.x, m.core.less_equal(p.x, y))
    assert_expr_equal(p.x == y, m.core.equal(p.x, y))
    assert_expr_equal(y == p.x, m.core.equal(p.x, y))
    assert_expr_equal(p.x != y, m.core.not_equal(p.x, y))
    assert_expr_equal(y != p.x, m.core.not_equal(p.x, y))
    assert_expr_equal(p.x >= y, m.core.greater_equal(p.x, y))
    assert_expr_equal(y <= p.x, m.core.greater_equal(p.x, y))
    assert_expr_equal(p.x > y, m.core.greater(p.x, y))
    assert_expr_equal(y < p.x, m.core.greater(p.x, y))
    assert_expr_equal(~p.x, m.core.presence_not(p.x))
    assert_expr_equal(p.x & y, m.core.presence_and(p.x, y))
    assert_expr_equal(y & p.x, m.core.presence_and(y, p.x))
    assert_expr_equal(p.x | y, m.core.presence_or(p.x, y))
    assert_expr_equal(y | p.x, m.core.presence_or(y, p.x))
    assert_expr_equal(+p.x, m.math.pos(p.x))
    assert_expr_equal(-p.x, m.math.neg(p.x))
    assert_expr_equal(p.x + y, m.math.add(p.x, y))
    assert_expr_equal(y + p.x, m.math.add(y, p.x))
    assert_expr_equal(p.x - y, m.math.subtract(p.x, y))
    assert_expr_equal(y - p.x, m.math.subtract(y, p.x))
    assert_expr_equal(p.x * y, m.math.multiply(p.x, y))
    assert_expr_equal(y * p.x, m.math.multiply(y, p.x))
    assert_expr_equal(p.x / y, m.math.divide(p.x, y))
    assert_expr_equal(y / p.x, m.math.divide(y, p.x))
    assert_expr_equal(p.x // y, m.math.floordiv(p.x, y))
    assert_expr_equal(y // p.x, m.math.floordiv(y, p.x))
    assert_expr_equal(p.x % y, m.math.mod(p.x, y))
    assert_expr_equal(y % p.x, m.math.mod(y, p.x))
    assert_expr_equal(p.x**y, m.math.pow(p.x, y))
    assert_expr_equal(y**p.x, m.math.pow(y, p.x))


if __name__ == '__main__':
  absltest.main()
