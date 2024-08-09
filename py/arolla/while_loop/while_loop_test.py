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

"""Tests for py.arolla.operators.while_loop."""

import inspect

from absl.testing import absltest
from arolla import arolla
from arolla.while_loop import while_loop

L = arolla.L
P = arolla.P
M = arolla.M


class WhileLoopTest(absltest.TestCase):

  def test_simple_loop(self):
    # Expression computing GCD of the a and b leaves.
    # Python equivalent:
    #   x = L.a
    #   y = L.b
    #   while y != 0:
    #     x, y = y, x % y
    #   gcd = x
    #
    gcd = while_loop.while_loop(
        initial_state=dict(x=L.a, y=L.b),
        condition=P.y != 0,
        body=dict(x=P.y, y=P.x % P.y),
    )
    self.assertEqual(
        arolla.eval(gcd, a=arolla.int32(1), b=arolla.int32(1))['x'], 1
    )
    self.assertEqual(
        arolla.eval(gcd, a=arolla.int32(57), b=arolla.int32(9))['x'], 3
    )

  def test_pointwise_loop(self):
    # Evaluate a loop pointwise using core.map operator.
    scalar_gcd = arolla.LambdaOperator(
        'a, b',
        M.namedtuple.get_field(
            while_loop.while_loop(
                initial_state=dict(x=P.a, y=P.b),
                condition=P.y != 0,
                body=dict(x=P.y, y=P.x % P.y),
            ),
            'x',
        ),
    )
    array_gcd = M.core.map(scalar_gcd, L.a, L.b)
    arolla.testing.assert_qvalue_allequal(
        arolla.eval(
            array_gcd, a=arolla.array([1, 57, 114]), b=arolla.array([1, 9, 171])
        ),
        arolla.array([1, 3, 57]),
    )

  def test_vectorized_loop(self):
    # Evaluate GCD on arrays until all the rows are finished (compare with the
    # pointwise example using core.map above).

    # Use "P.y or 1" to avoid division by zero error.
    # TODO(b/174063091) Use M.core.mod(P.x, P.y, default=1) instead.
    y_or_1 = M.core.where(P.y != 0, P.y, M.core.const_like(P.y, 1))
    array_gcd = M.namedtuple.get_field(
        while_loop.while_loop(
            initial_state=dict(x=L.a, y=L.b),
            condition=M.array.count(P.y != 0) != 0,
            body=dict(
                x=M.core.where(P.y != 0, P.y, P.x),
                y=M.core.where(P.y != 0, P.x % y_or_1, P.y),
            ),
        ),
        'x',
    )
    arolla.testing.assert_qvalue_allequal(
        arolla.eval(
            array_gcd, a=arolla.array([1, 57, 114]), b=arolla.array([1, 9, 171])
        ),
        arolla.array([1, 3, 57]),
    )

  def test_loop_with_leaves(self):
    # Expression (very slowly) computing floor(L.a).
    # Python equivalent:
    #   x = 0
    #   while float(x + 1) <= L.a:
    #     x += 1
    #   floor_of_a = x
    #
    floor_of_a = M.namedtuple.get_field(
        while_loop.while_loop(
            initial_state=dict(x=0),
            condition=M.core.to_float64(P.x + 1) <= L.a,
            body=dict(x=P.x + 1),
        ),
        'x',
    )
    self.assertEqual(arolla.eval(floor_of_a, a=arolla.float32(5.0)), 5)
    self.assertEqual(arolla.eval(floor_of_a, a=arolla.float32(5.7)), 5)

  def test_nested_loop(self):
    # Python equivalent:
    #   num = P.n
    #   divisor = 2
    #   while num % divisor != 0 and divisor**2 <= num:
    #     divisor = divisor + 1
    #   divisor_candidate = divisor
    divisor_candidate = M.namedtuple.get_field(
        while_loop.while_loop(
            initial_state=dict(num=P.n, divisor=2),
            condition=(P.num % P.divisor != 0)
            & (P.divisor * P.divisor <= P.num),
            body=dict(divisor=P.divisor + 1),
        ),
        'divisor',
    )

    # Operator evaluates to `present` if the given argument is prime.
    is_prime = arolla.LambdaOperator(
        (P.n % divisor_candidate != 0) | (divisor_candidate == P.n),
        name='is_prime',
    )

    self.assertTrue(arolla.eval(is_prime(2)))
    self.assertTrue(arolla.eval(is_prime(3)))
    self.assertFalse(arolla.eval(is_prime(16)))

    # Expression computing the prime next to L.n.
    # Python equivalent:
    #   n = L.n
    #   while not is_prime(n):
    #     n = n+1
    #   next_prime = n
    #
    next_prime = M.namedtuple.get_field(
        while_loop.while_loop(
            initial_state=dict(n=L.n),
            condition=M.core.presence_not(is_prime(P.n)),
            body=dict(n=P.n + 1),
        ),
        'n',
    )

    self.assertEqual(arolla.eval(next_prime, n=arolla.int32(2)), 2)
    self.assertEqual(arolla.eval(next_prime, n=arolla.int32(57)), 59)

  def test_expression_with_tolower_depending_on_literals(self):
    iter_loop = M.namedtuple.get_field(
        while_loop.while_loop(
            initial_state=dict(z=L.z),
            # arolla.FLOAT32 literal should be available for core.cast_values
            # lowering, so it cannot be extracted as "immutable".
            condition=M.core.cast_values(P.z, arolla.FLOAT32) < 5.0,
            body=dict(z=P.z + 1.0),
        ),
        'z',
    )
    self.assertEqual(
        arolla.eval(iter_loop, z=arolla.optional_float32(1.0)),
        arolla.optional_float32(5.0),
    )

  def test_implicit_casting(self):
    # Initial value of x is int32, while the updated one is int64.
    loop = M.namedtuple.get_field(
        while_loop.while_loop(
            initial_state=dict(x=arolla.int32(1)),
            condition=P.x < 100,
            body=dict(x=P.x + arolla.int64(2)),
        ),
        'x',
    )
    loop_result = arolla.eval(loop)
    self.assertEqual(loop_result, 101)
    self.assertEqual(loop_result.qtype, arolla.INT64)

  def test_maximum_iterations(self):
    loop_results = while_loop.while_loop(
        initial_state=dict(x=10000),
        # infinite loop
        condition=P.x != 0,
        body=dict(x=P.x + 1),
        maximum_iterations=57,
    )
    self.assertEqual(arolla.eval(loop_results)['x'], 10057)

  def test_loop_operator_wrapper(self):
    # Expression computing GCD of the a and b leaves.
    gcd = M.namedtuple.get_field(
        while_loop.while_loop(
            initial_state=dict(x=L.a, y=L.b),
            condition=P.y != 0,
            body=dict(x=P.y, y=P.x % P.y),
        ),
        'x',
    )

    loop_op = gcd.node_deps[0].op

    self.assertLen(inspect.signature(loop_op.internal_body).parameters, 1)
    arolla.testing.assert_expr_equal_by_fingerprint(
        loop_op.internal_body.lambda_body,
        M.namedtuple.make(
            'x,y',
            M.core.get_nth(P.loop_state, arolla.int64(1)),
            M.math.mod(
                M.core.get_nth(P.loop_state, arolla.int64(0)),
                M.core.get_nth(P.loop_state, arolla.int64(1)),
            ),
        ),
    )

    self.assertLen(inspect.signature(loop_op.internal_condition).parameters, 1)
    arolla.testing.assert_expr_equal_by_fingerprint(
        loop_op.internal_condition.lambda_body,
        M.core.not_equal(M.core.get_nth(P.loop_state, arolla.int64(1)), 0),
    )


if __name__ == '__main__':
  absltest.main()
