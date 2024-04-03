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

"""Tests for operator_repr."""

import re

from absl.testing import absltest
from arolla import rl


L = rl.L
M = rl.unsafe_operators_container()


class OperatorClassReprTest(absltest.TestCase):

  def test_register(self):
    def repr_fn(node, node_tokens):
      self.assertIsInstance(node.op, rl.types.DummyOperator)
      x_token = node_tokens[node.node_deps[0]]
      y_token = node_tokens[node.node_deps[1]]
      res = rl.abc.ReprToken()
      res.text = f'{x_token.text} + {y_token.text}'
      return res

    rl.abc.register_op_repr_fn_by_qvalue_specialization_key(
        '::arolla::operator_loader::DummyOperator', repr_fn
    )
    op = rl.types.DummyOperator(
        'test_register.op', 'x, y', result_qtype=rl.FLOAT32
    )
    self.assertEqual(repr(op(L.x, L.y)), 'L.x + L.y')

  def test_precedence(self):
    add_op = rl.types.DummyOperator(
        'test_precedence.add', 'x, y', result_qtype=rl.FLOAT32
    )
    mod_op = rl.types.DummyOperator(
        'test_precedence.mod', 'x, y', result_qtype=rl.FLOAT32
    )

    def repr_fn(node, node_tokens):
      x_token = node_tokens[node.node_deps[0]]
      y_token = node_tokens[node.node_deps[1]]
      if node.op == add_op:
        res = rl.abc.ReprToken()
        res.text = f'{x_token.text} + {y_token.text}'
        res.precedence.left = 5
        res.precedence.right = 4
        return res
      else:
        self.assertEqual(node.op, mod_op)
        self.assertEqual(x_token.precedence.left, 5)
        self.assertEqual(x_token.precedence.right, 4)
        res = rl.abc.ReprToken()
        res.text = f'({x_token.text}) % {y_token.text}'
        res.precedence.left = 3
        res.precedence.right = 2
        return res

    rl.abc.register_op_repr_fn_by_qvalue_specialization_key(
        '::arolla::operator_loader::DummyOperator', repr_fn
    )
    expr = -mod_op(add_op(L.x, L.y), L.z)
    self.assertEqual(repr(expr), '-((L.x + L.y) % L.z)')

  def test_repr_fn_none_fallback(self):
    # Uses normal printing if None.

    def repr_fn(node, node_tokens):
      del node, node_tokens
      return None

    rl.abc.register_op_repr_fn_by_qvalue_specialization_key(
        '::arolla::operator_loader::DummyOperator', repr_fn
    )
    op = rl.types.DummyOperator(
        'test_repr_fn_none_fallback.op', 'x, y', result_qtype=rl.FLOAT32
    )
    self.assertEqual(
        repr(op(L.x, L.y)), 'test_repr_fn_none_fallback.op(L.x, L.y)'
    )

  def test_missing_node(self):
    def repr_fn(_, node_tokens):
      with self.assertRaisesRegex(LookupError, 'not found'):
        _ = node_tokens[L.test_missing_node]
      return None

    rl.abc.register_op_repr_fn_by_qvalue_specialization_key(
        '::arolla::operator_loader::DummyOperator', repr_fn
    )
    op = rl.types.DummyOperator(
        'test_missing_node.op', 'x', result_qtype=rl.FLOAT32
    )
    self.assertEqual(repr(op(L.x)), 'test_missing_node.op(L.x)')

  def test_repr_fn_raises(self):
    def repr_fn(node, node_tokens):
      del node, node_tokens
      raise ValueError('test')

    rl.abc.register_op_repr_fn_by_qvalue_specialization_key(
        '::arolla::operator_loader::DummyOperator', repr_fn
    )
    op = rl.types.DummyOperator(
        'test_repr_fn_raises.op', 'x', result_qtype=rl.FLOAT32
    )
    expr = op(L.x)
    with self.assertWarnsRegex(
        RuntimeWarning,
        re.escape(
            'failed to evaluate the repr_fn on node with'
            f' op={op.display_name} and fingerprint={expr.fingerprint}'
        ),
    ):
      repr_expr = repr(expr)
    self.assertEqual(repr_expr, 'test_repr_fn_raises.op(L.x)')

  def test_avoid_unsafe_deref(self):
    # Must not be done as it is unsafe.
    node_tokens_cache = None

    def repr_fn(node, node_tokens):
      # Store node_tokens in a cache that we try to access later.
      nonlocal node_tokens_cache
      node_tokens_cache = node_tokens

      x_token = node_tokens[node.node_deps[0]]
      res = rl.abc.ReprToken()
      res.text = f'DummyOp({x_token.text})'
      return res

    rl.abc.register_op_repr_fn_by_qvalue_specialization_key(
        '::arolla::operator_loader::DummyOperator', repr_fn
    )
    op = rl.types.DummyOperator(
        'test_avoid_unsafe_deref.op', 'x', result_qtype=rl.FLOAT32
    )
    expr = op(L.x)
    self.assertEqual(repr(expr), 'DummyOp(L.x)')

    self.assertIsNotNone(node_tokens_cache)
    # The underlying map is destroyed and should not be used.
    with self.assertRaisesWithLiteralMatch(
        LookupError,
        'arolla.abc.NodeTokenView.__getitem__() node with op=None and'
        f' fingerprint={L.x.fingerprint} was not found',
    ):
      _ = node_tokens_cache[L.x]  # pytype: disable=unsupported-operands

    # Repr works as expected still.
    self.assertEqual(repr(op(L.x)), 'DummyOp(L.x)')

  def test_performance(self):
    def repr_fn(node, node_tokens):
      del node, node_tokens
      return None

    rl.abc.register_op_repr_fn_by_qvalue_specialization_key(
        '::arolla::operator_loader::DummyOperator', repr_fn
    )
    op = rl.types.DummyOperator(
        'test_register.op', 'x', result_qtype=rl.FLOAT32
    )
    expr = L.x
    # 2k -> ~30s if the node_tokens dict is copied at each call. ~0.1s using
    # a NodeTokenView instead.
    for _ in range(2000):
      expr = op(expr)
    _ = repr(expr)

  def test_repr_token(self):
    token = rl.abc.ReprToken()
    with self.subTest('default'):
      self.assertEmpty(token.text)
      self.assertIsInstance(token.precedence, rl.abc.ReprToken.Precedence)
      self.assertEqual(token.precedence.left, -1)
      self.assertEqual(token.precedence.right, -1)
    with self.subTest('text'):
      token.text = 'test'
      self.assertEqual(token.text, 'test')
    with self.subTest('precedence'):
      token.precedence.left = 1
      token.precedence.right = 2
      self.assertEqual(token.precedence.left, 1)
      self.assertEqual(token.precedence.right, 2)

  def test_precedence_constants(self):
    self.assertEqual(rl.abc.ReprToken.PRECEDENCE_OP_SUBSCRIPTION.left, 0)
    self.assertEqual(rl.abc.ReprToken.PRECEDENCE_OP_SUBSCRIPTION.right, -1)


@rl.optools.add_to_registry()
@rl.optools.as_lambda_operator('test.add')
def test_add(x, y):
  return x + y


@rl.optools.add_to_registry()
@rl.optools.as_lambda_operator('test.subtract')
def test_subtract(x, y):
  return x - y


@rl.optools.add_to_registry()
@rl.optools.as_lambda_operator('test.multiply')
def test_multiply(x, y):
  return x * y


class RegisteredOperatorReprTest(absltest.TestCase):

  def test_register(self):
    def repr_fn(node, node_tokens):
      self.assertEqual(node.op, test_add)
      x_token = node_tokens[node.node_deps[0]]
      y_token = node_tokens[node.node_deps[1]]
      res = rl.abc.ReprToken()
      res.text = f'{x_token.text} + {y_token.text}'
      return res

    with self.subTest('initial_registration'):
      rl.abc.register_op_repr_fn_by_registration_name('test.add', repr_fn)
      self.assertEqual(repr(M.test.add(L.x, L.y)), 'L.x + L.y')

    def new_repr_fn(node, node_tokens):
      del node, node_tokens
      res = rl.abc.ReprToken()
      res.text = 'ABC'
      return res

    with self.subTest('re_registration'):
      rl.abc.register_op_repr_fn_by_registration_name('test.add', new_repr_fn)
      self.assertEqual(repr(M.test.add(L.x, L.y)), 'ABC')

  def test_repr_fn_none_fallback(self):
    # Uses normal printing if None.

    def repr_fn(node, node_tokens):
      del node, node_tokens
      return None

    rl.abc.register_op_repr_fn_by_registration_name('test.subtract', repr_fn)
    self.assertEqual(
        repr(M.test.subtract(L.x, L.y)), 'M.test.subtract(L.x, L.y)'
    )


if __name__ == '__main__':
  absltest.main()
