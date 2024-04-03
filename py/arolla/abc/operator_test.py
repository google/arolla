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

"""Tests for arolla.abc.operator.

To test ingeration with IPython/Colab, we need to verify the behaviour of:

  * oinspect.getdoc(op)
  * oinspect.getdoc(op._eval)
  * oinspect.getdoc(op.__call__)

  * oinspect.signature(op)
  * oinspect.signature(op._eval)
  * oinspect.signature(op.__call__)
"""

import inspect
import re
from typing import Any

from absl.testing import absltest
from arolla.abc import aux_binding_policy as abc_aux_binding_policy
from arolla.abc import clib
from arolla.abc import expr as abc_expr
from arolla.abc import operator as abc_operator
from arolla.abc import qtype as abc_qtype


def _oinspect_getdoc(obj: Any) -> str | None:
  """Testing implementation of IPython.core.oinspect.getdoc()."""
  try:
    return inspect.cleandoc(obj.getdoc())
  except AttributeError:
    pass
  return inspect.getdoc(obj)


def _oinspect_signature(obj: Any) -> inspect.Signature:
  """Testing implementation of IPython.core.oinspect.inspect()."""
  return inspect.signature(obj)


# Wrappers supporting cleanups between tests.
def _register_aux_binding_policy(
    aux_policy: str,
    policy_implementation: abc_aux_binding_policy.AuxBindingPolicy,
):
  _registered_aux_binding_policies.add(aux_policy)
  abc_aux_binding_policy.register_aux_binding_policy(
      aux_policy, policy_implementation
  )


def _remove_all_aux_binding_policies():
  global _registered_aux_binding_policies
  for aux_policy in _registered_aux_binding_policies:
    abc_aux_binding_policy.remove_aux_binding_policy(aux_policy)
  _registered_aux_binding_policies = set()


_registered_aux_binding_policies = set()


# A helper class that default-implements the abstract methods.
class _AuxBindingPolicy(abc_aux_binding_policy.AuxBindingPolicy):

  def make_python_signature(self, signature):
    return signature

  def bind_arguments(self, *args, **kwargs):
    raise NotImplementedError


class OperatorTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    _remove_all_aux_binding_policies()

  def test_type(self):
    op = abc_expr.make_lambda('x', abc_expr.placeholder('x'))
    self.assertIsInstance(op, abc_operator.Operator)
    self.assertIsNotNone(abc_operator.Operator.__call__)
    self.assertIsNotNone(abc_operator.Operator._eval)

  def test_bool(self):
    op = abc_expr.make_lambda('x', abc_expr.placeholder('x'))
    self.assertTrue(op)

  def test_hashable(self):
    op1 = abc_expr.make_lambda('x', abc_expr.placeholder('x'))
    op2 = abc_expr.make_lambda('y', abc_expr.placeholder('y'))
    self.assertEqual(hash(op1), hash(op1.fingerprint))
    self.assertEqual(op1, op1)
    self.assertFalse(bool(op1 == abc_qtype.QTYPE))
    self.assertNotEqual(op1, op2)
    self.assertNotEqual(op1, abc_qtype.QTYPE)

  def test_display_name(self):
    op = abc_expr.make_lambda(
        'x', abc_expr.placeholder('x'), name='display-name'
    )
    self.assertEqual(op.display_name, 'display-name')

  def test_oinspect_doc(self):
    _register_aux_binding_policy('policy', _AuxBindingPolicy())
    op = abc_expr.make_lambda(
        'x|policy', abc_expr.placeholder('x'), doc='custom-doc-string'
    )
    self.assertEqual(_oinspect_getdoc(op), 'custom-doc-string')
    self.assertEqual(_oinspect_getdoc(op._eval), 'custom-doc-string')
    self.assertEqual(_oinspect_getdoc(op.__call__), 'custom-doc-string')
    with self.assertRaisesRegex(
        ValueError, re.escape("operator 'no.such.operator' not found")
    ):
      _oinspect_getdoc(
          abc_expr.unsafe_make_registered_operator('no.such.operator')
      )

  def test_oinspect_signature(self):
    class CustomBindingPolicy(_AuxBindingPolicy):

      def __init__(self):
        super().__init__()
        self._sig = inspect.signature(
            lambda x, /, y=abc_qtype.Unspecified(), *args, z, **kwargs: None
        )

      def make_python_signature(self, signature):
        del signature
        return self._sig

    _register_aux_binding_policy('custom_policy', CustomBindingPolicy())
    op = abc_expr.make_lambda('x|custom_policy', abc_expr.placeholder('x'))
    self.assertIs(
        _oinspect_signature(op),
        abc_aux_binding_policy.aux_inspect_signature(op),
    )
    self.assertIs(
        _oinspect_signature(op._eval),
        abc_aux_binding_policy.aux_inspect_signature(op),
    )
    self.assertIs(
        _oinspect_signature(op.__call__),
        abc_aux_binding_policy.aux_inspect_signature(op),
    )

  def test_bind(self):
    id_op = abc_expr.make_lambda('x', abc_expr.placeholder('x'))

    class CustomBindingPolicy(_AuxBindingPolicy):

      def bind_arguments(self, _signature, *_args, **_kwargs):
        return (abc_qtype.Unspecified(),)

      def make_literal(self, value):
        return abc_expr.bind_op(id_op, value)

    _register_aux_binding_policy('policy', CustomBindingPolicy())
    op = abc_expr.make_lambda('x|policy', abc_expr.placeholder('x'))
    expr = op(object())
    self.assertIsInstance(expr, abc_expr.Expr)
    self.assertTrue(
        op(object()).equals(
            abc_expr.bind_op(
                op, abc_expr.bind_op(id_op, abc_qtype.Unspecified())
            )
        )
    )

  def test_eval(self):
    class CustomBindingPolicy(_AuxBindingPolicy):

      def bind_arguments(self, _signature, *_args, **_kwargs):
        return (abc_qtype.Unspecified(),)

    _register_aux_binding_policy('policy', CustomBindingPolicy())
    op = abc_expr.make_lambda('x|policy', abc_expr.placeholder('x'))
    self.assertEqual(op._eval(object()), abc_qtype.Unspecified())

  def test_helper_get_operator_name(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected an operator, got object'
    ):
      clib.get_operator_name(object())  # pytype: disable=wrong-arg-types
    self.assertEqual(
        inspect.signature(clib.get_operator_name),
        inspect.signature(lambda op, /: None),
    )

  def test_helper_get_operator_doc(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected an operator, got object'
    ):
      clib.get_operator_doc(object())  # pytype: disable=wrong-arg-types
    self.assertEqual(
        inspect.signature(clib.get_operator_doc),
        inspect.signature(lambda op, /: None),
    )


class RegisteredOperatorTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    _remove_all_aux_binding_policies()

  def test_basics(self):
    op = abc_operator.RegisteredOperator('qtype.qtype_of')
    self.assertIsInstance(op, abc_operator.RegisteredOperator)
    with self.assertRaisesWithLiteralMatch(
        LookupError, 'unknown operator: unknown.operator'
    ):
      _ = abc_operator.RegisteredOperator('unknown.operator')

  def test_dynamic_behaviour(self):
    _register_aux_binding_policy('', _AuxBindingPolicy())
    reg_op_name = 'registered_operator_test.test_dynamic_behaviour'
    op1 = abc_expr.make_lambda(
        'x, unused_y', abc_expr.placeholder('x'), doc='doc1'
    )
    reg_op = abc_expr.register_operator(reg_op_name, op1)
    with self.subTest('op1-getdoc'):
      self.assertEqual(_oinspect_getdoc(reg_op), op1.getdoc())
      self.assertEqual(_oinspect_getdoc(reg_op._eval), op1.getdoc())
      self.assertEqual(_oinspect_getdoc(reg_op.__call__), op1.getdoc())
    with self.subTest('op1-signature'):
      self.assertEqual(
          _oinspect_signature(reg_op),
          abc_aux_binding_policy.aux_inspect_signature(op1),
      )
      self.assertEqual(
          _oinspect_signature(reg_op._eval),
          abc_aux_binding_policy.aux_inspect_signature(op1),
      )
      self.assertEqual(
          _oinspect_signature(reg_op.__call__),
          abc_aux_binding_policy.aux_inspect_signature(op1),
      )
    op2 = abc_expr.make_lambda(
        'unused_x, y', abc_expr.placeholder('y'), doc='doc2'
    )
    with self.assertWarns(RuntimeWarning):
      abc_expr.unsafe_override_registered_operator(reg_op_name, op2)
    with self.subTest('op2-getdoc'):
      self.assertEqual(_oinspect_getdoc(reg_op), op2.getdoc())
      self.assertEqual(_oinspect_getdoc(reg_op._eval), op2.getdoc())
      self.assertEqual(_oinspect_getdoc(reg_op.__call__), op2.getdoc())
    with self.subTest('op2-signature'):
      self.assertEqual(
          _oinspect_signature(reg_op),
          abc_aux_binding_policy.aux_inspect_signature(op2),
      )
      self.assertEqual(
          _oinspect_signature(reg_op._eval),
          abc_aux_binding_policy.aux_inspect_signature(op2),
      )
      self.assertEqual(
          _oinspect_signature(reg_op.__call__),
          abc_aux_binding_policy.aux_inspect_signature(op2),
      )


if __name__ == '__main__':
  absltest.main()
