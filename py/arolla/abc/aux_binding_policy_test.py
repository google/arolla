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

"""Tests for arolla.abc.aux_binding_policy."""

import inspect
import re
from typing import Any, Callable
from unittest import mock

from absl.testing import absltest
from arolla.abc import aux_binding_policy as abc_aux_binding_policy
from arolla.abc import clib
from arolla.abc import expr as abc_expr
from arolla.abc import qtype as abc_qtype
from arolla.abc import signature as abc_signature

_registered_aux_binding_policies = set()


# Wrapper for register_aux_binding_policy() that records the registered policies
# for cleanup between tests.
def _register_aux_binding_policy(
    aux_policy: str,
    policy_implementation: abc_aux_binding_policy.AuxBindingPolicy,
):
  _registered_aux_binding_policies.add(aux_policy)
  abc_aux_binding_policy.register_aux_binding_policy(
      aux_policy, policy_implementation
  )


# Wrapper for register_classic_aux_binding_policy_with_custom_boxing() that
# records the registered policies for cleanup between tests.
def _register_classic_aux_binding_policy_with_custom_boxing(
    aux_policy: str,
    as_qvalue_or_expr_fn: Callable[[Any], abc_qtype.QValue | abc_expr.Expr],
    *,
    make_literal_fn: Callable[[abc_qtype.QValue], abc_expr.Expr] | None = None,
):
  _registered_aux_binding_policies.add(aux_policy)
  abc_aux_binding_policy.register_classic_aux_binding_policy_with_custom_boxing(
      aux_policy, as_qvalue_or_expr_fn, make_literal_fn=make_literal_fn
  )


def _remove_all_aux_binding_policies():
  global _registered_aux_binding_policies
  for aux_policy in _registered_aux_binding_policies:
    abc_aux_binding_policy.remove_aux_binding_policy(aux_policy)
  _registered_aux_binding_policies = set()


# A helper class that default-implements the abstract methods.
class _AuxBindingPolicy(abc_aux_binding_policy.AuxBindingPolicy):

  def make_python_signature(self, signature):
    raise NotImplementedError

  def bind_arguments(self, *_):
    raise NotImplementedError


# A helper operator.
_make_tuple_op = abc_expr.make_lambda(
    '*args', abc_expr.placeholder('args'), name='make_tuple'
)


class AuxBindingPolicyTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    _remove_all_aux_binding_policies()

  def test_aux_inspect_signature_with_inspect_signature(self):
    op = abc_expr.make_lambda('x|aux_policy', abc_expr.placeholder('x'))
    sig = inspect.signature(
        lambda x, /, y=abc_qtype.Unspecified(), *args, z, **kwargs: None
    )

    class CustomBindingPolicy(_AuxBindingPolicy):

      def make_python_signature(self, signature):
        nonlocal op
        nonlocal sig
        assert signature == abc_signature.get_operator_signature(op)
        return sig

    _register_aux_binding_policy('aux_policy', CustomBindingPolicy())
    self.assertIs(abc_aux_binding_policy.aux_inspect_signature(op), sig)

  def test_aux_inspect_signature_with_abc_signature(self):
    op = abc_expr.make_lambda('x|aux_policy', abc_expr.placeholder('x'))

    class CustomBindingPolicy(_AuxBindingPolicy):

      def make_python_signature(self, signature):
        nonlocal op
        assert signature == abc_signature.get_operator_signature(op)
        return abc_signature.Signature((
            (
                abc_signature.SignatureParameter(
                    ('a', 'positional-only', None)
                ),
                abc_signature.SignatureParameter(
                    ('b', 'positional-or-keyword', abc_qtype.Unspecified())
                ),
                abc_signature.SignatureParameter(
                    ('args', 'variadic-positional', None)
                ),
                abc_signature.SignatureParameter(('c', 'keyword-only', None)),
                abc_signature.SignatureParameter(
                    ('kwargs', 'variadic-keyword', None)
                ),
            ),
            '',
        ))

    _register_aux_binding_policy('aux_policy', CustomBindingPolicy())
    self.assertEqual(
        abc_aux_binding_policy.aux_inspect_signature(op),
        inspect.signature(
            lambda a, /, b=abc_qtype.Unspecified(), *args, c, **kwargs: None
        ),
    )

  def test_aux_inspect_signature_by_name(self):
    class CustomBindingPolicy(_AuxBindingPolicy):

      def make_python_signature(self, signature):
        return signature

    _register_aux_binding_policy('', CustomBindingPolicy())
    self.assertEqual(
        abc_aux_binding_policy.aux_inspect_signature('annotation.name'),
        inspect.signature(lambda expr, name: None),
    )
    with self.assertRaisesWithLiteralMatch(
        LookupError,
        'arolla.abc.aux_get_python_signature() operator not found:'
        " 'unknown.op'",
    ):
      _ = abc_aux_binding_policy.aux_inspect_signature('unknown.op')

  def test_aux_inspect_signature_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.aux_get_python_signature() expected Operator|str, got op:'
        ' object',
    ):
      _ = abc_aux_binding_policy.aux_inspect_signature(object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.aux_get_python_signature() expected Operator|str, got op:'
        ' Unspecified',
    ):
      _ = abc_aux_binding_policy.aux_inspect_signature(abc_qtype.Unspecified())

  def test_aux_inspect_signature_value_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape("operator 'unknown.operator' not found")
    ):
      _ = abc_aux_binding_policy.aux_inspect_signature(
          abc_expr.unsafe_make_registered_operator('unknown.operator')
      )

  def test_aux_bind_op(self):
    class CustomBindingPolicy(_AuxBindingPolicy):

      def bind_arguments(self, signature, *args, **kwargs):
        assert signature == abc_signature.get_operator_signature(op)
        assert (
            len(args) == 1
            and args[0].fingerprint == abc_expr.placeholder('x').fingerprint
        )
        assert kwargs == dict(y=None)
        return (abc_expr.placeholder('x'), abc_qtype.Unspecified())

    op = abc_expr.make_lambda(
        'x, unused_y|aux_policy', abc_expr.placeholder('x')
    )
    _register_aux_binding_policy('aux_policy', CustomBindingPolicy())
    expected_expr = abc_expr.make_operator_node(
        op,
        (abc_expr.placeholder('x'), abc_expr.literal(abc_qtype.Unspecified())),
    )
    self.assertEqual(
        abc_aux_binding_policy.aux_bind_op(
            op, abc_expr.placeholder('x'), y=None
        ).fingerprint,
        expected_expr.fingerprint,
    )

  def test_aux_bind_op_make_literal(self):
    id_op = abc_expr.make_lambda('x', abc_expr.placeholder('x'))

    class CustomBindingPolicy(_AuxBindingPolicy):

      def bind_arguments(self, signature, x):
        return (abc_qtype.Unspecified(),)

      def make_literal(self, value):
        return abc_expr.bind_op(id_op, value)

    op = abc_expr.make_lambda('x |aux_policy', abc_expr.placeholder('x'))
    _register_aux_binding_policy('aux_policy', CustomBindingPolicy())
    expected_expr = abc_expr.bind_op(
        op, abc_expr.bind_op(id_op, abc_qtype.Unspecified())
    )
    self.assertEqual(
        abc_aux_binding_policy.aux_bind_op(
            op, abc_expr.placeholder('x')
        ).fingerprint,
        expected_expr.fingerprint,
    )

  def test_aux_bind_op_make_literal_default_impl(self):
    class CustomBindingPolicy(_AuxBindingPolicy):

      def bind_arguments(self, signature, x):
        return (abc_qtype.Unspecified(),)

    op = abc_expr.make_lambda('x |aux_policy', abc_expr.placeholder('x'))
    _register_aux_binding_policy('aux_policy', CustomBindingPolicy())
    expected_expr = abc_expr.bind_op(op, abc_qtype.Unspecified())
    self.assertEqual(
        abc_aux_binding_policy.aux_bind_op(
            op, abc_expr.placeholder('x')
        ).fingerprint,
        expected_expr.fingerprint,
    )

  def test_aux_bind_op_make_literal_skips_expr(self):
    class CustomBindingPolicy(_AuxBindingPolicy):

      def bind_arguments(self, signature, x):
        return (abc_expr.literal(abc_qtype.Unspecified()),)

      def make_literal(self, value):
        raise NotImplementedError

    op = abc_expr.make_lambda('x |aux_policy', abc_expr.placeholder('x'))
    _register_aux_binding_policy('aux_policy', CustomBindingPolicy())
    expected_expr = abc_expr.bind_op(op, abc_qtype.Unspecified())
    self.assertEqual(
        abc_aux_binding_policy.aux_bind_op(
            op, abc_expr.placeholder('x')
        ).fingerprint,
        expected_expr.fingerprint,
    )

  def test_aux_bind_op_make_literal_invalid_output(self):
    class CustomBindingPolicy(_AuxBindingPolicy):

      def bind_arguments(self, signature, x):
        return (abc_qtype.Unspecified(),)

      def make_literal(self, value):
        return value

    op = abc_expr.make_lambda('x |aux_policy', abc_expr.placeholder('x'))
    _register_aux_binding_policy('aux_policy', CustomBindingPolicy())

    try:
      _ = abc_aux_binding_policy.aux_bind_op(op, abc_expr.placeholder('x'))
      self.fail('expected a RuntimeError')
    except RuntimeError as ex:
      outer_ex = ex
    self.assertEqual(
        str(outer_ex), 'arolla.abc.aux_bind_op() call to make_literal() failed'
    )
    self.assertEqual(
        str(outer_ex.__cause__), 'expected arolla.abc.Expr, got Unspecified'
    )

  def test_aux_bind_op_make_literal_different_fingerprint(self):
    class CustomBindingPolicy(_AuxBindingPolicy):

      def bind_arguments(self, signature, x):
        return (abc_qtype.Unspecified(),)

      def make_literal(self, value):
        return abc_expr.literal(abc_qtype.NOTHING)

    op = abc_expr.make_lambda('x |aux_policy', abc_expr.placeholder('x'))
    _register_aux_binding_policy('aux_policy', CustomBindingPolicy())

    try:
      _ = abc_aux_binding_policy.aux_bind_op(op, abc_expr.placeholder('x'))
      self.fail('expected a RuntimeError')
    except RuntimeError as ex:
      outer_ex = ex
    self.assertEqual(
        str(outer_ex), 'arolla.abc.aux_bind_op() call to make_literal() failed'
    )
    self.assertEqual(
        str(outer_ex.__cause__),
        'make_literal(x).qvalue.fingerprint != x.fingerprint',
    )

  def test_aux_bind_op_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "arolla.abc.aux_bind_op() missing 1 required positional argument: 'op'",
    ):
      _ = abc_aux_binding_policy.aux_bind_op()  # pytype: disable=missing-parameter
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.aux_bind_op() expected Operator|str, got op: bytes',
    ):
      _ = abc_aux_binding_policy.aux_bind_op(b'op_name')  # pytype: disable=wrong-arg-types

  def test_aux_bind_op_value_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape("operator 'unknown.operator' not found")
    ):
      _ = abc_aux_binding_policy.aux_bind_op(
          abc_expr.unsafe_make_registered_operator('unknown.operator')
      )

  def test_aux_bind_op_make_op_node_failed(self):
    class CustomBindingPolicy(_AuxBindingPolicy):

      def bind_arguments(self, _signature, *_args, **_kwargs):
        x = abc_expr.placeholder('x')
        return (x, x)

    op = abc_expr.make_lambda('x|aux_policy', abc_expr.placeholder('x'))
    _register_aux_binding_policy('aux_policy', CustomBindingPolicy())
    try:
      _ = abc_aux_binding_policy.aux_bind_op(op, abc_qtype.Unspecified())
      self.fail('expected a RuntimeError')
    except RuntimeError as ex:
      outer_ex = ex
    self.assertEqual(
        str(outer_ex),
        'arolla.abc.aux_bind_arguments() auxiliary binding policy has failed:'
        " 'aux_policy'",
    )
    self.assertRegex(
        str(outer_ex.__cause__),
        re.escape(
            'incorrect number of dependencies passed to an operator node:'
            ' expected 1 but got 2'
        ),
    )

  def test_aux_bind_op_signature(self):
    self.assertEqual(
        inspect.signature(abc_aux_binding_policy.aux_bind_op),
        inspect.signature(lambda op, /, *args, **kwargs: None),
    )

  def test_aux_get_python_signature_signature(self):
    self.assertEqual(
        inspect.signature(clib.aux_get_python_signature),
        inspect.signature(lambda op, /: None),
    )

  def test_aux_binding_policy_raises(self):
    _register_aux_binding_policy('aux_policy', _AuxBindingPolicy())
    op = abc_expr.make_lambda('x|aux_policy', abc_expr.placeholder('x'))
    with self.assertRaisesWithLiteralMatch(
        RuntimeError,
        'arolla.abc.aux_make_python_signature() auxiliary binding policy has'
        " failed: 'aux_policy'",
    ):
      _ = abc_aux_binding_policy.aux_inspect_signature(op)
    with self.assertRaisesWithLiteralMatch(
        RuntimeError,
        'arolla.abc.aux_bind_arguments() auxiliary binding policy has failed:'
        " 'aux_policy'",
    ):
      _ = abc_aux_binding_policy.aux_bind_op(op)

  def test_unregistered_aux_policy(self):
    op = abc_expr.make_lambda('x|aux_policy', abc_expr.placeholder('x'))
    with self.assertRaisesWithLiteralMatch(
        RuntimeError,
        'arolla.abc.aux_make_python_signature() auxiliary binding policy not'
        " found: 'aux_policy'",
    ):
      _ = abc_aux_binding_policy.aux_inspect_signature(op)
    with self.assertRaisesWithLiteralMatch(
        RuntimeError,
        'arolla.abc.aux_bind_arguments() auxiliary binding policy not found:'
        " 'aux_policy'",
    ):
      _ = abc_aux_binding_policy.aux_bind_op(op)

  def test_register_aux_binding_policy_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'expected an AuxBindingPolicy, got policy_implementation: object',
    ):
      _register_aux_binding_policy('', object())  # pytype: disable=wrong-arg-types

  def test_py_aux_binding_policy_bind_arguments_non_tuple_result(self):
    class CustomBindingPolicy(_AuxBindingPolicy):

      def bind_arguments(self, signature, *args, **kwargs):
        return []

    _register_aux_binding_policy('aux_policy', CustomBindingPolicy())
    op = abc_expr.make_lambda('x|aux_policy', abc_expr.placeholder('x'))
    try:
      _ = abc_aux_binding_policy.aux_bind_op(op, abc_qtype.Unspecified())
      self.fail('expected a RuntimeError')
    except RuntimeError as ex:
      outer_ex = ex
    self.assertEqual(
        str(outer_ex),
        'arolla.abc.aux_bind_arguments() auxiliary binding policy has failed:'
        " 'aux_policy'",
    )
    self.assertIsInstance(outer_ex.__cause__, RuntimeError)
    self.assertEqual(
        str(outer_ex.__cause__),
        'expected tuple[QValue|Expr, ...], but .bind_arguments() returned list',
    )

  def test_py_aux_binding_policy_bind_arguments_tuple_of_non_qvalue_or_expr(
      self,
  ):
    class CustomBindingPolicy(_AuxBindingPolicy):

      def bind_arguments(self, signature, *args, **kwargs):
        return (abc_expr.placeholder('x'), abc_qtype.Unspecified(), object())

    _register_aux_binding_policy('aux_policy', CustomBindingPolicy())
    op = abc_expr.make_lambda('*x|aux_policy', abc_expr.placeholder('x'))
    try:
      _ = abc_aux_binding_policy.aux_bind_op(op, abc_qtype.Unspecified())
      self.fail('expected a RuntimeError')
    except RuntimeError as ex:
      outer_ex = ex
    self.assertEqual(
        str(outer_ex),
        'arolla.abc.aux_bind_arguments() auxiliary binding policy has failed:'
        " 'aux_policy'",
    )
    self.assertIsInstance(outer_ex.__cause__, RuntimeError)
    self.assertEqual(
        str(outer_ex.__cause__),
        'expected tuple[QValue|Expr, ...], but .bind_arguments() returned'
        ' result[2]: object',
    )

  def test_py_aux_binding_policy_bind_arguments_raises(self):
    class CustomBindingPolicy(_AuxBindingPolicy):

      def bind_arguments(self, signature, *args, **kwargs):
        raise args[0]

    _register_aux_binding_policy('aux_policy', CustomBindingPolicy())
    op = abc_expr.make_lambda('*x|aux_policy', abc_expr.placeholder('x'))
    with self.assertRaisesWithLiteralMatch(TypeError, 'bind-arguments-token'):
      _ = abc_aux_binding_policy.aux_bind_op(
          op, TypeError('bind-arguments-token')
      )
    with self.assertRaisesWithLiteralMatch(ValueError, 'bind-arguments-token'):
      _ = abc_aux_binding_policy.aux_bind_op(
          op, ValueError('bind-arguments-token')
      )
    with self.assertRaises(RuntimeError):
      _ = abc_aux_binding_policy.aux_bind_op(op, IOError())


class ClassicAuxBindingPolicyWithCustomBoxingTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    _remove_all_aux_binding_policies()

  def test_aux_inspect_signature(self):
    def as_qvalue_or_expr(_):
      raise NotImplementedError

    _register_classic_aux_binding_policy_with_custom_boxing(
        'aux_policy', as_qvalue_or_expr
    )
    op = abc_expr.make_lambda(
        ('x,y=,*z|aux_policy', abc_qtype.Unspecified()),
        abc_expr.make_operator_node(
            _make_tuple_op,
            (
                abc_expr.placeholder('x'),
                abc_expr.placeholder('y'),
                abc_expr.placeholder('z'),
            ),
        ),
    )
    expected_signature = inspect.Signature([
        inspect.Parameter('x', inspect.Parameter.POSITIONAL_OR_KEYWORD),
        inspect.Parameter(
            'y',
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            default=abc_qtype.Unspecified(),
        ),
        inspect.Parameter('z', inspect.Parameter.VAR_POSITIONAL),
    ])
    self.assertEqual(
        abc_aux_binding_policy.aux_inspect_signature(op), expected_signature
    )

  def test_bind_arguments(self):
    def as_qvalue_or_expr(_):
      raise NotImplementedError

    _register_classic_aux_binding_policy_with_custom_boxing(
        'aux_policy', as_qvalue_or_expr
    )
    a = abc_expr.leaf('a')
    b = abc_expr.leaf('b')
    c = abc_expr.leaf('c')
    u = abc_expr.literal(abc_qtype.Unspecified())
    arg0 = abc_expr.placeholder('arg0')
    arg1 = abc_expr.placeholder('arg1')
    aux_bind_op = abc_aux_binding_policy.aux_bind_op
    make_op_node = abc_expr.make_operator_node

    with self.subTest('arg0'):
      op = abc_expr.make_lambda('arg0|aux_policy', arg0)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "missing 1 required argument: 'arg0'"
      ):
        _ = aux_bind_op(op)
      self.assertEqual(
          aux_bind_op(op, a).fingerprint,
          make_op_node(op, (a,)).fingerprint,
      )
      self.assertEqual(
          aux_bind_op(op, arg0=a).fingerprint,
          make_op_node(op, (a,)).fingerprint,
      )
      with self.assertRaisesWithLiteralMatch(
          TypeError, "unexpected keyword argument: 'arg1'"
      ):
        _ = aux_bind_op(op, arg1=a)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "unexpected keyword arguments: 'arg1', 'arg2'"
      ):
        _ = aux_bind_op(op, arg1=a, arg2=b)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "got multiple values for parameter: 'arg0'"
      ):
        _ = aux_bind_op(op, a, arg0=b)
      with self.assertRaisesWithLiteralMatch(
          TypeError, 'expected 1 positional argument, got 2'
      ):
        _ = aux_bind_op(op, a, b)

    with self.subTest('arg0='):
      op = abc_expr.make_lambda(
          ('arg0=|aux_policy', abc_qtype.Unspecified()), arg0
      )
      self.assertEqual(
          aux_bind_op(op).fingerprint,
          make_op_node(op, (u,)).fingerprint,
      )
      self.assertEqual(
          aux_bind_op(op, a).fingerprint,
          make_op_node(op, (a,)).fingerprint,
      )
      self.assertEqual(
          aux_bind_op(op, arg0=a).fingerprint,
          make_op_node(op, (a,)).fingerprint,
      )
      with self.assertRaisesWithLiteralMatch(
          TypeError, "unexpected keyword argument: 'arg1'"
      ):
        _ = aux_bind_op(op, arg1=a)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "unexpected keyword arguments: 'arg1', 'arg2'"
      ):
        _ = aux_bind_op(op, arg1=a, arg2=b)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "got multiple values for parameter: 'arg0'"
      ):
        _ = aux_bind_op(op, a, arg0=b)
      with self.assertRaisesWithLiteralMatch(
          TypeError, 'expected from 0 to 1 positional arguments, got 2'
      ):
        _ = aux_bind_op(op, a, b)

    with self.subTest('arg0, arg1'):
      op = abc_expr.make_lambda(
          'arg0, arg1|aux_policy', make_op_node(_make_tuple_op, (arg0, arg1))
      )
      with self.assertRaisesWithLiteralMatch(
          TypeError, "missing 2 required arguments: 'arg0', 'arg1'"
      ):
        _ = aux_bind_op(op)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "missing 1 required argument: 'arg1'"
      ):
        _ = aux_bind_op(op, a)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "missing 1 required argument: 'arg1'"
      ):
        _ = aux_bind_op(op, arg0=a)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "missing 1 required argument: 'arg0'"
      ):
        _ = aux_bind_op(op, arg1=a)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "unexpected keyword argument: 'arg2'"
      ):
        _ = aux_bind_op(op, arg1=a, arg2=b)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "got multiple values for parameter: 'arg0'"
      ):
        _ = aux_bind_op(op, a, arg0=b)
      self.assertEqual(
          aux_bind_op(op, a, b).fingerprint,
          make_op_node(op, (a, b)).fingerprint,
      )
      with self.assertRaisesWithLiteralMatch(
          TypeError, 'expected 2 positional arguments, got 3'
      ):
        _ = aux_bind_op(op, a, b, c)

    with self.subTest('*arg0'):
      op = abc_expr.make_lambda('*arg0|aux_policy', arg0)
      self.assertEqual(
          aux_bind_op(op).fingerprint,
          make_op_node(op, ()).fingerprint,
      )
      self.assertEqual(
          aux_bind_op(op, a).fingerprint,
          make_op_node(op, (a,)).fingerprint,
      )
      with self.assertRaisesWithLiteralMatch(
          TypeError, "unexpected keyword argument: 'arg0'"
      ):
        _ = aux_bind_op(op, arg0=a)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "unexpected keyword argument: 'arg1'"
      ):
        _ = aux_bind_op(op, arg1=a)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "unexpected keyword arguments: 'arg1', 'arg2'"
      ):
        _ = aux_bind_op(op, arg1=a, arg2=b)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "unexpected keyword argument: 'arg0'"
      ):
        _ = aux_bind_op(op, a, arg0=b)
      self.assertEqual(
          aux_bind_op(op, a, b).fingerprint,
          make_op_node(op, (a, b)).fingerprint,
      )

  def test_make_literal(self):
    p_x = abc_expr.placeholder('x')
    id_op = abc_expr.make_lambda('x', p_x)
    op = abc_expr.make_lambda('x |aux_policy', p_x)

    def as_qvalue_or_expr(x):
      raise NotImplementedError

    def make_literal(x):
      return abc_expr.bind_op(id_op, x)

    _register_classic_aux_binding_policy_with_custom_boxing(
        'aux_policy', as_qvalue_or_expr, make_literal_fn=make_literal
    )
    u = abc_qtype.Unspecified()
    self.assertEqual(
        abc_aux_binding_policy.aux_bind_op(op, u).fingerprint,
        abc_expr.bind_op(op, abc_expr.bind_op(id_op, u)).fingerprint,
    )

  def test_make_literal_skips_expr(self):
    def as_qvalue_or_expr(x):
      raise NotImplementedError

    def make_literal(x):
      raise NotImplementedError

    op = abc_expr.make_lambda('x |aux_policy', abc_expr.placeholder('x'))
    _register_classic_aux_binding_policy_with_custom_boxing(
        'aux_policy', as_qvalue_or_expr, make_literal_fn=make_literal
    )
    expected_expr = abc_expr.bind_op(op, abc_qtype.Unspecified())
    self.assertEqual(
        abc_aux_binding_policy.aux_bind_op(
            op, abc_expr.literal(abc_qtype.Unspecified())
        ).fingerprint,
        expected_expr.fingerprint,
    )

  def test_make_literal_invalid_output(self):
    p_x = abc_expr.placeholder('x')
    op = abc_expr.make_lambda('x |aux_policy', p_x)

    def as_qvalue_or_expr(x):
      return x

    def make_literal(x):
      return x

    _register_classic_aux_binding_policy_with_custom_boxing(
        'aux_policy', as_qvalue_or_expr, make_literal_fn=make_literal
    )
    try:
      _ = abc_aux_binding_policy.aux_bind_op(op, abc_qtype.Unspecified())
      self.fail('expected a RuntimeError')
    except RuntimeError as ex:
      outer_ex = ex
    self.assertEqual(
        str(outer_ex), 'arolla.abc.aux_bind_op() call to make_literal() failed'
    )
    self.assertEqual(
        str(outer_ex.__cause__), 'expected arolla.abc.Expr, got Unspecified'
    )

  def test_aux_bind_op_make_literal_different_fingerprint(self):
    p_x = abc_expr.placeholder('x')
    op = abc_expr.make_lambda('x |aux_policy', p_x)

    def as_qvalue_or_expr(x):
      return x

    def make_literal(x):
      del x
      return abc_expr.literal(abc_qtype.NOTHING)

    _register_classic_aux_binding_policy_with_custom_boxing(
        'aux_policy', as_qvalue_or_expr, make_literal_fn=make_literal
    )
    try:
      _ = abc_aux_binding_policy.aux_bind_op(op, abc_qtype.Unspecified())
      self.fail('expected a RuntimeError')
    except RuntimeError as ex:
      outer_ex = ex
    self.assertEqual(
        str(outer_ex), 'arolla.abc.aux_bind_op() call to make_literal() failed'
    )
    self.assertEqual(
        str(outer_ex.__cause__),
        'make_literal(x).qvalue.fingerprint != x.fingerprint',
    )

  def test_register_aux_binding_policy_make_literal_not_a_callable_error(self):
    def as_qvalue_or_expr(x):
      return x

    make_literal = object()
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'expected Callable[[QValue], Expr] | None, got make_literal_fn: object',
    ):
      _register_classic_aux_binding_policy_with_custom_boxing(  # pytype: disable=wrong-arg-types
          'aux_policy', as_qvalue_or_expr, make_literal_fn=make_literal
      )

  def test_bind_arguments_handle_type_error(self):
    def as_qvalue_or_expr(_):
      raise TypeError('as-qvalue-or-expr-token')

    _register_classic_aux_binding_policy_with_custom_boxing(
        'aux_policy', as_qvalue_or_expr
    )
    op = abc_expr.make_lambda('x|aux_policy', abc_expr.placeholder('x'))
    with self.subTest('positional'):
      try:
        _ = abc_aux_binding_policy.aux_bind_op(op, object())
        self.fail('expected a TypeError')
      except TypeError as ex:
        outer_ex = ex
      self.assertEqual(
          str(outer_ex),
          'unable to represent as QValue or Expr: args[0]: object',
      )
      self.assertIsInstance(outer_ex.__cause__, TypeError)
      self.assertEqual(str(outer_ex.__cause__), 'as-qvalue-or-expr-token')
    with self.subTest('keyword'):
      try:
        _ = abc_aux_binding_policy.aux_bind_op(op, x=object())
        self.fail('expected a TypeError')
      except TypeError as ex:
        outer_ex = ex
      self.assertEqual(
          str(outer_ex),
          "unable to represent as QValue or Expr: kwargs['x']: object",
      )
      self.assertIsInstance(outer_ex.__cause__, TypeError)
      self.assertEqual(str(outer_ex.__cause__), 'as-qvalue-or-expr-token')

  def test_bind_arguments_handle_value_error(self):
    def as_qvalue_or_expr(_):
      raise ValueError('as-qvalue-or-expr-token')

    _register_classic_aux_binding_policy_with_custom_boxing(
        'aux_policy', as_qvalue_or_expr
    )
    op = abc_expr.make_lambda('x|aux_policy', abc_expr.placeholder('x'))
    with self.subTest('positional'):
      try:
        _ = abc_aux_binding_policy.aux_bind_op(op, object())
        self.fail('expected a ValueError')
      except ValueError as ex:
        outer_ex = ex
      self.assertEqual(
          str(outer_ex),
          'unable to represent as QValue or Expr: args[0]: object',
      )
      self.assertIsInstance(outer_ex.__cause__, ValueError)
      self.assertEqual(str(outer_ex.__cause__), 'as-qvalue-or-expr-token')
    with self.subTest('keyword'):
      try:
        _ = abc_aux_binding_policy.aux_bind_op(op, x=object())
        self.fail('expected a ValueError')
      except ValueError as ex:
        outer_ex = ex
      self.assertEqual(
          str(outer_ex),
          "unable to represent as QValue or Expr: kwargs['x']: object",
      )
      self.assertIsInstance(outer_ex.__cause__, ValueError)
      self.assertEqual(str(outer_ex.__cause__), 'as-qvalue-or-expr-token')

  def test_arguments_handle_unexpected_error(self):
    def as_qvalue_or_expr(_):
      raise NotImplementedError('as-qvalue-or-expr-token')

    _register_classic_aux_binding_policy_with_custom_boxing(
        'aux_policy', as_qvalue_or_expr
    )
    op = abc_expr.make_lambda('x|aux_policy', abc_expr.placeholder('x'))
    with self.subTest('positional'):
      try:
        _ = abc_aux_binding_policy.aux_bind_op(op, object())
        self.fail('expected a RuntimeError')
      except RuntimeError as ex:
        outer_ex = ex
      self.assertEqual(
          str(outer_ex),
          'arolla.abc.aux_bind_arguments() auxiliary binding policy has'
          " failed: 'aux_policy'",
      )
      self.assertIsInstance(outer_ex.__cause__, NotImplementedError)
      self.assertEqual(str(outer_ex.__cause__), 'as-qvalue-or-expr-token')
    with self.subTest('keyword'):
      try:
        _ = abc_aux_binding_policy.aux_bind_op(op, x=object())
        self.fail('expected a RuntimeError')
      except RuntimeError as ex:
        outer_ex = ex
      self.assertEqual(
          str(outer_ex),
          'arolla.abc.aux_bind_arguments() auxiliary binding policy has'
          " failed: 'aux_policy'",
      )
      self.assertIsInstance(outer_ex.__cause__, NotImplementedError)
      self.assertEqual(str(outer_ex.__cause__), 'as-qvalue-or-expr-token')

  def test_as_qvalue_or_expr_fn_usage(self):
    op = abc_expr.make_lambda('x|aux_policy', abc_expr.placeholder('x'))

    with self.subTest('not called for qvalue'):
      mock_as_qvalue_or_expr = mock.Mock()
      _register_classic_aux_binding_policy_with_custom_boxing(
          'aux_policy', mock_as_qvalue_or_expr
      )
      self.assertEqual(
          abc_aux_binding_policy.aux_bind_op(
              op, abc_qtype.Unspecified()
          ).fingerprint,
          abc_expr.make_operator_node(
              op, (abc_expr.literal(abc_qtype.Unspecified()),)
          ).fingerprint,
      )
      mock_as_qvalue_or_expr.assert_not_called()

    with self.subTest('not called for expr'):
      mock_as_qvalue_or_expr = mock.Mock()
      _register_classic_aux_binding_policy_with_custom_boxing(
          'aux_policy', mock_as_qvalue_or_expr
      )
      self.assertEqual(
          abc_aux_binding_policy.aux_bind_op(
              op, abc_expr.placeholder('a')
          ).fingerprint,
          abc_expr.make_operator_node(
              op, (abc_expr.placeholder('a'),)
          ).fingerprint,
      )
      mock_as_qvalue_or_expr.assert_not_called()

    with self.subTest('returns qvalue'):
      mock_as_qvalue_or_expr = mock.Mock(return_value=abc_qtype.Unspecified())
      _register_classic_aux_binding_policy_with_custom_boxing(
          'aux_policy', mock_as_qvalue_or_expr
      )
      self.assertEqual(
          abc_aux_binding_policy.aux_bind_op(op, object()).fingerprint,
          abc_expr.make_operator_node(
              op, (abc_expr.literal(abc_qtype.Unspecified()),)
          ).fingerprint,
      )
      mock_as_qvalue_or_expr.assert_called_once()

    with self.subTest('returns expr'):
      mock_as_qvalue_or_expr = mock.Mock(
          return_value=abc_expr.literal(abc_qtype.Unspecified())
      )
      _register_classic_aux_binding_policy_with_custom_boxing(
          'aux_policy', mock_as_qvalue_or_expr
      )
      self.assertEqual(
          abc_aux_binding_policy.aux_bind_op(op, object()).fingerprint,
          abc_expr.make_operator_node(
              op, (abc_expr.literal(abc_qtype.Unspecified()),)
          ).fingerprint,
      )
      mock_as_qvalue_or_expr.assert_called_once()

    with self.subTest('returns unexpected type'):
      mock_as_qvalue_or_expr = mock.Mock(return_value=object())
      _register_classic_aux_binding_policy_with_custom_boxing(
          'aux_policy', mock_as_qvalue_or_expr
      )
      try:
        _ = abc_aux_binding_policy.aux_bind_op(op, 'boo')
        self.fail('expected a RuntimeError')
      except RuntimeError as ex:
        outer_ex = ex
      self.assertEqual(
          str(outer_ex),
          'arolla.abc.aux_bind_arguments() auxiliary binding policy has'
          " failed: 'aux_policy'",
      )
      self.assertIsInstance(outer_ex.__cause__, RuntimeError)
      self.assertEqual(
          str(outer_ex.__cause__),
          'expected QValue or Expr, but as_qvalue_or_expr(arg: str) returned'
          ' object',
      )
      mock_as_qvalue_or_expr.assert_called_once()

  def test_as_qvalue_or_expr_fn_non_callable(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'expected Callable[[Any], QValue|Expr], got as_qvalue_or_expr_fn:'
        ' object',
    ):
      _register_classic_aux_binding_policy_with_custom_boxing(  # pytype: disable=wrong-arg-types
          'aux_policy', object()
      )


if __name__ == '__main__':
  absltest.main()
