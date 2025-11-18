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
  abc_aux_binding_policy.register_aux_binding_policy(
      aux_policy, policy_implementation
  )
  _registered_aux_binding_policies.add(aux_policy)


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


# Wrapper for register_adhoc_aux_binding_policy() that records the registered
# policies for cleanup between tests.
def _register_adhoc_aux_binding_policy(
    aux_policy: str,
    bind_arguments_fn: Callable[
        ..., tuple[abc_qtype.QValue | abc_expr.Expr, ...]
    ],
    *,
    make_literal_fn: Callable[[abc_qtype.QValue], abc_expr.Expr] | None = None,
):
  _registered_aux_binding_policies.add(aux_policy)
  abc_aux_binding_policy.register_adhoc_aux_binding_policy(
      aux_policy, bind_arguments_fn, make_literal_fn=make_literal_fn
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
    op = abc_expr.make_lambda('x|aux_policy:param', abc_expr.placeholder('x'))
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
    op = abc_expr.make_lambda('x|aux_policy:param', abc_expr.placeholder('x'))

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
        'x, unused_y|aux_policy:param', abc_expr.placeholder('x')
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

    op = abc_expr.make_lambda('x |aux_policy:param', abc_expr.placeholder('x'))
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

    op = abc_expr.make_lambda('x |aux_policy:param', abc_expr.placeholder('x'))
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

    op = abc_expr.make_lambda('x |aux_policy:param', abc_expr.placeholder('x'))
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
        str(outer_ex.__cause__),
        'expected arolla.abc.expr.Expr, got Unspecified',
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

  def test_aux_bind_arguments(self):
    class CustomBindingPolicy(_AuxBindingPolicy):

      def bind_arguments(self, sig, *args, **kwargs):
        assert signature == sig
        assert args == (1, 2, 3)
        assert kwargs == {'c': 'd', 'e': 'f'}
        return (abc_expr.placeholder('z'), abc_qtype.Unspecified())

    _register_aux_binding_policy('aux_policy', CustomBindingPolicy())
    signature = abc_signature.make_operator_signature('x, y|aux_policy:param')
    bound_args = abc_aux_binding_policy.aux_bind_arguments(
        signature, 1, 2, 3, c='d', e='f'
    )
    self.assertLen(bound_args, 2)
    self.assertTrue(
        bound_args[0].fingerprint, abc_expr.placeholder('x').fingerprint
    )
    self.assertTrue(
        bound_args[1].fingerprint, abc_qtype.Unspecified().fingerprint
    )

  def test_aux_bind_arguments_signature(self):
    self.assertEqual(
        inspect.signature(clib.aux_bind_arguments),
        inspect.signature(lambda signature, /, *args, **kwargs: None),
    )

  def test_aux_bind_arguments_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.aux_bind_arguments() missing 1 required positional'
        " argument: 'signature'",
    ):
      _ = abc_aux_binding_policy.aux_bind_arguments()  # pytype: disable=missing-parameter
    try:
      _ = abc_aux_binding_policy.aux_bind_arguments(b'signature')  # pytype: disable=wrong-arg-types
      self.fail('expected TypeError')
    except TypeError as ex:
      outer_ex = ex
    self.assertEqual(
        str(outer_ex), 'arolla.abc.aux_bind_arguments() got invalid signature'
    )
    self.assertIsInstance(outer_ex.__cause__, TypeError)
    self.assertEqual(str(outer_ex.__cause__), 'expected a signature, got bytes')

  def test_aux_bind_arguments_runtime_error(self):
    class CustomBindingPolicy(_AuxBindingPolicy):

      def bind_arguments(self, sig, *args, **kwargs):
        del sig, args, kwargs
        return None

    _register_aux_binding_policy('aux_policy', CustomBindingPolicy())
    signature = abc_signature.make_operator_signature('x, y|aux_policy:param')
    with self.assertRaisesWithLiteralMatch(
        RuntimeError,
        'arolla.abc.aux_bind_arguments() auxiliary binding policy has failed:'
        " 'aux_policy:param'",
    ):
      _ = abc_aux_binding_policy.aux_bind_arguments(signature)

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

  def test_register_aux_binding_policy_value_error(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        "aux_policy_name contains a `:` character: 'name:param'",
    ):
      abc_aux_binding_policy.register_aux_binding_policy(
          'name:param', _AuxBindingPolicy()
      )

  def test_register_aux_binding_policy_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'expected an AuxBindingPolicy, got policy_implementation: object',
    ):
      _register_aux_binding_policy('', object())  # pytype: disable=wrong-arg-types

  def test_py_aux_binding_policy_bind_arguments_non_tuple_result(self):
    class CustomBindingPolicy(_AuxBindingPolicy):

      def bind_arguments(self, signature, *args, **kwargs):
        return object()

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
        'expected tuple[QValue|Expr, ...], but .bind_arguments() returned'
        ' object',
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

  def test_remove_aux_policy_value_error(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        "aux_policy_name contains a `:` character: 'name:param'",
    ):
      abc_aux_binding_policy.remove_aux_binding_policy('name:param')


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
          TypeError, "missing 1 required positional argument: 'arg0'"
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
          TypeError, "missing 1 required positional argument: 'arg0'"
      ):
        _ = aux_bind_op(op, arg1=a)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "missing 1 required positional argument: 'arg0'"
      ):
        _ = aux_bind_op(op, arg1=a, arg2=b)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "multiple values for argument 'arg0'"
      ):
        _ = aux_bind_op(op, a, arg0=b)
      with self.assertRaisesWithLiteralMatch(
          TypeError, 'takes 1 positional argument but 2 were given'
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
          TypeError, "an unexpected keyword argument: 'arg1'"
      ):
        _ = aux_bind_op(op, arg1=a)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "an unexpected keyword argument: 'arg1'"
      ):
        _ = aux_bind_op(op, arg1=a, arg2=b)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "multiple values for argument 'arg0'"
      ):
        _ = aux_bind_op(op, a, arg0=b)
      with self.assertRaisesWithLiteralMatch(
          TypeError, 'takes from 0 to 1 positional arguments but 2 were given'
      ):
        _ = aux_bind_op(op, a, b)

    with self.subTest('arg0, arg1'):
      op = abc_expr.make_lambda(
          'arg0, arg1|aux_policy', make_op_node(_make_tuple_op, (arg0, arg1))
      )
      with self.assertRaisesWithLiteralMatch(
          TypeError,
          "missing 2 required positional arguments: 'arg0' and 'arg1'",
      ):
        _ = aux_bind_op(op)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "missing 1 required positional argument: 'arg1'"
      ):
        _ = aux_bind_op(op, a)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "missing 1 required positional argument: 'arg1'"
      ):
        _ = aux_bind_op(op, arg0=a)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "missing 1 required positional argument: 'arg0'"
      ):
        _ = aux_bind_op(op, arg1=a)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "missing 1 required positional argument: 'arg0'"
      ):
        _ = aux_bind_op(op, arg1=a, arg2=b)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "multiple values for argument 'arg0'"
      ):
        _ = aux_bind_op(op, a, arg0=b)
      self.assertEqual(
          aux_bind_op(op, a, b).fingerprint,
          make_op_node(op, (a, b)).fingerprint,
      )
      with self.assertRaisesWithLiteralMatch(
          TypeError, 'takes 2 positional arguments but 3 were given'
      ):
        _ = aux_bind_op(op, a, b, c)

    with self.subTest('*arg0'):
      op = abc_expr.make_lambda('*arg0|aux_policy', arg0)
      self.assertEqual(
          aux_bind_op(op).fingerprint,
          make_op_node(op, ()).fingerprint,
          f'{aux_bind_op(op)}',
      )
      self.assertEqual(
          aux_bind_op(op, a).fingerprint,
          make_op_node(op, (a,)).fingerprint,
      )
      with self.assertRaisesWithLiteralMatch(
          TypeError, "an unexpected keyword argument: 'arg0'"
      ):
        _ = aux_bind_op(op, arg0=a)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "an unexpected keyword argument: 'arg1'"
      ):
        _ = aux_bind_op(op, arg1=a)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "an unexpected keyword argument: 'arg1'"
      ):
        _ = aux_bind_op(op, arg1=a, arg2=b)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "an unexpected keyword argument: 'arg0'"
      ):
        _ = aux_bind_op(op, a, arg0=b)
      self.assertEqual(
          aux_bind_op(op, a, b).fingerprint,
          make_op_node(op, (a, b)).fingerprint,
      )

    with self.subTest('arg0, *arg1'):
      op = abc_expr.make_lambda('arg0, *arg1|aux_policy', arg0)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "missing 1 required positional argument: 'arg0'"
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
          TypeError, "missing 1 required positional argument: 'arg0'"
      ):
        _ = aux_bind_op(op, arg1=a)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "missing 1 required positional argument: 'arg0'"
      ):
        _ = aux_bind_op(op, arg1=a, arg2=b)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "multiple values for argument 'arg0'"
      ):
        _ = aux_bind_op(op, a, arg0=b)
      self.assertEqual(
          aux_bind_op(op, a, b).fingerprint,
          make_op_node(op, (a, b)).fingerprint,
      )

    with self.subTest('arg0, arg1, arg2'):
      op = abc_expr.make_lambda('arg0, arg1, arg2|aux_policy', arg0)
      with self.assertRaisesWithLiteralMatch(
          TypeError,
          "missing 3 required positional arguments: 'arg0', 'arg1' and 'arg2'",
      ):
        _ = aux_bind_op(op)
      with self.assertRaisesWithLiteralMatch(
          TypeError,
          "missing 2 required positional arguments: 'arg1' and 'arg2'",
      ):
        _ = aux_bind_op(op, a)
      with self.assertRaisesWithLiteralMatch(
          TypeError,
          "missing 2 required positional arguments: 'arg1' and 'arg2'",
      ):
        _ = aux_bind_op(op, arg0=a)
      with self.assertRaisesWithLiteralMatch(
          TypeError,
          "missing 2 required positional arguments: 'arg0' and 'arg2'",
      ):
        _ = aux_bind_op(op, arg1=a)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "missing 1 required positional argument: 'arg0'"
      ):
        _ = aux_bind_op(op, arg1=a, arg2=b)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "multiple values for argument 'arg0'"
      ):
        _ = aux_bind_op(op, a, arg0=b)
      with self.assertRaisesWithLiteralMatch(
          TypeError, "missing 1 required positional argument: 'arg2'"
      ):
        _ = aux_bind_op(op, a, b)
      self.assertEqual(
          aux_bind_op(op, a, b, c).fingerprint,
          make_op_node(op, (a, b, c)).fingerprint,
      )

  def test_make_literal(self):
    p_x = abc_expr.placeholder('x')
    id_op = abc_expr.make_lambda('x', p_x)
    op = abc_expr.make_lambda('x |aux_policy', p_x)

    def as_qvalue_or_expr(_):
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
    def as_qvalue_or_expr(_):
      raise NotImplementedError

    def make_literal(_):
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
        str(outer_ex.__cause__),
        'expected arolla.abc.expr.Expr, got Unspecified',
    )

  def test_make_literal_raises(self):
    p_x = abc_expr.placeholder('x')
    op = abc_expr.make_lambda('x |aux_policy', p_x)

    def as_qvalue_or_expr(x):
      return x

    def make_literal(_):
      raise NotImplementedError

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
    self.assertIsInstance(outer_ex.__cause__, NotImplementedError)

  def test_register_aux_binding_policy_make_literal_not_a_callable_error(self):
    def as_qvalue_or_expr(x):
      return x

    make_literal = object()
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'expected Callable[[QValue], Expr], got make_literal_fn: object',
    ):
      _register_classic_aux_binding_policy_with_custom_boxing(  # pytype: disable=wrong-arg-types
          'aux_policy', as_qvalue_or_expr, make_literal_fn=make_literal
      )

  def test_bind_arguments_handle_error(self):
    def as_qvalue_or_expr(_):
      raise TypeError('as-qvalue-or-expr-token')

    _register_classic_aux_binding_policy_with_custom_boxing(
        'aux_policy', as_qvalue_or_expr
    )
    op = abc_expr.make_lambda('x|aux_policy', abc_expr.placeholder('x'))
    with self.subTest('positional'):
      with self.assertRaisesWithLiteralMatch(
          TypeError, 'as-qvalue-or-expr-token'
      ) as cm:
        _ = abc_aux_binding_policy.aux_bind_op(op, object())
      self.assertEqual(
          cm.exception.__notes__,
          ['Error occurred while processing argument: `x`'],
      )
    with self.subTest('keyword'):
      with self.assertRaisesWithLiteralMatch(
          TypeError, 'as-qvalue-or-expr-token'
      ) as cm:
        _ = abc_aux_binding_policy.aux_bind_op(op, x=object())
      self.assertEqual(
          cm.exception.__notes__,
          ['Error occurred while processing argument: `x`'],
      )

  def test_arguments_handle_unexpected_error(self):
    def as_qvalue_or_expr(_):
      raise NotImplementedError('as-qvalue-or-expr-token')

    _register_classic_aux_binding_policy_with_custom_boxing(
        'aux_policy', as_qvalue_or_expr
    )
    op = abc_expr.make_lambda('x|aux_policy', abc_expr.placeholder('x'))
    with self.subTest('positional'):
      with self.assertRaisesWithLiteralMatch(
          RuntimeError,
          'arolla.abc.aux_bind_arguments() auxiliary binding policy has failed:'
          " 'aux_policy'",
      ) as cm:
        _ = abc_aux_binding_policy.aux_bind_op(op, object())
      self.assertIsInstance(cm.exception.__cause__, NotImplementedError)
      self.assertEqual(str(cm.exception.__cause__), 'as-qvalue-or-expr-token')
      self.assertEqual(
          cm.exception.__cause__.__notes__,  # pytype: disable=attribute-error
          ['Error occurred while processing argument: `x`'],
      )

    with self.subTest('keyword'):
      with self.assertRaisesWithLiteralMatch(
          RuntimeError,
          'arolla.abc.aux_bind_arguments() auxiliary binding policy has'
          " failed: 'aux_policy'",
      ) as cm:
        _ = abc_aux_binding_policy.aux_bind_op(op, x=object())
      self.assertIsInstance(cm.exception.__cause__, NotImplementedError)
      self.assertEqual(str(cm.exception.__cause__), 'as-qvalue-or-expr-token')
      self.assertEqual(
          cm.exception.__cause__.__notes__,  # pytype: disable=attribute-error
          ['Error occurred while processing argument: `x`'],
      )

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

  def test_as_qvalue_or_expr_fn_errors(self):
    op = abc_expr.make_lambda('x, *y|aux_policy', abc_expr.placeholder('x'))
    mock_as_qvalue_or_expr = mock.Mock(return_value=object())
    _register_classic_aux_binding_policy_with_custom_boxing(
        'aux_policy', mock_as_qvalue_or_expr
    )

    mock_as_qvalue_or_expr.side_effect = TypeError('unsupported type')
    with self.assertRaisesWithLiteralMatch(TypeError, 'unsupported type') as cm:
      _ = abc_aux_binding_policy.aux_bind_op(op, 'boom!')
    self.assertEqual(
        cm.exception.__notes__,
        ['Error occurred while processing argument: `x`'],
    )

    mock_as_qvalue_or_expr.side_effect = [
        mock.DEFAULT,
        TypeError('unsupported type'),
    ]
    mock_as_qvalue_or_expr.return_value = abc_qtype.unspecified()
    with self.assertRaisesWithLiteralMatch(TypeError, 'unsupported type') as cm:
      _ = abc_aux_binding_policy.aux_bind_op(op, 'ok', 'boom!')
    self.assertEqual(
        cm.exception.__notes__,
        ['Error occurred while processing argument: `y[0]`'],
    )

    mock_as_qvalue_or_expr.side_effect = [
        mock.DEFAULT,
        mock.DEFAULT,
        TypeError('unsupported type'),
    ]
    mock_as_qvalue_or_expr.return_value = abc_qtype.unspecified()
    with self.assertRaisesWithLiteralMatch(TypeError, 'unsupported type') as cm:
      _ = abc_aux_binding_policy.aux_bind_op(op, 'ok', 'ok', 'boom!')
    self.assertEqual(
        cm.exception.__notes__,
        ['Error occurred while processing argument: `y[1]`'],
    )

  def test_as_qvalue_or_expr_fn_non_callable(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'expected Callable[[Any], QValue|Expr], got as_qvalue_or_expr_fn:'
        ' object',
    ):
      _register_classic_aux_binding_policy_with_custom_boxing(  # pytype: disable=wrong-arg-types
          'aux_policy', object()
      )

  def test_aux_policy_name_contains_colon(self):
    def as_qvalue_or_expr(_):
      raise NotImplementedError

    with self.assertRaisesWithLiteralMatch(
        ValueError,
        "aux_policy_name contains a `:` character: 'name:param'",
    ):
      abc_aux_binding_policy.register_classic_aux_binding_policy_with_custom_boxing(
          'name:param', as_qvalue_or_expr
      )


class AdhocAuxBindingPolicyTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    _remove_all_aux_binding_policies()

  def test_make_python_signature(self):
    def bind_arguments(a, *b, c=1, **d):
      del a, b, c, d
      raise NotImplementedError

    _register_adhoc_aux_binding_policy('aux_policy', bind_arguments)
    op = abc_expr.make_lambda(
        'x,y|aux_policy',
        abc_expr.make_operator_node(
            _make_tuple_op,
            (abc_expr.placeholder('x'), abc_expr.placeholder('y')),
        ),
    )
    self.assertEqual(
        abc_aux_binding_policy.aux_inspect_signature(op),
        inspect.signature(bind_arguments),
    )

  def test_bind_arguments(self):
    def bind_arguments(a, *b, c=abc_qtype.unspecified()):
      return (a, *b, c)

    _register_adhoc_aux_binding_policy('aux_policy', bind_arguments)
    sig = abc_signature.make_operator_signature('*args|aux_policy')
    self.assertEqual(
        abc_aux_binding_policy.aux_bind_arguments(
            sig, abc_qtype.QTYPE, c=abc_qtype.unspecified()
        ),
        (abc_qtype.QTYPE, abc_qtype.unspecified()),
    )
    self.assertEqual(
        repr(
            abc_aux_binding_policy.aux_bind_arguments(
                sig,
                abc_qtype.QTYPE,
                abc_expr.leaf('x'),
                c=abc_qtype.unspecified(),
            )
        ),
        '(QTYPE, L.x, unspecified)',
    )

  def test_bind_arguments_error_missing_argument(self):
    def bind_arguments(a):
      return (a,)

    _register_adhoc_aux_binding_policy('aux_policy', bind_arguments)
    sig = abc_signature.make_operator_signature('*args|aux_policy')
    with self.assertRaisesRegex(
        TypeError, re.escape("missing 1 required positional argument: 'a'")
    ):
      _ = abc_aux_binding_policy.aux_bind_arguments(sig)

  def test_bind_arguments_unexpected_error(self):
    def bind_arguments():
      raise NotImplementedError('Boom!')

    _register_adhoc_aux_binding_policy('aux_policy', bind_arguments)
    sig = abc_signature.make_operator_signature('*args|aux_policy')
    try:
      _ = abc_aux_binding_policy.aux_bind_arguments(sig)
      self.fail('expected a RuntimeError')
    except RuntimeError as ex:
      outer_ex = ex
    self.assertEqual(
        str(outer_ex),
        'arolla.abc.aux_bind_arguments() auxiliary binding policy has failed:'
        " 'aux_policy'",
    )
    self.assertIsInstance(outer_ex.__cause__, NotImplementedError)
    self.assertEqual(str(outer_ex.__cause__), 'Boom!')

  def test_bind_arguments_keyboard_interrupt_error(self):
    def bind_arguments():
      raise KeyboardInterrupt('Boom!')

    _register_adhoc_aux_binding_policy('aux_policy', bind_arguments)
    sig = abc_signature.make_operator_signature('*args|aux_policy')
    with self.assertRaisesRegex(KeyboardInterrupt, re.escape('Boom!')):
      _ = abc_aux_binding_policy.aux_bind_arguments(sig)

  def test_bind_arguments_non_tuple_result(self):
    def bind_arguments():
      return object()

    _register_adhoc_aux_binding_policy('aux_policy', bind_arguments)
    sig = abc_signature.make_operator_signature('*args|aux_policy')
    try:
      _ = abc_aux_binding_policy.aux_bind_arguments(sig)
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
        ' object',
    )

  def test_bind_arguments_tuple_of_non_qvalue_or_expr(
      self,
  ):
    def bind_arguments():
      return (abc_expr.placeholder('x'), abc_qtype.Unspecified(), object())

    _register_adhoc_aux_binding_policy('aux_policy', bind_arguments)
    sig = abc_signature.make_operator_signature('*args|aux_policy')
    try:
      _ = abc_aux_binding_policy.aux_bind_arguments(sig)
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

  def test_make_literal_default(self):
    def bind_arguments(a, *b, c=abc_qtype.unspecified()):
      return (a, *b, c)

    _register_adhoc_aux_binding_policy('aux_policy', bind_arguments)
    op = abc_expr.make_lambda('*x|aux_policy', abc_expr.placeholder('x'))
    expected_expr = abc_expr.make_operator_node(
        op,
        (abc_expr.placeholder('x'), abc_expr.literal(abc_qtype.unspecified())),
    )
    self.assertEqual(
        abc_aux_binding_policy.aux_bind_op(
            op, abc_expr.placeholder('x'), c=abc_qtype.unspecified()
        ).fingerprint,
        expected_expr.fingerprint,
    )

  def test_make_literal_custom(self):
    id_op = abc_expr.make_lambda('x', abc_expr.placeholder('x'))

    def make_literal(x):
      return abc_expr.bind_op(id_op, x)

    def bind_arguments(a, *b, c=abc_qtype.unspecified()):
      return (a, *b, c)

    _register_adhoc_aux_binding_policy(
        'aux_policy', bind_arguments, make_literal_fn=make_literal
    )
    op = abc_expr.make_lambda('*x|aux_policy', abc_expr.placeholder('x'))
    expected_expr = abc_expr.make_operator_node(
        op,
        (abc_expr.placeholder('x'), make_literal(abc_qtype.unspecified())),
    )
    self.assertEqual(
        abc_aux_binding_policy.aux_bind_op(
            op, abc_expr.placeholder('x'), c=abc_qtype.unspecified()
        ).fingerprint,
        expected_expr.fingerprint,
    )

  def test_make_literal_value_error(self):

    def make_literal(_):
      raise ValueError('Boom!')

    def bind_arguments(*args):
      return args

    _register_adhoc_aux_binding_policy(
        'aux_policy', bind_arguments, make_literal_fn=make_literal
    )
    op = abc_expr.make_lambda('*x|aux_policy', abc_expr.placeholder('x'))
    with self.assertRaisesWithLiteralMatch(ValueError, 'Boom!'):
      _ = abc_aux_binding_policy.aux_bind_op(op, abc_qtype.unspecified())

  def test_make_literal_fails(self):

    def make_literal(_):
      raise TypeError('Boom!')

    def bind_arguments(*args):
      return args

    _register_adhoc_aux_binding_policy(
        'aux_policy', bind_arguments, make_literal_fn=make_literal
    )
    op = abc_expr.make_lambda('*x|aux_policy', abc_expr.placeholder('x'))
    try:
      _ = abc_aux_binding_policy.aux_bind_op(op, abc_qtype.unspecified())
      self.fail('expected a RuntimeError')
    except RuntimeError as ex:
      outer_ex = ex
    self.assertEqual(
        str(outer_ex), 'arolla.abc.aux_bind_op() call to make_literal() failed'
    )
    self.assertIsInstance(outer_ex.__cause__, TypeError)
    self.assertEqual(str(outer_ex.__cause__), 'Boom!')

  def test_make_literal_keyboard_interrupt_error(self):
    def make_literal(_):
      raise KeyboardInterrupt('Boom!')

    def bind_arguments(*args):
      return args

    _register_adhoc_aux_binding_policy(
        'aux_policy', bind_arguments, make_literal_fn=make_literal
    )
    op = abc_expr.make_lambda('*x|aux_policy', abc_expr.placeholder('x'))
    with self.assertRaisesWithLiteralMatch(KeyboardInterrupt, 'Boom!'):
      _ = abc_aux_binding_policy.aux_bind_op(op, abc_qtype.unspecified())

  def test_make_literal_returns_non_expr(self):

    def make_literal(x):
      return x

    def bind_arguments(*args):
      return args

    _register_adhoc_aux_binding_policy(
        'aux_policy', bind_arguments, make_literal_fn=make_literal
    )
    op = abc_expr.make_lambda('*x|aux_policy', abc_expr.placeholder('x'))
    try:
      _ = abc_aux_binding_policy.aux_bind_op(op, abc_qtype.unspecified())
      self.fail('expected a RuntimeError')
    except RuntimeError as ex:
      outer_ex = ex
    self.assertEqual(
        str(outer_ex), 'arolla.abc.aux_bind_op() call to make_literal() failed'
    )
    self.assertIsInstance(outer_ex.__cause__, TypeError)
    self.assertEqual(
        str(outer_ex.__cause__),
        'expected arolla.abc.expr.Expr, got Unspecified',
    )

  def test_aux_policy_name_using_operator(self):
    def bind_arguments(*x):
      return x

    op = abc_expr.make_lambda(
        '*y|test_aux_policy_name_using_operator', abc_expr.placeholder('y')
    )
    abc_aux_binding_policy.register_adhoc_aux_binding_policy(op, bind_arguments)
    self.assertEqual(
        abc_aux_binding_policy.aux_inspect_signature(op),
        inspect.signature(lambda *x: None),
    )

  def test_aux_policy_name_error(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError, '"ad hoc" aux_policy_name cannot be empty'
    ):
      _register_adhoc_aux_binding_policy('', lambda: None)
    with self.assertRaisesWithLiteralMatch(
        ValueError, '"ad hoc" aux_policy_name cannot contain `:`: \'abc:xyz\''
    ):
      abc_aux_binding_policy.register_adhoc_aux_binding_policy(
          'abc:xyz', lambda: None
      )

  def test_bind_arguments_fn_non_callable(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'expected Callable[..., QValue|Expr], got bind_arguments_fn: object',
    ):
      _register_adhoc_aux_binding_policy(  # pytype: disable=wrong-arg-types
          'aux_policy', object()
      )

  def test_make_literal_fn_non_callable(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'expected Callable[[QValue], Expr], got make_literal_fn: object',
    ):
      _register_adhoc_aux_binding_policy(  # pytype: disable=wrong-arg-types
          'aux_policy', lambda: None, make_literal_fn=object()
      )


if __name__ == '__main__':
  absltest.main()
