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

"""Tests for arolla.abc.Signature."""

import inspect
import re

from absl.testing import absltest
from arolla.abc import clib
from arolla.abc import expr as abc_expr
from arolla.abc import qtype as abc_qtype
from arolla.abc import signature as abc_signature
from arolla.abc import testing_clib


class SignatureTest(absltest.TestCase):

  def test_basics(self):
    sig_as_tuple = (
        (
            ('a', 'positional-only', abc_qtype.Unspecified()),
            ('b', 'positional-or-keyword', abc_qtype.QTYPE),
            ('c', 'variadic-positional', None),
            ('d', 'keyword-only', abc_qtype.NOTHING),
            ('e', 'variadic-keyword', None),
        ),
        'aux-policy',
    )
    sig = testing_clib.python_c_api_signature_from_signature(sig_as_tuple)
    self.assertIsInstance(sig, abc_signature.Signature)
    self.assertEqual(sig, sig_as_tuple)
    self.assertLen(sig.parameters, 5)
    self.assertEqual(sig.parameters[0].name, 'a')
    self.assertEqual(sig.parameters[1].kind, 'positional-or-keyword')
    self.assertIsNone(sig.parameters[2].default)
    self.assertEqual(sig.parameters[3].default, abc_qtype.NOTHING)
    self.assertEqual(sig.aux_policy, 'aux-policy')
    self.assertEqual(
        repr(sig),
        "arolla.abc.Signature(parameters=(arolla.abc.SignatureParameter(name='a',"
        " kind='positional-only', default=unspecified),"
        " arolla.abc.SignatureParameter(name='b', kind='positional-or-keyword',"
        " default=QTYPE), arolla.abc.SignatureParameter(name='c',"
        " kind='variadic-positional', default=None),"
        " arolla.abc.SignatureParameter(name='d', kind='keyword-only',"
        " default=NOTHING), arolla.abc.SignatureParameter(name='e',"
        " kind='variadic-keyword', default=None)), aux_policy='aux-policy')",
    )

  def test_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected a signature, got list'
    ):
      _ = testing_clib.python_c_api_signature_from_signature([])
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'expected tuple[SignatureParameter, ...], got signature.parameters:'
        ' list',
    ):
      _ = testing_clib.python_c_api_signature_from_signature(([], ''))
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected a string, got signature.aux_policy: bytes'
    ):
      _ = testing_clib.python_c_api_signature_from_signature(
          ((), b'aux-policy')
      )
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected a parameter, got signature.parameters[1]: str'
    ):
      _ = testing_clib.python_c_api_signature_from_signature((
          (('param0', 'positional-or-keyword', None), 'param1'),
          '',
      ))
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'expected a string, got signature.parameters[0].name: bytes',
    ):
      _ = testing_clib.python_c_api_signature_from_signature((
          ((b'param0', 'positional-or-keyword', None),),
          '',
      ))
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'expected a string, got signature.parameters[0].kind: bytes',
    ):
      _ = testing_clib.python_c_api_signature_from_signature((
          (('param0', b'positional-or-keyword', None),),
          '',
      ))
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'expected QValue|None, got signature.parameters[0].default: int',
    ):
      _ = testing_clib.python_c_api_signature_from_signature((
          (('param0', 'positional-or-keyword', 1),),
          '',
      ))

  def test_value_error(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'expected len(signature)=2, got 1'
    ):
      _ = testing_clib.python_c_api_signature_from_signature(((),))
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'expected len(signature.parameters[0])=3, got 0'
    ):
      _ = testing_clib.python_c_api_signature_from_signature((((),), ''))


class OperatorSignatureTest(absltest.TestCase):

  def test_basics(self):
    sig_as_tuple = (
        (
            ('x', 'positional-or-keyword', None),
            ('y', 'positional-or-keyword', abc_qtype.Unspecified()),
            ('z', 'variadic-positional', None),
        ),
        'aux-policy',
    )
    sig = testing_clib.python_c_api_operator_signature_from_operator_signature(
        sig_as_tuple
    )
    self.assertIsInstance(sig, abc_signature.Signature)
    self.assertEqual(sig, sig_as_tuple)
    self.assertEqual(
        repr(sig),
        "arolla.abc.Signature(parameters=(arolla.abc.SignatureParameter(name='x',"
        " kind='positional-or-keyword', default=None),"
        " arolla.abc.SignatureParameter(name='y', kind='positional-or-keyword',"
        " default=unspecified), arolla.abc.SignatureParameter(name='z',"
        " kind='variadic-positional', default=None)), aux_policy='aux-policy')",
    )

  def test_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected a signature, got list'
    ):
      _ = testing_clib.python_c_api_operator_signature_from_operator_signature(
          []
      )
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'expected tuple[SignatureParameter, ...], got signature.parameters:'
        ' list',
    ):
      _ = testing_clib.python_c_api_operator_signature_from_operator_signature(
          ([], '')
      )
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected a string, got signature.aux_policy: bytes'
    ):
      _ = testing_clib.python_c_api_operator_signature_from_operator_signature(
          ((), b'aux-policy')
      )
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected a parameter, got signature.parameters[1]: str'
    ):
      _ = testing_clib.python_c_api_operator_signature_from_operator_signature((
          (('param0', 'positional-or-keyword', None), 'param1'),
          '',
      ))
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'expected a string, got signature.parameters[0].name: bytes',
    ):
      _ = testing_clib.python_c_api_operator_signature_from_operator_signature((
          ((b'param0', 'positional-or-keyword', None),),
          '',
      ))
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'expected a string, got signature.parameters[0].kind: bytes',
    ):
      _ = testing_clib.python_c_api_operator_signature_from_operator_signature((
          (('param0', b'positional-or-keyword', None),),
          '',
      ))
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'expected QValue|None, got signature.parameters[0].default: int',
    ):
      _ = testing_clib.python_c_api_operator_signature_from_operator_signature((
          (('param0', 'positional-or-keyword', 1),),
          '',
      ))

  def test_value_error(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'expected len(signature)=2, got 1'
    ):
      _ = testing_clib.python_c_api_operator_signature_from_operator_signature((
          (),
      ))
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'expected len(signature.parameters[0])=3, got 0'
    ):
      _ = testing_clib.python_c_api_operator_signature_from_operator_signature(
          (((),), '')
      )
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        "expected 'positional-or-keyword' or 'variadic-positional', got"
        " signature.parameters[0].kind='kind\\n'",
    ):
      _ = testing_clib.python_c_api_operator_signature_from_operator_signature(
          ((('name', 'kind\n', None),), '')
      )


class GetOperatorSignatureTest(absltest.TestCase):

  def test_basics(self):
    sig_as_tuple = (
        (
            ('x', 'positional-or-keyword', None),
            ('unused_y', 'positional-or-keyword', abc_qtype.Unspecified()),
            ('unused_z', 'variadic-positional', None),
        ),
        'aux-policy',
    )
    op = clib.internal_make_lambda(
        'op-name', sig_as_tuple, abc_expr.placeholder('x'), 'doc'
    )
    sig = abc_signature.get_operator_signature(op)
    self.assertIsInstance(sig, abc_signature.Signature)
    self.assertEqual(sig, sig_as_tuple)

  def test_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected an operator, got object'
    ):
      _ = abc_signature.get_operator_signature(object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected an operator, got UNSPECIFIED'
    ):
      _ = abc_signature.get_operator_signature(abc_qtype.Unspecified())

  def test_value_error(self):
    with self.assertRaises(ValueError):
      op = abc_expr.unsafe_make_registered_operator('missing.operator')
      _ = abc_signature.get_operator_signature(op)


class MakeOperatorSignatureTest(absltest.TestCase):

  def test_idempotence(self):
    sig = abc_signature.make_operator_signature('')
    self.assertIs(abc_signature.make_operator_signature(sig), sig)

  def test_from_str(self):
    with self.subTest('empty'):
      sig = abc_signature.make_operator_signature('')
      self.assertIsInstance(sig, abc_signature.Signature)
      self.assertEmpty(sig.parameters)
      self.assertEmpty(sig.aux_policy)
    with self.subTest('x,y,z,*args'):
      sig = abc_signature.make_operator_signature('x,y,z,*args | policy')
      self.assertIsInstance(sig, abc_signature.Signature)
      self.assertLen(sig.parameters, 4)
      self.assertEqual(sig.parameters[0].name, 'x')
      self.assertEqual(sig.parameters[0].kind, 'positional-or-keyword')
      self.assertIsNone(sig.parameters[0].default)
      self.assertEqual(sig.parameters[1].name, 'y')
      self.assertEqual(sig.parameters[1].kind, 'positional-or-keyword')
      self.assertIsNone(sig.parameters[1].default)
      self.assertEqual(sig.parameters[2].name, 'z')
      self.assertEqual(sig.parameters[2].kind, 'positional-or-keyword')
      self.assertIsNone(sig.parameters[2].default)
      self.assertEqual(sig.parameters[3].name, 'args')
      self.assertEqual(sig.parameters[3].kind, 'variadic-positional')
      self.assertIsNone(sig.parameters[3].default)
      self.assertEqual(sig.aux_policy, 'policy')
    with self.subTest('signature_spec_error'):
      with self.assertRaisesRegex(
          ValueError, re.escape('variadic parameter must be the last')
      ):
        _ = abc_signature.make_operator_signature('*x, *y')

  def test_from_tuple(self):
    with self.subTest('empty'):
      sig = abc_signature.make_operator_signature('')
      self.assertIsInstance(sig, abc_signature.Signature)
      self.assertEmpty(sig.parameters)
      self.assertEmpty(sig.aux_policy)
    with self.subTest('x,y=,z=,*args'):
      defaults = [abc_qtype.QTYPE, abc_qtype.NOTHING]
      sig = abc_signature.make_operator_signature(
          ('x,y=,z=,*args | policy', *defaults)
      )
      self.assertIsInstance(sig, abc_signature.Signature)
      self.assertLen(sig.parameters, 4)
      self.assertEqual(sig.parameters[0].name, 'x')
      self.assertEqual(sig.parameters[0].kind, 'positional-or-keyword')
      self.assertIsNone(sig.parameters[0].default)
      self.assertEqual(sig.parameters[1].name, 'y')
      self.assertEqual(sig.parameters[1].kind, 'positional-or-keyword')
      self.assertEqual(sig.parameters[1].default, defaults[0])
      self.assertEqual(sig.parameters[2].name, 'z')
      self.assertEqual(sig.parameters[2].kind, 'positional-or-keyword')
      self.assertEqual(sig.parameters[2].default, defaults[1])
      self.assertEqual(sig.parameters[3].name, 'args')
      self.assertEqual(sig.parameters[3].kind, 'variadic-positional')
      self.assertIsNone(sig.parameters[3].default)
      self.assertEqual(sig.aux_policy, 'policy')
    with self.subTest('signature_spec_error'):
      with self.assertRaisesRegex(
          ValueError, re.escape('variadic parameter must be the last')
      ):
        _ = abc_signature.make_operator_signature(('*x, *y',))
    with self.subTest('as_qvalue_error'):
      with self.assertRaisesRegex(
          TypeError, re.escape('unable to represent (int) 1 as QValue')
      ):
        _ = abc_signature.make_operator_signature(('x=', 1))

  def test_from_inspect_signature(self):
    with self.subTest('empty'):
      sig = abc_signature.make_operator_signature(
          inspect.signature(lambda: None)
      )
      self.assertIsInstance(sig, abc_signature.Signature)
      self.assertEmpty(sig.parameters)
      self.assertEmpty(sig.aux_policy)

    with self.subTest('x,y=,z=,*args'):
      sig = abc_signature.make_operator_signature(
          inspect.signature(
              lambda x, y=abc_qtype.unspecified(), z=abc_qtype.QTYPE, *args: None
          )
      )
      self.assertIsInstance(sig, abc_signature.Signature)
      self.assertLen(sig.parameters, 4)
      self.assertEqual(sig.parameters[0].name, 'x')
      self.assertEqual(sig.parameters[0].kind, 'positional-or-keyword')
      self.assertIsNone(sig.parameters[0].default)
      self.assertEqual(sig.parameters[1].name, 'y')
      self.assertEqual(sig.parameters[1].kind, 'positional-or-keyword')
      self.assertEqual(sig.parameters[1].default, abc_qtype.unspecified())
      self.assertEqual(sig.parameters[2].name, 'z')
      self.assertEqual(sig.parameters[2].kind, 'positional-or-keyword')
      self.assertEqual(sig.parameters[2].default, abc_qtype.QTYPE)
      self.assertEqual(sig.parameters[3].name, 'args')
      self.assertEqual(sig.parameters[3].kind, 'variadic-positional')
      self.assertIsNone(sig.parameters[3].default)
      self.assertEmpty(sig.aux_policy)

    with self.subTest('unsupported_parameter_kind'):
      with self.assertRaisesWithLiteralMatch(
          ValueError,
          "unsupported parameter kind: param.name='key',"
          ' param.kind=KEYWORD_ONLY',
      ):
        _ = abc_signature.make_operator_signature(
            inspect.signature(lambda *, key: None)
        )

    with self.subTest('as_qvalue_error'):
      with self.assertRaisesRegex(
          TypeError, re.escape('unable to represent (float) 1.5 as QValue')
      ):
        _ = abc_signature.make_operator_signature(
            inspect.signature(lambda x=1.5: None)
        )

  def test_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'expected str, tuple, or inspect.Signature, got signature: object',
    ):
      _ = abc_signature.make_operator_signature(object())  # pytype: disable=wrong-arg-types


class MakeInspectSignatureTest(absltest.TestCase):

  def test_basics(self):
    sig = abc_signature.make_inspect_signature(
        abc_signature.Signature((
            (
                abc_signature.SignatureParameter(
                    ('arg1', 'positional-only', None)
                ),
                abc_signature.SignatureParameter(
                    ('arg2', 'positional-or-keyword', abc_qtype.Unspecified())
                ),
                abc_signature.SignatureParameter(
                    ('args', 'variadic-positional', None)
                ),
                abc_signature.SignatureParameter(
                    ('arg3', 'keyword-only', abc_qtype.QTYPE)
                ),
                abc_signature.SignatureParameter(
                    ('kwargs', 'variadic-keyword', None)
                ),
            ),
            '',
        ))
    )
    self.assertIsInstance(sig, inspect.Signature)
    arg1, arg2, args, arg3, kwargs = sig.parameters.values()
    self.assertEqual(arg1.name, 'arg1')
    self.assertEqual(arg1.kind, inspect.Parameter.POSITIONAL_ONLY)
    self.assertEqual(arg1.default, inspect.Parameter.empty)
    self.assertEqual(arg2.name, 'arg2')
    self.assertEqual(arg2.kind, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    self.assertEqual(arg2.default, abc_qtype.Unspecified())
    self.assertEqual(args.name, 'args')
    self.assertEqual(args.kind, inspect.Parameter.VAR_POSITIONAL)
    self.assertEqual(args.default, inspect.Parameter.empty)
    self.assertEqual(arg3.name, 'arg3')
    self.assertEqual(arg3.kind, inspect.Parameter.KEYWORD_ONLY)
    self.assertEqual(arg3.default, abc_qtype.QTYPE)
    self.assertEqual(kwargs.name, 'kwargs')
    self.assertEqual(kwargs.kind, inspect.Parameter.VAR_KEYWORD)
    self.assertEqual(kwargs.default, inspect.Parameter.empty)

  def test_value_error(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        "unexpected parameter kind: param.name='arg', param.kind='custom-kind'",
    ):
      _ = abc_signature.make_inspect_signature(
          abc_signature.Signature((
              (abc_signature.SignatureParameter(('arg', 'custom-kind', None)),),
              '',
          ))
      )


if __name__ == '__main__':
  absltest.main()
