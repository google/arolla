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

"""Tests for arolla.abc.au_eval_op."""

import inspect
import re

from absl.testing import absltest
from arolla.abc import aux_binding_policy as abc_aux_binding_policy
from arolla.abc import clib
from arolla.abc import expr as abc_expr
from arolla.abc import qtype as abc_qtype

abc_aux_binding_policy.register_classic_aux_binding_policy_with_custom_boxing(
    'aux_eval_op_test_policy', lambda x: x
)

op_tuple = abc_expr.make_lambda(
    '*args |aux_eval_op_test_policy', abc_expr.placeholder('args')
)
op_first = abc_expr.make_lambda(
    ('x=, *_args |aux_eval_op_test_policy', abc_qtype.unspecified()),
    abc_expr.placeholder('x'),
)
op_id = abc_expr.make_lambda(
    'x |aux_eval_op_test_policy', abc_expr.placeholder('x')
)


class AuxEvalOpTest(absltest.TestCase):

  def test_aux_eval_op(self):
    self.assertEqual(repr(clib.aux_eval_op(op_tuple)), '()')
    self.assertEqual(
        repr(clib.aux_eval_op(op_tuple, abc_qtype.unspecified())),
        '(unspecified)',
    )
    self.assertEqual(
        repr(
            clib.aux_eval_op(
                op_tuple, abc_qtype.NOTHING, abc_qtype.unspecified()
            )
        ),
        '(NOTHING, unspecified)',
    )
    self.assertEqual(clib.aux_eval_op(op_first), abc_qtype.unspecified())
    self.assertEqual(
        clib.aux_eval_op(op_first, x=abc_qtype.NOTHING), abc_qtype.NOTHING
    )
    self.assertEqual(
        clib.aux_eval_op(op_first, abc_qtype.QTYPE, abc_qtype.NOTHING),
        abc_qtype.QTYPE,
    )

  def test_aux_eval_op_with_wrong_arg_count(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "arolla.abc.aux_eval_op() missing 1 required positional argument: 'op'",
    ):
      clib.aux_eval_op()  # pytype: disable=missing-parameter

  def test_aux_eval_op_with_wrong_arg_types(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.aux_eval_op() expected Operator|str, got op:'
        ' arolla.abc.qtype.QType',
    ):
      clib.aux_eval_op(abc_qtype.QTYPE)
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.aux_eval_op() expected Operator|str, got op: object',
    ):
      clib.aux_eval_op(object())  # pytype: disable=wrong-arg-types

  def test_aux_eval_op_operator_not_found(self):
    with self.assertRaisesRegex(
        ValueError, re.escape("operator 'operator.not.found' not found")
    ):
      clib.aux_eval_op(
          abc_expr.unsafe_make_registered_operator('operator.not.found')
      )

  def test_aux_eval_op_bind_argument_fails(self):
    with self.assertRaisesWithLiteralMatch(
        RuntimeError,
        'arolla.abc.aux_bind_arguments() auxiliary binding policy has failed:'
        " 'aux_eval_op_test_policy'",
    ):
      clib.aux_eval_op(op_tuple, object())

  def test_aux_eval_op_unexpected_expr(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.aux_eval_op() expected all arguments to be qvalues, got'
        " an expression for the parameter 'args[0]'",
    ):
      clib.aux_eval_op(op_tuple, abc_expr.leaf('x'))
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.aux_eval_op() expected all arguments to be qvalues, got'
        " an expression for the parameter 'x'",
    ):
      clib.aux_eval_op(op_first, abc_expr.leaf('x'), abc_qtype.unspecified())

    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.aux_eval_op() expected all arguments to be qvalues, got'
        " an expression for the parameter 'x'",
    ):
      clib.aux_eval_op(op_id, abc_expr.leaf('x'))

  def test_aux_eval_op_literal_unsupported(self):
    op_qtype = abc_expr.make_lambda(
        'x, qtype |aux_eval_op_test_policy',
        clib.bind_op(
            'annotation.qtype',
            abc_expr.placeholder('x'),
            abc_expr.placeholder('qtype'),
        ),
    )
    with self.assertRaisesRegex(
        ValueError, re.escape('`qtype` must be a literal')
    ):
      clib.aux_eval_op(op_qtype, abc_qtype.QTYPE, qtype=abc_qtype.QTYPE)

  def test_aux_eval_op_unknown_aux_policy(self):
    op = abc_expr.make_lambda(
        'x |unknown_aux_policy', abc_expr.placeholder('x')
    )
    with self.assertRaisesWithLiteralMatch(
        RuntimeError,
        'arolla.abc.aux_bind_arguments() auxiliary binding policy not found:'
        " 'unknown_aux_policy'",
    ):
      clib.aux_eval_op(op, abc_qtype.QTYPE)

  def test_aux_eval_op_signature(self):
    self.assertEqual(
        inspect.signature(clib.aux_eval_op),
        inspect.signature(lambda op, /, *args, **kwargs: None),
    )


if __name__ == '__main__':
  absltest.main()
