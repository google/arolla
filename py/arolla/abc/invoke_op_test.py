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

"""Tests for arolla.abc.invoke_op."""

import gc
import inspect
import re

from absl.testing import absltest
from arolla.abc import clib
from arolla.abc import dummy_types
from arolla.abc import expr as abc_expr
from arolla.abc import qtype as abc_qtype

make_tuple_op = abc_expr.make_lambda(
    '*args', abc_expr.placeholder('args'), name='make_tuple'
)
abc_expr.register_operator('invoke_op_test.make_tuple', make_tuple_op)


class InvokeOpTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    clib.clear_eval_compile_cache()

  def test_invoke_op(self):
    self.assertEqual(repr(clib.invoke_op('invoke_op_test.make_tuple')), '()')
    self.assertEqual(
        repr(
            clib.invoke_op(
                make_tuple_op, (abc_qtype.NOTHING, abc_qtype.unspecified())
            )
        ),
        '(NOTHING, unspecified)',
    )

  def test_invoke_op_with_wrong_arg_count(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "arolla.abc.invoke_op() missing 1 required positional argument: 'op'",
    ):
      clib.invoke_op()  # pytype: disable=missing-parameter
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.invoke_op() takes 2 positional arguments but 3 were given',
    ):
      clib.invoke_op(1, 2, 3)  # pytype: disable=wrong-arg-count

  def test_invoke_op_with_wrong_arg_types(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.invoke_op() expected Operator|str, got op:'
        ' arolla.abc.qtype.QType',
    ):
      clib.invoke_op(abc_qtype.QTYPE)
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.invoke_op() expected Operator|str, got op: object',
    ):
      clib.invoke_op(object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.invoke_op() expected a tuple[QValue, ...], got'
        ' input_qvalues: object',
    ):
      clib.invoke_op(make_tuple_op, object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.invoke_op() expected qvalues, got input_qvalues[1]:'
        ' object',
    ):
      clib.invoke_op(make_tuple_op, (abc_qtype.NOTHING, object()))  # pytype: disable=wrong-arg-types

  def test_invoke_op_caching(self):
    gc.collect()
    cnt = dummy_types.count_dummy_value_instances()
    clib.invoke_op(
        abc_expr.make_lambda(
            '', abc_expr.literal(dummy_types.make_dummy_value())
        )
    )
    gc.collect()
    self.assertGreater(dummy_types.count_dummy_value_instances(), cnt)
    clib.clear_eval_compile_cache()
    self.assertEqual(dummy_types.count_dummy_value_instances(), cnt)

  def test_invoke_op_with_unknown_operator(self):
    with self.assertRaisesWithLiteralMatch(
        LookupError,
        "arolla.abc.invoke_op() operator not found: 'no.such.operator'",
    ):
      clib.invoke_op('no.such.operator')

  def test_invoke_op_with_wrong_inputs_count(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'incorrect number of dependencies passed to an operator node'
        ),
    ):
      clib.invoke_op(
          'qtype.common_qtype',
          (abc_qtype.QTYPE, abc_qtype.QTYPE, abc_qtype.QTYPE),
      )

  def test_invoke_op_signature(self):
    self.assertEqual(
        inspect.signature(clib.invoke_op),
        inspect.signature(lambda op, input_qvalues=(), /: None),
    )

  def test_invoke_op_attribute_inference_regression(self):
    # Get an operator that requires no input and is fully lowered.
    op = abc_expr.to_lowest(clib.bind_op(make_tuple_op)).op
    self.assertNotEqual(op.fingerprint, make_tuple_op.fingerprint)
    clib.invoke_op(op)  # expect no exception


if __name__ == '__main__':
  absltest.main()
