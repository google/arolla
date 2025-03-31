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

import inspect
import re
import traceback

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.expr import expr as arolla_expr
from arolla.operators import operators_clib as _
from arolla.optools import decorators
from arolla.s11n import s11n as arolla_s11n
from arolla.testing import testing as arolla_testing
from arolla.types import types as arolla_types

L = arolla_expr.LeafContainer()
M = arolla_expr.OperatorsContainer()
P = arolla_expr.PlaceholderContainer()


def gen_eval_testcases():
  yield (
      lambda x, y: x + y,
      (arolla_types.int32(1), arolla_types.int32(2)),
      arolla_types.int32(3),
  )
  yield (
      lambda *args: sum(args, 0.0),
      (
          arolla_types.float32(1),
          arolla_types.float32(2),
          arolla_types.float64(3),
      ),
      arolla_types.float64(6),
  )
  yield (
      lambda x, y=1: x + y,
      (arolla_types.array_int32([1, 2]),),
      arolla_types.array_int32([2, 3]),
  )

  # Testing defaults + varargs combination.
  fn = lambda x, y=1, *args: x + y + sum(args)
  yield (
      fn,
      (arolla_types.array_int32([1, 2]),),
      arolla_types.array_int32([2, 3]),
  )
  yield (
      fn,
      (arolla_types.array_int32([1, 2]), arolla_types.int32(2)),
      arolla_types.array_int32([3, 4]),
  )
  yield (
      fn,
      (
          arolla_types.array_int32([1, 2]),
          arolla_types.int32(2),
          arolla_types.int32(3),
      ),
      arolla_types.array_int32([6, 7]),
  )

  # Function with context.
  y = 1
  fn = lambda x: x + y
  yield (
      fn,
      (arolla_types.array_int32([1, 2]),),
      arolla_types.array_int32([2, 3]),
  )

  # Callable.
  class CallableClass:

    def __init__(self, y):
      self.y = y

    def __call__(self, x):
      return x + self.y

  yield (
      CallableClass(1),
      (arolla_types.array_int32([1, 2]),),
      arolla_types.array_int32([2, 3]),
  )


EVAL_TEST_CASES = tuple(gen_eval_testcases())


class PyFunctionOperatorTest(parameterized.TestCase):

  def test_op_x_y(self):
    @decorators.as_py_function_operator(
        'test.op_x_y',
        qtype_constraints=[
            (M.qtype.is_numeric_qtype(P.x), 'expected numeric `x`, got {x}'),
            (M.qtype.is_numeric_qtype(P.y), 'expected numeric `y`, got {y}'),
        ],
        qtype_inference_expr=M.qtype.common_qtype(P.x, P.y),
    )
    def op(x, y):
      return x + y

    self.assertEqual(op.display_name, 'test.op_x_y')
    self.assertEqual(
        inspect.signature(op), inspect.signature(lambda x, y: None)
    )
    self.assertEqual(
        arolla_abc.infer_attr(
            op, (arolla_types.OPTIONAL_INT32, arolla_types.FLOAT32)
        ).qtype,
        arolla_types.OPTIONAL_FLOAT32,
    )
    arolla_testing.assert_qvalue_allequal(
        arolla_abc.aux_eval_op(op, 1, 1.5), arolla_types.float32(2.5)
    )
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'expected numeric `x`, got OPTIONAL_TEXT'
    ):
      arolla_abc.infer_attr(op, (arolla_types.OPTIONAL_TEXT, None))
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'expected numeric `y`, got OPTIONAL_TEXT'
    ):
      arolla_abc.infer_attr(op, (None, arolla_types.OPTIONAL_TEXT))
    with self.assertRaisesRegex(
        ValueError, re.escape('qtype inference expression produced no qtype')
    ):
      arolla_abc.infer_attr(
          op, (arolla_types.ARRAY_INT32, arolla_types.DENSE_ARRAY_INT32)
      )

  def test_op_no_args(self):
    @decorators.as_py_function_operator(
        'test.no_args', qtype_inference_expr=arolla_types.FLOAT32
    )
    def op():
      return arolla_types.float32(1.5)

    self.assertEqual(inspect.signature(op), inspect.signature(lambda: None))
    self.assertEqual(arolla_abc.infer_attr(op, ()).qtype, arolla_types.FLOAT32)
    arolla_testing.assert_qvalue_allequal(
        arolla_abc.aux_eval_op(op), arolla_types.float32(1.5)
    )

  def test_op_args(self):
    @decorators.as_py_function_operator(
        'test.op_args', qtype_inference_expr=P.args
    )
    def op(*args):
      return arolla_types.tuple_(*args)

    self.assertEqual(op.display_name, 'test.op_args')
    self.assertEqual(
        inspect.signature(op), inspect.signature(lambda *args: None)
    )
    self.assertEqual(
        arolla_abc.infer_attr(op, ()).qtype, arolla_types.make_tuple_qtype()
    )
    arolla_testing.assert_qvalue_equal_by_fingerprint(
        arolla_abc.aux_eval_op(op), arolla_types.tuple_()
    )
    self.assertEqual(
        arolla_abc.infer_attr(
            op, (arolla_types.INT32, arolla_types.FLOAT32)
        ).qtype,
        arolla_types.make_tuple_qtype(arolla_types.INT32, arolla_types.FLOAT32),
    )
    arolla_testing.assert_qvalue_equal_by_fingerprint(
        arolla_abc.aux_eval_op(
            op, arolla_types.int32(1), arolla_types.float32(1.5)
        ),
        arolla_types.tuple_(1, 1.5),
    )

  def test_op_x_args(self):
    @decorators.as_py_function_operator(
        'test.op_x_args',
        qtype_inference_expr=M.qtype.make_tuple_qtype(P.x, P.args),
    )
    def op(x, *args):
      return arolla_types.tuple_(x, arolla_types.tuple_(*args))

    self.assertEqual(op.display_name, 'test.op_x_args')
    self.assertEqual(
        inspect.signature(op), inspect.signature(lambda x, *args: None)
    )
    self.assertEqual(
        arolla_abc.infer_attr(op, (arolla_types.INT32,)).qtype,
        arolla_types.make_tuple_qtype(
            arolla_types.INT32, arolla_types.make_tuple_qtype()
        ),
    )
    arolla_testing.assert_qvalue_equal_by_fingerprint(
        arolla_abc.aux_eval_op(op, arolla_types.int32(1)),
        arolla_types.tuple_(arolla_types.int32(1), arolla_types.tuple_()),
    )
    self.assertEqual(
        arolla_abc.infer_attr(
            op, (arolla_types.INT32, arolla_types.FLOAT32)
        ).qtype,
        arolla_types.make_tuple_qtype(
            arolla_types.INT32,
            arolla_types.make_tuple_qtype(arolla_types.FLOAT32),
        ),
    )
    arolla_testing.assert_qvalue_equal_by_fingerprint(
        arolla_abc.aux_eval_op(
            op, arolla_types.int32(1), arolla_types.float32(1.5)
        ),
        arolla_types.tuple_(1, arolla_types.tuple_(1.5)),
    )

  def test_getdoc(self):
    @decorators.as_py_function_operator('test.no_doc', qtype_inference_expr=P.x)
    def op_no_doc(x):
      return x

    @decorators.as_py_function_operator(
        'test.multiline_doc', qtype_inference_expr=arolla_abc.QTYPE
    )
    def op_multiline_doc():
      """Returns nothing.

      Extra line.
      """
      pass

    self.assertEqual(op_no_doc.getdoc(), '')
    self.assertEqual(
        op_multiline_doc.getdoc(), 'Returns nothing.\n\nExtra line.'
    )

  @parameterized.parameters(*EVAL_TEST_CASES)
  def test_eval(self, fn, args, expected_res):
    op = decorators.as_py_function_operator(
        'test.foo', qtype_inference_expr=expected_res.qtype
    )(fn)
    arolla_testing.assert_qvalue_allequal(
        arolla_abc.aux_eval_op(op, *args), expected_res
    )

  def test_non_qvalue_return_raises(self):
    @decorators.as_py_function_operator('test.foo', qtype_inference_expr=P.x)
    def op(x):
      del x
      return 'Boom!'

    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected the result to be a qvalue, got str'
    ):
      arolla_abc.eval_expr(op(1))

  def test_incorrect_qtype_return(self):
    @decorators.as_py_function_operator(
        'test.foo', qtype_inference_expr=arolla_types.INT32
    )
    def op(x):
      del x
      return arolla_types.float32(1.5)

    with self.assertRaisesWithLiteralMatch(
        ValueError, 'expected the result to have qtype INT32, got FLOAT32'
    ):
      arolla_abc.eval_expr(op(1))

  def test_experimental_aux_policy(self):
    @decorators.as_py_function_operator(
        'test.foo', qtype_inference_expr=arolla_abc.QTYPE
    )
    def op1():
      return None

    @decorators.as_py_function_operator(
        'test.foo',
        qtype_inference_expr=arolla_abc.QTYPE,
        experimental_aux_policy='policy-name',
    )
    def op2():
      return None

    self.assertEqual(arolla_abc.get_operator_signature(op1).aux_policy, '')
    self.assertEqual(
        arolla_abc.get_operator_signature(op2).aux_policy, 'policy-name'
    )

  def test_compile_error(self):
    @decorators.as_py_function_operator('test.foo', qtype_inference_expr=P.x)
    def my_op(x):
      del x
      raise NotImplementedError('not yet implemented')

    def my_fn():
      return arolla_types.eval_(my_op(1))

    try:
      my_fn()
    except NotImplementedError as ex:
      e = ex
    self.assertIsInstance(e, NotImplementedError)
    self.assertRegex(str(e), 'not yet implemented')
    self.assertEqual(e.operator_name, 'test.foo')  # pytype: disable=attribute-error
    self.assertIn('in my_op', ''.join(traceback.format_exception(e)))
    self.assertIn('in my_fn', ''.join(traceback.format_exception(e)))

  def test_compile_chained_error(self):
    @decorators.as_py_function_operator('test.foo', qtype_inference_expr=P.x)
    def my_op_1(x):
      del x
      raise NotImplementedError('not yet implemented')

    @decorators.as_py_function_operator('test.bar', qtype_inference_expr=P.x)
    def my_op_2(x):
      return arolla_types.eval_(my_op_1(x))

    try:
      arolla_types.eval_(my_op_2(1))
    except NotImplementedError as ex:
      e = ex
    self.assertIsInstance(e, NotImplementedError)
    self.assertRegex(str(e), 'not yet implemented')
    self.assertEqual(e.operator_name, 'test.bar')  # pytype: disable=attribute-error
    self.assertIn('in my_op_1', ''.join(traceback.format_exception(e)))
    self.assertIn('in my_op_2', ''.join(traceback.format_exception(e)))

  def test_eval_error(self):
    @decorators.as_py_function_operator('test.foo', qtype_inference_expr=P.x)
    def my_op(x):
      del x
      raise NotImplementedError('not yet implemented')

    def my_fn():
      return arolla_types.eval_(my_op(L.x), x=1)

    try:
      my_fn()
    except NotImplementedError as ex:
      e = ex
    self.assertIsInstance(e, NotImplementedError)
    self.assertRegex(str(e), 'not yet implemented')
    self.assertEqual(e.operator_name, 'test.foo')  # pytype: disable=attribute-error
    self.assertIn('in my_op', ''.join(traceback.format_exception(e)))
    self.assertIn('in my_fn', ''.join(traceback.format_exception(e)))

  def test_eval_chained_error(self):
    @decorators.as_py_function_operator('test.foo', qtype_inference_expr=P.x)
    def my_op_1(x):
      del x
      raise NotImplementedError('not yet implemented')

    @decorators.as_py_function_operator('test.bar', qtype_inference_expr=P.x)
    def my_op_2(x):
      return arolla_types.eval_(my_op_1(L.x), x=x)

    try:
      arolla_types.eval_(my_op_2(L.x), x=1)
    except NotImplementedError as ex:
      e = ex
    self.assertIsInstance(e, NotImplementedError)
    self.assertRegex(str(e), 'not yet implemented')
    self.assertEqual(e.operator_name, 'test.bar')  # pytype: disable=attribute-error
    self.assertIn('in my_op_1', ''.join(traceback.format_exception(e)))
    self.assertIn('in my_op_2', ''.join(traceback.format_exception(e)))

  def test_serialization(self):
    ref_codec = arolla_types.PyObjectReferenceCodec()

    @decorators.as_py_function_operator(
        'test.add',
        qtype_constraints=[
            (M.qtype.is_numeric_qtype(P.x), 'expected numeric `x`'),
            (M.qtype.is_numeric_qtype(P.y), 'expected numeric `y`'),
        ],
        qtype_inference_expr=M.qtype.common_qtype(P.x, P.y),
        codec=ref_codec.name,
    )
    def op_add(x, y):
      """Add docstring."""
      return x + y

    op_add = arolla_s11n.loads(arolla_s11n.dumps(op_add))

    self.assertEqual(op_add.display_name, 'test.add')  # pytype: disable=attribute-error
    self.assertEqual(op_add.getdoc(), 'Add docstring.')  # pytype: disable=attribute-error
    self.assertEqual(
        inspect.signature(op_add), inspect.signature(lambda x, y: None)
    )
    self.assertEqual(
        arolla_abc.infer_attr(
            op_add, (arolla_types.OPTIONAL_INT32, arolla_types.FLOAT32)
        ).qtype,
        arolla_types.OPTIONAL_FLOAT32,
    )
    arolla_testing.assert_qvalue_allequal(
        arolla_abc.aux_eval_op(op_add, 1, 1.5), arolla_types.float32(2.5)
    )
    with self.assertRaisesWithLiteralMatch(ValueError, 'expected numeric `x`'):
      arolla_abc.infer_attr(op_add, (arolla_types.OPTIONAL_TEXT, None))
    with self.assertRaisesWithLiteralMatch(ValueError, 'expected numeric `y`'):
      arolla_abc.infer_attr(op_add, (None, arolla_types.OPTIONAL_TEXT))
    with self.assertRaisesRegex(
        ValueError, re.escape('qtype inference expression produced no qtype')
    ):
      arolla_abc.infer_attr(
          op_add, (arolla_types.ARRAY_INT32, arolla_types.DENSE_ARRAY_INT32)
      )

  def test_serialization_no_codec_error(self):
    @decorators.as_py_function_operator(
        'test.add', qtype_inference_expr=M.qtype.common_qtype(P.x, P.y)
    )
    def op_add(x, y):
      return x + y

    with self.assertRaisesRegex(
        ValueError,
        r'missing serialization codec for PyObject\{<.*\.op_add.*>\}',
    ):
      arolla_s11n.dumps(op_add)


if __name__ == '__main__':
  absltest.main()
