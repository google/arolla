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

from absl.testing import absltest
from arolla.abc import abc as arolla_abc
from arolla.expr import expr as arolla_expr
from arolla.optools import helpers
from arolla.testing import testing as arolla_testing
from arolla.types import types as arolla_types

presence_and = arolla_abc.lookup_operator('core.presence_and')
presence_or = arolla_abc.lookup_operator('core.presence_or')
source_location = arolla_abc.lookup_operator('annotation.source_location')


class MakeLambdaTest(absltest.TestCase):

  def test_lambda(self):
    arolla_testing.assert_qvalue_equal_by_fingerprint(
        helpers.make_lambda(arolla_abc.placeholder('x'), name='foo', doc='bar'),
        arolla_types.LambdaOperator(
            arolla_abc.placeholder('x'), name='foo', doc='bar'
        ),
    )

  def test_restricted_lambda(self):
    arolla_testing.assert_qvalue_equal_by_fingerprint(
        helpers.make_lambda(
            arolla_abc.placeholder('x'),
            name='foo',
            doc='bar',
            qtype_constraints=[
                (arolla_abc.literal(arolla_types.present()), 'errro-message')
            ],
        ),
        arolla_types.RestrictedLambdaOperator(
            arolla_abc.placeholder('x'),
            name='foo',
            doc='bar',
            qtype_constraints=[
                (arolla_abc.literal(arolla_types.present()), 'errro-message')
            ],
        ),
    )


class TraceFunctionTest(absltest.TestCase):

  def test_basics(self):
    def fn(a, /, b, *c, x, **y):
      return (0, a, b, c, x, y)

    i0, a, b, c, x, y = helpers.trace_function(fn)
    self.assertEqual(i0, 0)
    arolla_testing.assert_expr_equal_by_fingerprint(
        a, arolla_abc.placeholder('a')
    )
    arolla_testing.assert_expr_equal_by_fingerprint(
        b, arolla_abc.placeholder('b')
    )
    self.assertIsInstance(c, tuple)
    self.assertLen(c, 1)
    arolla_testing.assert_expr_equal_by_fingerprint(
        c[0], arolla_abc.placeholder('c')
    )
    arolla_testing.assert_expr_equal_by_fingerprint(
        x, arolla_abc.placeholder('x')
    )
    self.assertIsInstance(y, dict)
    self.assertLen(y, 1)
    arolla_testing.assert_expr_equal_by_fingerprint(
        y['y'], arolla_abc.placeholder('y')
    )

  def test_fix_variadics(self):
    def fn(a, /, b, *c, x, **y):
      c, y = helpers.fix_trace_args_kwargs(c, y)
      return (0, a, b, c, x, y)

    i0, a, b, c, x, y = helpers.trace_function(fn)
    self.assertEqual(i0, 0)
    arolla_testing.assert_expr_equal_by_fingerprint(
        a, arolla_abc.placeholder('a')
    )
    arolla_testing.assert_expr_equal_by_fingerprint(
        b, arolla_abc.placeholder('b')
    )
    arolla_testing.assert_expr_equal_by_fingerprint(
        c, arolla_abc.placeholder('c')
    )
    arolla_testing.assert_expr_equal_by_fingerprint(
        x, arolla_abc.placeholder('x')
    )
    arolla_testing.assert_expr_equal_by_fingerprint(
        y, arolla_abc.placeholder('y')
    )

  def test_custom_tracers(self):
    def fn(a, /, *, x):
      return (a, x)

    a, x = helpers.trace_function(fn, gen_tracer=arolla_abc.leaf)
    arolla_testing.assert_expr_equal_by_fingerprint(a, arolla_abc.leaf('a'))
    arolla_testing.assert_expr_equal_by_fingerprint(x, arolla_abc.leaf('x'))

  def test_callable_class_instance(self):
    class MyCallable:

      def __call__(self, a, b):
        return presence_and(a, b)

    my_callable = MyCallable()
    res = helpers.trace_function(my_callable)
    arolla_testing.assert_expr_equal_by_fingerprint(
        res,
        presence_and(arolla_abc.placeholder('a'), arolla_abc.placeholder('b')),
    )

  def test_error_non_callable(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected a callable, got int'
    ):
      helpers.trace_function(57)  # pytype: disable=wrong-arg-types

  def test_annotate_with_source_locations(self):
    def fn(x, y, z):
      x_and_y = presence_and(x, y)
      return presence_or(x_and_y, z)

    annotated = helpers.trace_function(fn, annotate_with_source_locations=True)
    arolla_testing.assert_qvalue_equal_by_fingerprint(
        annotated.op, source_location
    )
    arolla_testing.assert_qvalue_equal_by_fingerprint(
        annotated.node_deps[1].qvalue,
        arolla_types.text('fn'),
    )
    arolla_testing.assert_qvalue_equal_by_fingerprint(
        annotated.node_deps[2].qvalue,
        arolla_types.text('arolla/optools/helpers_test.py'),
    )
    self.assertGreater(annotated.node_deps[3].qvalue.py_value(), 0)
    arolla_testing.assert_qvalue_equal_by_fingerprint(
        annotated.node_deps[4].qvalue, arolla_types.int32(0)
    )

    line_texts = []

    def collect_line_texts(node, _):
      if node.is_operator and node.op == source_location:
        line_texts.append(node.node_deps[5].qvalue.py_value())
      return node

    arolla_abc.post_order_traverse(annotated, collect_line_texts)

    self.assertLen(line_texts, 2)
    self.assertIn('x_and_y = presence_and(x, y)', line_texts[0])
    self.assertIn('return presence_or(x_and_y, z)', line_texts[1])

  def test_annotate_with_source_locations_default_view(self):
    def fn(x, y):
      return x | y

    annotated = helpers.trace_function(fn, annotate_with_source_locations=True)
    arolla_testing.assert_qvalue_equal_by_fingerprint(
        annotated.op, source_location
    )
    arolla_testing.assert_qvalue_equal_by_fingerprint(
        annotated.node_deps[2].qvalue,
        # Not default_view.py, because it has _arolla_tracebackhide_.
        arolla_types.text('arolla/optools/helpers_test.py'),
    )

  def test_annotate_with_source_locations_not_expr(self):
    def fn(unused_x):
      return 1

    res = helpers.trace_function(fn, annotate_with_source_locations=True)
    self.assertIs(res, 1)


class FixTraceArgsKwargs(absltest.TestCase):

  def test_fix_trace_args(self):
    arolla_testing.assert_expr_equal_by_fingerprint(
        helpers.fix_trace_args((arolla_abc.placeholder('x'),)),
        arolla_abc.placeholder('x'),
    )
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected `*args` provided by `trace_function(...)`'
    ):
      _ = helpers.fix_trace_args(arolla_abc.placeholder('x'))  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected `*args` provided by `trace_function(...)`'
    ):
      _ = helpers.fix_trace_args(arolla_abc.placeholder('x'))  # pytype: disable=wrong-arg-types

  def test_fix_trace_kwargs(self):
    arolla_testing.assert_expr_equal_by_fingerprint(
        helpers.fix_trace_kwargs({'x': arolla_abc.placeholder('x')}),
        arolla_abc.placeholder('x'),
    )
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected `**kwargs` provided by `trace_function(...)`'
    ):
      _ = helpers.fix_trace_kwargs(arolla_abc.placeholder('x'))  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected `**kwargs` provided by `trace_function(...)`'
    ):
      _ = helpers.fix_trace_kwargs(arolla_abc.placeholder('x'))  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected `**kwargs` provided by `trace_function(...)`'
    ):
      _ = helpers.fix_trace_kwargs({'a': 2})  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected `**kwargs` provided by `trace_function(...)`'
    ):
      _ = helpers.fix_trace_kwargs({1: arolla_abc.placeholder('x')})  # pytype: disable=wrong-arg-types

  def test_fix_trace_args_kwargs(self):
    x, y = helpers.fix_trace_args_kwargs(
        (arolla_abc.placeholder('x'),), {'y': arolla_abc.placeholder('y')}
    )
    arolla_testing.assert_expr_equal_by_fingerprint(
        x, arolla_abc.placeholder('x')
    )
    arolla_testing.assert_expr_equal_by_fingerprint(
        y, arolla_abc.placeholder('y')
    )


class RegisterNamespaceDocstringTest(absltest.TestCase):

  def test_basic(self):
    helpers.register_namespace_docstring(
        'helpers_test.basic.ns',
        'My docstring.',
        if_present='unsafe_override',
    )
    m = arolla_expr.containers.OperatorsContainer(
        unsafe_extra_namespaces=['helpers_test.basic.ns']
    )
    self.assertEqual(m.helpers_test.basic.ns.__doc__, 'My docstring.')

  def test_empty_raises(self):
    with self.assertRaisesRegex(ValueError, 'docstring must be non-empty'):
      helpers.register_namespace_docstring('helpers_test.empty.ns', '')

  def test_none_raises(self):
    with self.assertRaisesRegex(ValueError, 'docstring must be non-empty'):
      helpers.register_namespace_docstring(
          'helpers_test.none.ns', None  # pytype: disable=wrong-arg-types
      )

  def test_duplicate_raises(self):
    helpers.register_namespace_docstring(
        'helpers_test.dup.ns',
        'First.',
        if_present='unsafe_override',
    )
    with self.assertRaises(ValueError):
      helpers.register_namespace_docstring('helpers_test.dup.ns', 'Second.')


if __name__ == '__main__':
  absltest.main()
