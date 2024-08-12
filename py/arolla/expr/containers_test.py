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

"""Tests for arolla.expr.containers."""

from absl.testing import absltest
from arolla.abc import abc as arolla_abc
from arolla.expr import containers
from arolla.operators import operators_clib as _


class ContainersTest(absltest.TestCase):

  def test_leaf_container(self):
    l = containers.LeafContainer()
    abc_leaf = l.abc
    self.assertTrue(abc_leaf.is_leaf)
    self.assertEqual(abc_leaf.leaf_key, 'abc')
    self.assertEqual(str(abc_leaf), 'L.abc')
    self.assertEqual(repr(abc_leaf), 'L.abc')
    self.assertEqual(arolla_abc.get_leaf_keys(abc_leaf), ['abc'])
    qwe_leaf = l.qwe
    self.assertTrue(qwe_leaf.is_leaf)
    self.assertEqual(qwe_leaf.leaf_key, 'qwe')
    self.assertEqual(str(qwe_leaf), 'L.qwe')
    self.assertEqual(repr(qwe_leaf), 'L.qwe')
    hello_leaf = l['Hello, World!']
    self.assertEqual(repr(hello_leaf), "L['Hello, World!']")

  def test_leaf_container_error(self):
    l = containers.LeafContainer()
    with self.assertRaisesRegex(TypeError, 'Leaf key must be str.*bytes.*'):
      _ = l[b'a']  # pytype: disable=unsupported-operands

  def test_placeholder_container(self):
    p = containers.PlaceholderContainer()
    abc_placeholder = p.abc
    self.assertTrue(abc_placeholder.is_placeholder)
    self.assertEqual(abc_placeholder.placeholder_key, 'abc')
    self.assertEqual(str(abc_placeholder), 'P.abc')
    self.assertEqual(repr(abc_placeholder), 'P.abc')
    qwe_placeholder = p.qwe
    self.assertTrue(qwe_placeholder.is_placeholder)
    self.assertEqual(qwe_placeholder.placeholder_key, 'qwe')
    self.assertEqual(str(qwe_placeholder), 'P.qwe')
    self.assertEqual(repr(qwe_placeholder), 'P.qwe')
    non_identifier_placeholder = p['/@']
    self.assertTrue(non_identifier_placeholder.is_placeholder)
    self.assertEqual(non_identifier_placeholder.placeholder_key, '/@')
    self.assertEqual(str(non_identifier_placeholder), "P['/@']")
    self.assertEqual(repr(non_identifier_placeholder), "P['/@']")


class OperatorsContainerTest(absltest.TestCase):

  def test_getattr(self):
    m = containers.OperatorsContainer()
    self.assertIsInstance(m.math, containers.OperatorsContainer)
    self.assertIsInstance(m.math.add, arolla_abc.RegisteredOperator)

  def test_getitem(self):
    m = containers.OperatorsContainer()
    with self.assertRaisesWithLiteralMatch(
        LookupError, "operator 'math' is not present in container"
    ):
      _ = m['math']
    self.assertIsInstance(m['math.add'], arolla_abc.RegisteredOperator)
    self.assertEqual(m.math['add'], m['math.add'])

  def test_registration(self):
    reg_op_name = 'math.operators_container_test_registration_op_name'
    m = containers.OperatorsContainer()
    with self.assertRaisesWithLiteralMatch(
        LookupError, f'operator {reg_op_name!r} is not present in container'
    ):
      _ = m[reg_op_name]
    arolla_abc.register_operator(reg_op_name, m.math.add)
    _ = m[reg_op_name]  # no exception

  def test_extra_modules(self):
    class FakeModule:

      def get_namespaces(self):
        return [
            'container_test_extra_modules.test.test1',
            'container_test_extra_modules.test2',
        ]

    m = containers.OperatorsContainer(FakeModule())
    arolla_abc.register_operator(
        'container_test_extra_modules.test.test1.op', m.math.add
    )
    arolla_abc.register_operator(
        'container_test_extra_modules.test2.op', m.math.add
    )
    arolla_abc.register_operator(
        'container_test_extra_modules.test3.op', m.math.add
    )
    self.assertIsInstance(
        m.container_test_extra_modules.test, containers.OperatorsContainer
    )
    self.assertIsInstance(
        m.container_test_extra_modules.test.test1, containers.OperatorsContainer
    )
    self.assertIsInstance(
        m.container_test_extra_modules.test.test1.op,
        arolla_abc.RegisteredOperator,
    )
    self.assertIsInstance(
        m.container_test_extra_modules.test2, containers.OperatorsContainer
    )
    self.assertIsInstance(
        m.container_test_extra_modules.test2.op, arolla_abc.RegisteredOperator
    )
    with self.assertRaisesWithLiteralMatch(
        AttributeError,
        "'container_test_extra_modules.test.DoesNotExist' is not present in"
        ' container',
    ):
      _ = m.container_test_extra_modules.test.DoesNotExist
    with self.assertRaisesWithLiteralMatch(
        AttributeError,
        "'container_test_extra_modules.test3' is not present in container",
    ):
      _ = m.container_test_extra_modules.test3.op

  def test_dir(self):
    m = containers.OperatorsContainer()
    self.assertIn('math', dir(m))
    self.assertIn('trig', dir(m.math))
    self.assertIn('sum', dir(m.math))
    self.assertNotIn('', dir(m))
    self.assertNotIn('', dir(m.math))
    self.assertNotIn('add2', dir(m.math))
    self.assertNotIn('xyz', dir(m.math))
    _ = arolla_abc.register_operator('math.add2', m.math.add)
    _ = arolla_abc.register_operator('math.xyz.add', m.math.add)
    self.assertIn('add2', dir(m.math))
    self.assertNotIn('xyz', dir(m.math))

  def test_unsafe_operators_container(self):
    m = containers.unsafe_operators_container()
    self.assertNotIn('foo', dir(m))
    self.assertIsInstance(m.foo.bar, containers.OperatorsContainer)
    self.assertEmpty(dir(m.foo))
    self.assertEmpty(dir(m.foo.bar))
    arolla_abc.register_operator('foo.bar.op', m.math.add)
    self.assertIn('bar', dir(m.foo))
    self.assertIn('op', dir(m.foo.bar))
    self.assertIsInstance(m.foo.bar.op, arolla_abc.RegisteredOperator)

  def test_op_namespace_collision(self):
    m = containers.unsafe_operators_container()
    arolla_abc.register_operator('test_op_namespace_collision.a', m.math.add)
    arolla_abc.register_operator('test_op_namespace_collision.a.b', m.math.add)
    self.assertIsInstance(
        m.test_op_namespace_collision.a, arolla_abc.RegisteredOperator
    )
    with self.assertRaises(AttributeError):
      _ = m.test_op_namespace_collision.a.b

  def test_bool(self):
    class FakeModule:

      def get_namespaces(self):
        return ['container_test_bool.test']

    m = containers.OperatorsContainer(FakeModule())
    self.assertTrue(m)
    self.assertTrue(m.math)
    self.assertTrue(m.math.add)
    self.assertTrue(m.container_test_bool)
    self.assertFalse(m.container_test_bool.test)
    unsafe_m = containers.unsafe_operators_container()
    self.assertTrue(unsafe_m)
    self.assertTrue(unsafe_m.math)
    self.assertTrue(unsafe_m.math.add)
    self.assertFalse(unsafe_m.container_test_bool)
    self.assertFalse(unsafe_m.container_test_bool.test)

  def test_regression_unsafe_container_top_level_getitem_op(self):
    m = containers.OperatorsContainer()
    arolla_abc.register_operator(
        'test_regression_unsafe_container_top_level_getitem_op', m.math.add
    )
    _ = m['test_regression_unsafe_container_top_level_getitem_op']  # no error


if __name__ == '__main__':
  absltest.main()
