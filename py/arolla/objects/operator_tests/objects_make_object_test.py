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

import re
from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.objects import objects

M = arolla.M | objects.M
L = arolla.L


class ObjectsMakeObjectTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    named_tuple_qtypes = (
        arolla.make_namedtuple_qtype(),
        arolla.make_namedtuple_qtype(x=arolla.INT32),
        arolla.make_namedtuple_qtype(x=arolla.INT32, y=arolla.FLOAT32),
    )
    expected_qtypes = []
    for nt_qtype in named_tuple_qtypes:
      expected_qtypes.append((objects.OBJECT, nt_qtype, objects.OBJECT))
      expected_qtypes.append((arolla.UNSPECIFIED, nt_qtype, objects.OBJECT))
    possible_qtypes = (
        arolla.testing.DETECT_SIGNATURES_DEFAULT_QTYPES
        + named_tuple_qtypes
        + (objects.OBJECT,)
    )
    arolla.testing.assert_qtype_signatures(
        M.objects.make_object, expected_qtypes, possible_qtypes=possible_qtypes
    )

  @parameterized.parameters(
      (
          {'foo': arolla.int32(1), 'bar': arolla.float32(2.0)},
          'Object{attributes={bar=2., foo=1}}',
      ),
      ({}, 'Object{attributes={}}'),
  )
  def test_eval(self, fields, expected_repr):
    result = arolla.eval(M.objects.make_object(**fields))
    self.assertEqual(result.qtype, objects.OBJECT)
    self.assertEqual(repr(result), expected_repr)  # Proxy to test the result.

  @parameterized.parameters(
      (
          {'foo': arolla.int32(1), 'bar': arolla.float32(2.0)},
          (
              'Object{attributes={bar=2., foo=1},'
              ' prototype=Object{attributes={a=123}}}'
          ),
      ),
      ({}, 'Object{attributes={}, prototype=Object{attributes={a=123}}}'),
  )
  def test_eval_with_prototype(self, fields, expected_repr):
    prototype = arolla.eval(
        M.objects.make_object(a=arolla.int32(123))
    )
    result = arolla.eval(
        M.objects.make_object(prototype, **fields)
    )
    self.assertEqual(result.qtype, objects.OBJECT)
    self.assertEqual(repr(result), expected_repr)  # Proxy to test the result.

  def test_binding_policy_eager_node_deps(self):
    expr = M.objects.make_object(x=1, y=2)
    node_deps = expr.node_deps
    self.assertLen(node_deps, 2)
    arolla.testing.assert_expr_equal_by_fingerprint(
        node_deps[0], arolla.literal(arolla.unspecified())
    )
    arolla.testing.assert_expr_equal_by_fingerprint(
        node_deps[1], arolla.literal(arolla.namedtuple(x=1, y=2))
    )
    arolla.testing.assert_expr_equal_by_fingerprint(
        M.objects.make_object(*node_deps), expr
    )

  def test_binding_policy_eager_node_deps_with_prototype(self):
    prototype = arolla.eval(M.objects.make_object(x=1))
    expr = M.objects.make_object(prototype, y=2)
    node_deps = expr.node_deps
    self.assertLen(node_deps, 2)
    arolla.testing.assert_expr_equal_by_fingerprint(
        node_deps[0], arolla.literal(prototype)
    )
    arolla.testing.assert_expr_equal_by_fingerprint(
        node_deps[1], arolla.literal(arolla.namedtuple(y=2))
    )
    arolla.testing.assert_expr_equal_by_fingerprint(
        M.objects.make_object(*node_deps), expr
    )

  def test_binding_policy_eager_node_deps_with_prototype_and_attrs(self):
    prototype = arolla.eval(M.objects.make_object(x=1))
    attrs = arolla.namedtuple(y=2)
    expr = M.objects.make_object(prototype, attrs)
    node_deps = expr.node_deps
    self.assertLen(node_deps, 2)
    arolla.testing.assert_expr_equal_by_fingerprint(
        node_deps[0], arolla.literal(prototype)
    )
    arolla.testing.assert_expr_equal_by_fingerprint(
        node_deps[1], arolla.literal(attrs)
    )
    arolla.testing.assert_expr_equal_by_fingerprint(
        M.objects.make_object(*node_deps), expr
    )

  def test_binding_policy_leaf_node_deps(self):
    expr = M.objects.make_object(x=1, y=L.y)
    node_deps = expr.node_deps
    self.assertLen(node_deps, 2)
    arolla.testing.assert_expr_equal_by_fingerprint(
        node_deps[0], arolla.literal(arolla.unspecified())
    )
    arolla.testing.assert_expr_equal_by_fingerprint(
        node_deps[1], M.namedtuple.make(x=1, y=L.y)
    )
    arolla.testing.assert_expr_equal_by_fingerprint(
        M.objects.make_object(*node_deps), expr
    )

  def test_binding_policy_leaf_node_deps_with_prototype_and_attrs(self):
    expr = M.objects.make_object(L.prototype, L.attrs)
    node_deps = expr.node_deps
    self.assertLen(node_deps, 2)
    arolla.testing.assert_expr_equal_by_fingerprint(node_deps[0], L.prototype)
    arolla.testing.assert_expr_equal_by_fingerprint(node_deps[1], L.attrs)
    arolla.testing.assert_expr_equal_by_fingerprint(
        M.objects.make_object(*node_deps), expr
    )

  def test_binding_policy_attrs_and_kwargs_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'arguments `attrs` and `**kwargs` cannot be specified together'
        ),
    ):
      M.objects.make_object(L.prototype, L.attrs, x=1)


if __name__ == '__main__':
  absltest.main()
