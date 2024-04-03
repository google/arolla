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

"""Tests for multi_protopath."""

import itertools

from absl.testing import absltest
from arolla.codegen.io import accessors
from arolla.codegen.io import multi_protopath
from arolla.codegen.io import protopath


class MultiProtopathSingleValueTest(absltest.TestCase):

  def test_extract_single_value_protopath_accessors_modify_list(self):
    accessor_list = [
        ('x', accessors.path_accessor('.a')),
        ('y', protopath.Protopath.parse('a/z').accessor()),
        ('z', accessors.path_accessor('.b')),
        ('q', protopath.Protopath.parse('a/b').accessor()),
        ('t', protopath.Protopath.parse('a/b[:]').accessor()),
    ]
    new_list, root = multi_protopath.extract_single_value_protopath_accessors(
        accessor_list)
    self.assertListEqual([a[0] for a in new_list], ['x', 'z', 't'])
    self.assertListEqual([l.leaf_accessor_name for l in root.leaves()],
                         ['y', 'q'])

  def test_extract_single_value_protopath_accessors_respect_default_name(self):
    accessor = protopath.Protopath.parse('a/z').accessor()
    accessor_list = [
        ('', accessor),
        ('q', protopath.Protopath.parse('a/b').accessor()),
    ]
    _, root = multi_protopath.extract_single_value_protopath_accessors(
        accessor_list)
    self.assertListEqual([l.leaf_accessor_name for l in root.leaves()],
                         [accessor.default_name, 'q'])

  def test_extract_single_value_protopath_accessors_has_correct_accessor(self):
    accessor = protopath.Protopath.parse('a/z').accessor(cpp_type='int')
    accessor_list = [
        ('w', accessor),
        ('q', protopath.Protopath.parse('a/b').accessor()),
    ]
    _, root = multi_protopath.extract_single_value_protopath_accessors(
        accessor_list)
    self.assertListEqual([l.leaf_accessor.cpp_type for l in root.leaves()],
                         ['int', None])
    self.assertListEqual([l.leaf_accessor.protopath for l in root.leaves()],
                         ['a/z', 'a/b'])

  def test_define_protopath_tree(self):
    accessor_list = [('', protopath.Protopath.parse(ppath).accessor())
                     for ppath in ['a/b', 'a/z', 'x/y/q', 'x/y/count(w[:])']]
    _, root = multi_protopath.extract_single_value_protopath_accessors(
        accessor_list)
    self.assertEqual(
        multi_protopath.define_protopath_tree(
            root, multi_protopath.create_node_numeration(root)), """[]() {
  std::vector<std::vector<size_t>> tree(8);
  tree[2] = {0,1};
  tree[5] = {3,4};
  tree[6] = {5};
  tree[7] = {2,6};
  return tree;
}()""")

  def test_extract_single_value_protopath_accessors_tree(self):
    accessor_list = [('', protopath.Protopath.parse(ppath).accessor())
                     for ppath in ['a/b', 'a/z', 'x/y/q', 'x/y/count(w[:])']]
    _, root = multi_protopath.extract_single_value_protopath_accessors(
        accessor_list)
    self.assertIsNone(root.parent)
    self.assertIsNone(root.path_from_parent)
    self.assertIsNone(root.leaf_accessor)
    self.assertEmpty(root.leaf_accessor_name)
    self.assertFalse(root.is_size)
    self.assertFalse(root.is_leaf())
    self.assertTrue(root.has_size_leaf())
    self.assertTrue(root.has_optional_leaf())
    self.assertFalse(root.has_leaf_with_default_value())
    self.assertEqual(root.access_for_type('i'), 'i')
    self.assertEmpty(root.protopath())
    self.assertEqual(root.comment, 'protopath=`ROOT`')

    a, x = root.children
    self.assertIs(a.parent, root)
    self.assertIsNotNone(a.path_from_parent)
    self.assertIsNone(a.leaf_accessor)
    self.assertEmpty(a.leaf_accessor_name)
    self.assertFalse(a.is_size)
    self.assertFalse(a.is_leaf())
    self.assertFalse(a.has_size_leaf())
    self.assertTrue(a.has_optional_leaf())
    self.assertFalse(a.has_leaf_with_default_value())
    self.assertEqual(a.access_for_type('i'), 'i.a()')
    self.assertEqual(a.protopath(), 'a')
    self.assertEqual(a.comment, 'protopath=`a`')

    self.assertIs(x.parent, root)
    self.assertIsNotNone(x.path_from_parent)
    self.assertIsNone(x.leaf_accessor)
    self.assertEmpty(x.leaf_accessor_name)
    self.assertFalse(x.is_size)
    self.assertFalse(x.is_leaf())
    self.assertTrue(x.has_size_leaf())
    self.assertTrue(x.has_optional_leaf())
    self.assertFalse(x.has_leaf_with_default_value())
    self.assertEqual(x.access_for_type('i'), 'i.x()')
    self.assertEqual(x.protopath(), 'x')

    ab, az = a.children
    self.assertIs(ab.parent, a)
    self.assertIsNotNone(ab.path_from_parent)
    self.assertEqual(ab.leaf_accessor.protopath, 'a/b')
    self.assertEqual(ab.leaf_accessor_name, '/a/b')
    self.assertFalse(ab.is_size)
    self.assertTrue(ab.is_leaf())
    self.assertFalse(ab.has_size_leaf())
    self.assertTrue(ab.has_optional_leaf())
    self.assertEqual(ab.access_for_type('i'), 'i.a().b()')
    self.assertEqual(ab.protopath(), 'a/b')
    self.assertEqual(ab.comment, 'protopath=`a/b` name=`/a/b`')

    self.assertIs(az.parent, a)
    self.assertIsNotNone(az.path_from_parent)
    self.assertEqual(az.leaf_accessor.protopath, 'a/z')
    self.assertEqual(az.leaf_accessor_name, '/a/z')
    self.assertFalse(az.is_size)
    self.assertTrue(az.is_leaf())
    self.assertFalse(az.has_size_leaf())
    self.assertTrue(az.has_optional_leaf())
    self.assertEqual(az.access_for_type('i'), 'i.a().z()')
    self.assertEqual(az.protopath(), 'a/z')

    (xy,) = x.children
    self.assertIs(xy.parent, x)
    self.assertIsNotNone(xy.path_from_parent)
    self.assertIsNone(xy.leaf_accessor)
    self.assertEmpty(xy.leaf_accessor_name)
    self.assertFalse(xy.is_size)
    self.assertFalse(xy.is_leaf())
    self.assertTrue(xy.has_size_leaf())
    self.assertTrue(xy.has_optional_leaf())
    self.assertEqual(xy.access_for_type('i'), 'i.x().y()')
    self.assertEqual(xy.protopath(), 'x/y')

    xyq, xyw = xy.children
    self.assertIs(xyq.parent, xy)
    self.assertIsNotNone(xyq.path_from_parent)
    self.assertEqual(xyq.leaf_accessor.protopath, 'x/y/q')
    self.assertEqual(xyq.leaf_accessor_name, '/x/y/q')
    self.assertFalse(xyq.is_size)
    self.assertTrue(xyq.is_leaf())
    self.assertFalse(xyq.has_size_leaf())
    self.assertTrue(xyq.has_optional_leaf())
    self.assertEqual(xyq.access_for_type('i'), 'i.x().y().q()')
    self.assertEqual(xyq.protopath(), 'x/y/q')

    self.assertIs(xyw.parent, xy)
    self.assertIsNotNone(xyw.path_from_parent)
    self.assertEqual(xyw.leaf_accessor.protopath, 'x/y/count(w[:])')
    self.assertEqual(xyw.leaf_accessor_name, '/x/y/w/@size')
    self.assertTrue(xyw.is_size)
    self.assertTrue(xyw.is_leaf())
    self.assertTrue(xyw.has_size_leaf())
    self.assertFalse(xyw.has_optional_leaf())
    self.assertEqual(xyw.access_for_type('i'), 'i.x().y().w().size()')
    self.assertEqual(xyw.protopath(), 'x/y/count(w[:])')

    self.assertListEqual(root.post_order_nodes(),
                         [ab, az, a, xyq, xyw, xy, x, root])
    self.assertListEqual(a.post_order_nodes(), [ab, az, a])
    self.assertListEqual(x.post_order_nodes(), [xyq, xyw, xy, x])
    self.assertListEqual(xy.post_order_nodes(), [xyq, xyw, xy])

    self.assertListEqual(root.leaves(), [ab, az, xyq, xyw])
    self.assertListEqual(a.leaves(), [ab, az])
    self.assertListEqual(x.leaves(), [xyq, xyw])
    self.assertListEqual(xy.leaves(), [xyq, xyw])

    node_numeration = multi_protopath.create_node_numeration(root)
    self.assertDictEqual(node_numeration.leaf2id, {
        ab: 0,
        az: 1,
        xyq: 2,
        xyw: 3
    })
    self.assertDictEqual(node_numeration.intermediate2id, {
        a: 0,
        xy: 1,
        x: 2,
        root: 3
    })
    self.assertDictEqual(node_numeration.node2id, {
        ab: 0,
        az: 1,
        a: 2,
        xyq: 3,
        xyw: 4,
        xy: 5,
        x: 6,
        root: 7
    })

  def test_extract_single_value_protopath_accessors_tree_fictive_nodes(self):
    accessor_list = [('', protopath.Protopath.parse(ppath).accessor())
                     for ppath in [f'a/b[{i}]' for i in range(19)]]
    ppathes = [
        f'x[{i}]/y[{j}]' for i, j in itertools.product(range(15), range(11))
    ]
    accessor_list += [
        ('', protopath.Protopath.parse(ppath).accessor()) for ppath in ppathes
    ]
    _, root = multi_protopath.extract_single_value_protopath_accessors(
        accessor_list)
    self.assertLen(root.children, 2)
    self.assertFalse(root.is_fictive())
    r1, r2 = root.children
    # r1 and r2 are both fake nodes with 8 children
    self.assertIsNone(r1.path_from_parent)
    self.assertIsNone(r2.path_from_parent)
    self.assertLen(r1.children, 8)
    self.assertLen(r2.children, 8)

    a = r1.children[0]
    self.assertIsNotNone(a.path_from_parent)
    self.assertEqual(a.access_for_type('i'), 'i.a()')
    self.assertEqual(a.protopath(), 'a')
    self.assertEqual(a.comment, 'protopath=`a`')
    self.assertEqual(a.depth_without_fictive(), 1)
    self.assertListEqual([c.is_fictive() for c in a.children], [True] * 3)
    self.assertListEqual([c.comment for c in a.children],
                         ['protopath=`a` fictive'] * 3)
    self.assertListEqual([c.depth_without_fictive() for c in a.children],
                         [1] * 3)
    a1, a2, a3 = a.children
    self.assertIsNone(a1.path_from_parent)
    self.assertIsNone(a2.path_from_parent)
    self.assertIsNone(a3.path_from_parent)
    self.assertLen(a1.children, 8)
    self.assertLen(a2.children, 8)
    self.assertLen(a3.children, 3)
    for i, c in enumerate(a1.children + a2.children + a3.children):
      self.assertEqual(c.access_for_type('i'), f'i.a().b({i})')
      self.assertEqual(c.protopath(), f'a/b[{i}]')
      self.assertFalse(c.is_fictive())
      self.assertEqual(c.depth_without_fictive(), 2)

    for i, x in enumerate(r1.children[1:] + r2.children):
      self.assertIsNotNone(x.path_from_parent)
      self.assertEqual(x.access_for_type('i'), f'i.x({i})')
      self.assertEqual(x.protopath(), f'x[{i}]')
      self.assertListEqual([c.is_fictive() for c in x.children], [True] * 2)
      x1, x2 = x.children
      self.assertIsNone(x1.path_from_parent)
      self.assertEqual(x1.protopath(), f'x[{i}]')
      self.assertIsNone(x2.path_from_parent)
      self.assertEqual(x2.protopath(), f'x[{i}]')
      self.assertLen(x1.children, 8)
      self.assertLen(x2.children, 3)
      for j, y in enumerate(x1.children + x2.children):
        self.assertIsNotNone(y.path_from_parent)
        self.assertFalse(y.is_fictive())
        self.assertEqual(y.access_for_type('i'), f'i.x({i}).y({j})')
        self.assertEqual(y.protopath(), f'x[{i}]/y[{j}]')
        self.assertEqual(y.depth_without_fictive(), 2)

  def test_extract_single_value_protopath_accessors_tree_with_default(self):
    accessor_list = [
        ('', protopath.Protopath.parse(ppath).accessor())
        for ppath in ['a/b/c', 'a/z', 'a/w']
    ]

    def accessor_with_default(ppath):
      return protopath.Protopath.parse(
          ppath).scalar_accessor_with_default_value(
              cpp_type='int32_t', default_value_cpp='0')

    accessor_list += [
        ('', accessor_with_default(ppath)) for ppath in ['a/b/d', 'q/e']
    ]
    _, root = multi_protopath.extract_single_value_protopath_accessors(
        accessor_list
    )
    self.assertFalse(root.has_size_leaf())
    self.assertTrue(root.has_optional_leaf())
    self.assertTrue(root.has_leaf_with_default_value())

    a, q = root.children
    self.assertTrue(a.has_optional_leaf())
    self.assertTrue(a.has_leaf_with_default_value())

    self.assertFalse(q.has_optional_leaf())
    self.assertTrue(q.has_leaf_with_default_value())

    b, w, z = a.children
    self.assertTrue(b.has_optional_leaf())
    self.assertTrue(b.has_leaf_with_default_value())

    self.assertTrue(w.has_optional_leaf())
    self.assertFalse(w.has_leaf_with_default_value())

    self.assertTrue(z.has_optional_leaf())
    self.assertFalse(z.has_leaf_with_default_value())

  def test_size_leaf_ids(self):
    accessor_list = [
        ('', protopath.Protopath.parse(ppath).accessor())
        for ppath in ['a/b', 'a/count(z[:])', 'x/y/q', 'x/y/count(w[:])']
    ]
    _, root = multi_protopath.extract_single_value_protopath_accessors(
        accessor_list)
    node_numeration = multi_protopath.create_node_numeration(root)
    self.assertEqual(
        multi_protopath.size_leaf_ids(root, node_numeration), [1, 3])
    self.assertEqual(
        multi_protopath.size_leaf_ids(root.children[0], node_numeration), [1])
    self.assertEqual(
        multi_protopath.size_leaf_ids(root.children[1], node_numeration), [3])


class MultiProtopathMultiValueTest(absltest.TestCase):

  def test_extract_multi_value_protopath_accessors_modify_list(self):
    accessor_list = [
        ('x', accessors.path_accessor('.a')),
        ('y', protopath.Protopath.parse('a[:]/z').accessor()),
        ('z', protopath.Protopath.parse('a/q/count(c[:])').accessor()),
        ('q', protopath.Protopath.parse('a/b[:]').accessor()),
        ('t', protopath.Protopath.parse('a/w').accessor()),
    ]
    new_accessors, root = (
        multi_protopath.extract_multi_value_protopath_accessors(accessor_list))
    self.assertListEqual([a[0] for a in new_accessors], ['x', 'z', 't'])
    self.assertListEqual([l.leaf_accessor_name for l in root.leaves()],
                         ['y', 'q'])

  def test_extract_multi_value_protopath_accessors_respect_default_name(self):
    accessor = protopath.Protopath.parse('a[:]/z').accessor()
    accessor_list = [
        ('', accessor),
        ('q', protopath.Protopath.parse('a/b[:]').accessor()),
    ]
    _, root = multi_protopath.extract_multi_value_protopath_accessors(
        accessor_list)
    self.assertListEqual([l.leaf_accessor_name for l in root.leaves()],
                         [accessor.default_name, 'q'])

  def test_extract_multi_value_protopath_accessors_has_correct_accessor(self):
    accessor = protopath.Protopath.parse('a[:]/z').accessor(cpp_type='int')
    accessor_list = [
        ('w', accessor),
        ('q', protopath.Protopath.parse('a/b[:]').accessor()),
    ]
    _, root = multi_protopath.extract_multi_value_protopath_accessors(
        accessor_list)
    self.assertListEqual([l.leaf_accessor.cpp_type for l in root.leaves()],
                         ['int', None])
    self.assertListEqual([l.leaf_accessor.protopath for l in root.leaves()],
                         ['a[:]/z', 'a/b[:]'])

  def test_extract_multi_value_protopath_accessors_tree(self):
    paths = [
        'a/b[:]', 'a/z[:]', 'x[:]/y[:]/count(q[:])', 'x[:]/y[:]/w[:]',
        'x[:]/e/z[:]'
    ]
    accessor_list = [
        ('', protopath.Protopath.parse(ppath).accessor()) for ppath in paths
    ]
    _, root = multi_protopath.extract_multi_value_protopath_accessors(
        accessor_list)
    self.assertIsNone(root.parent)
    self.assertIsNone(root.path_from_parent_single)
    self.assertIsNone(root.path_from_parent_multi)
    self.assertIsNone(root.leaf_accessor)
    self.assertEmpty(root.leaf_accessor_name)
    self.assertFalse(root.is_leaf())
    self.assertFalse(root.is_size)
    self.assertEqual(root.access_for_type('i'), 'i')
    self.assertEmpty(root.protopath())
    self.assertFalse(root.is_repeated())
    self.assertEqual(root.repeated_depth(), 0)
    self.assertEqual(root.depth_without_fictive(), 0)
    self.assertTrue(root.is_ancestor_of(root))
    self.assertListEqual(root.path_from_ancestor(root), [root])
    self.assertEqual(root.ancestor_with_branch(), root)

    a, x = root.children
    self.assertIs(a.parent, root)
    self.assertIsNotNone(a.path_from_parent_single)
    self.assertIsNone(a.path_from_parent_multi)
    self.assertIsNone(a.leaf_accessor)
    self.assertEmpty(a.leaf_accessor_name)
    self.assertFalse(a.is_leaf())
    self.assertFalse(a.is_size)
    self.assertEqual(a.access_for_type('i'), 'i.a()')
    self.assertEqual(a.protopath(), 'a')
    self.assertFalse(a.is_repeated())
    self.assertEqual(a.repeated_depth(), 0)
    self.assertEqual(a.depth_without_fictive(), 1)
    self.assertTrue(root.is_ancestor_of(a))
    self.assertFalse(a.is_ancestor_of(root))
    self.assertFalse(a.is_ancestor_of(x))
    self.assertListEqual(a.path_from_ancestor(root), [root, a])
    self.assertListEqual(a.path_from_ancestor(a), [a])
    self.assertEqual(a.ancestor_with_branch(), root)

    self.assertIs(x.parent, root)
    self.assertIsNone(x.path_from_parent_single)
    self.assertIsNotNone(x.path_from_parent_multi)
    self.assertIsNone(x.leaf_accessor)
    self.assertEmpty(x.leaf_accessor_name)
    self.assertFalse(x.is_leaf())
    self.assertFalse(x.is_size)
    self.assertEqual(x.access_for_type('i'), 'i.x(0)')
    self.assertEqual(x.protopath(), 'x[:]')
    self.assertTrue(x.is_repeated())
    self.assertEqual(x.repeated_depth(), 1)
    self.assertEqual(x.depth_without_fictive(), 1)
    self.assertTrue(x.is_repeated_always_present())
    self.assertTrue(root.is_ancestor_of(x))
    self.assertFalse(x.is_ancestor_of(root))
    self.assertFalse(x.is_ancestor_of(a))
    self.assertListEqual(x.path_from_ancestor(root), [root, x])
    self.assertListEqual(x.path_from_ancestor(x), [x])
    self.assertEqual(x.ancestor_with_branch(), root)

    ab, az = a.children
    self.assertIs(ab.parent, a)
    self.assertIsNotNone(ab.path_from_parent_multi)
    self.assertIsNone(ab.path_from_parent_single)
    self.assertEqual(ab.leaf_accessor.protopath, 'a/b[:]')
    self.assertEqual(ab.leaf_accessor_name, '/a/b')
    self.assertTrue(ab.is_leaf())
    self.assertFalse(ab.is_size)
    self.assertEqual(ab.access_for_type('i'), 'i.a().b(0)')
    self.assertEqual(ab.protopath(), 'a/b[:]')
    self.assertTrue(ab.is_repeated())
    self.assertEqual(ab.repeated_depth(), 1)
    self.assertEqual(ab.depth_without_fictive(), 2)
    self.assertTrue(ab.is_repeated_always_present())
    self.assertTrue(root.is_ancestor_of(ab))
    self.assertTrue(a.is_ancestor_of(ab))
    self.assertFalse(ab.is_ancestor_of(root))
    self.assertFalse(ab.is_ancestor_of(a))
    self.assertListEqual(ab.path_from_ancestor(root), [root, a, ab])
    self.assertListEqual(ab.path_from_ancestor(a), [a, ab])
    self.assertEqual(ab.repeated_access_ancestor(), ab)
    self.assertEqual(ab.ancestor_with_branch(), a)

    self.assertIs(az.parent, a)
    self.assertIsNotNone(az.path_from_parent_multi)
    self.assertIsNone(az.path_from_parent_single)
    self.assertEqual(az.leaf_accessor.protopath, 'a/z[:]')
    self.assertEqual(az.leaf_accessor_name, '/a/z')
    self.assertTrue(az.is_leaf())
    self.assertFalse(az.is_size)
    self.assertEqual(az.access_for_type('i'), 'i.a().z(0)')
    self.assertEqual(az.protopath(), 'a/z[:]')
    self.assertTrue(az.is_repeated())
    self.assertEqual(az.repeated_depth(), 1)
    self.assertEqual(az.depth_without_fictive(), 2)
    self.assertTrue(az.is_repeated_always_present())
    self.assertTrue(root.is_ancestor_of(az))
    self.assertTrue(a.is_ancestor_of(az))
    self.assertFalse(az.is_ancestor_of(root))
    self.assertFalse(az.is_ancestor_of(a))
    self.assertListEqual(az.path_from_ancestor(root), [root, a, az])
    self.assertListEqual(az.path_from_ancestor(a), [a, az])
    self.assertEqual(az.repeated_access_ancestor(), az)
    self.assertEqual(az.ancestor_with_branch(), a)

    (xy, xe) = x.children
    self.assertIs(xy.parent, x)
    self.assertIsNotNone(xy.path_from_parent_multi)
    self.assertIsNone(xy.path_from_parent_single)
    self.assertIsNone(xy.leaf_accessor)
    self.assertEmpty(xy.leaf_accessor_name)
    self.assertFalse(xy.is_leaf())
    self.assertFalse(xy.is_size)
    self.assertEqual(xy.access_for_type('i'), 'i.x(0).y(0)')
    self.assertEqual(xy.protopath(), 'x[:]/y[:]')
    self.assertTrue(xy.is_repeated())
    self.assertEqual(xy.repeated_depth(), 2)
    self.assertEqual(xy.depth_without_fictive(), 2)
    self.assertTrue(xy.is_repeated_always_present())
    self.assertTrue(root.is_ancestor_of(xy))
    self.assertTrue(x.is_ancestor_of(xy))
    self.assertFalse(xy.is_ancestor_of(root))
    self.assertFalse(xy.is_ancestor_of(x))
    self.assertListEqual(xy.path_from_ancestor(root), [root, x, xy])
    self.assertListEqual(xy.path_from_ancestor(x), [x, xy])

    self.assertEqual(xe.repeated_depth(), 1)
    self.assertEqual(xe.depth_without_fictive(), 2)
    self.assertFalse(xe.is_repeated_always_present())
    self.assertFalse(xe.is_size)
    self.assertEqual(xe.protopath(), 'x[:]/e')

    xyq, xyw = xy.children
    self.assertIs(xyq.parent, xy)
    self.assertIsNotNone(xyq.path_from_parent_single)
    self.assertIsNone(xyq.path_from_parent_multi)
    self.assertEqual(xyq.leaf_accessor.protopath, 'x[:]/y[:]/count(q[:])')
    self.assertEqual(xyq.protopath(), 'x[:]/y[:]/count(q[:])')
    self.assertEqual(xyq.leaf_accessor_name, '/x/y/q/@size')
    self.assertTrue(xyq.is_leaf())
    self.assertTrue(xyq.is_size)
    self.assertEqual(xyq.access_for_type('i'), 'i.x(0).y(0).q().size()')
    self.assertTrue(xyq.is_repeated())
    self.assertEqual(xyq.repeated_depth(), 2)
    self.assertEqual(xyq.depth_without_fictive(), 3)
    self.assertTrue(xyq.is_repeated_always_present())
    self.assertTrue(root.is_ancestor_of(xyq))
    self.assertTrue(x.is_ancestor_of(xyq))
    self.assertTrue(xy.is_ancestor_of(xyq))
    self.assertFalse(xyq.is_ancestor_of(root))
    self.assertFalse(xyq.is_ancestor_of(xy))
    self.assertListEqual(xyq.path_from_ancestor(root), [root, x, xy, xyq])
    self.assertListEqual(xyq.path_from_ancestor(x), [x, xy, xyq])
    self.assertEqual(xyq.repeated_access_ancestor(), xy)

    (xez,) = xe.children
    self.assertFalse(xez.is_repeated_always_present())

    self.assertIs(xyw.parent, xy)
    self.assertIsNotNone(xyw.path_from_parent_multi)
    self.assertIsNone(xyw.path_from_parent_single)
    self.assertEqual(xyw.leaf_accessor.protopath, 'x[:]/y[:]/w[:]')
    self.assertEqual(xyw.protopath(), 'x[:]/y[:]/w[:]')
    self.assertEqual(xyw.leaf_accessor_name, '/x/y/w')
    self.assertTrue(xyw.is_leaf())
    self.assertFalse(xyw.is_size)
    self.assertEqual(xyw.access_for_type('i'), 'i.x(0).y(0).w(0)')
    self.assertTrue(xyw.is_repeated())
    self.assertEqual(xyw.repeated_depth(), 3)
    self.assertEqual(xyw.depth_without_fictive(), 3)
    self.assertTrue(xyw.is_repeated_always_present())
    self.assertTrue(root.is_ancestor_of(xyw))
    self.assertTrue(x.is_ancestor_of(xyw))
    self.assertTrue(xy.is_ancestor_of(xyw))
    self.assertFalse(xyw.is_ancestor_of(root))
    self.assertFalse(xyw.is_ancestor_of(xy))
    self.assertListEqual(xyw.path_from_ancestor(root), [root, x, xy, xyw])
    self.assertListEqual(xyw.path_from_ancestor(x), [x, xy, xyw])
    self.assertEqual(xyw.repeated_access_ancestor(), xyw)

    self.assertListEqual(root.post_order_nodes(),
                         [ab, az, a, xyq, xyw, xy, xez, xe, x, root])
    self.assertListEqual(a.post_order_nodes(), [ab, az, a])
    self.assertListEqual(x.post_order_nodes(), [xyq, xyw, xy, xez, xe, x])
    self.assertListEqual(xy.post_order_nodes(), [xyq, xyw, xy])
    self.assertListEqual(xe.post_order_nodes(), [xez, xe])

    self.assertListEqual(root.leaves(), [ab, az, xyq, xyw, xez])
    self.assertListEqual(a.leaves(), [ab, az])
    self.assertListEqual(x.leaves(), [xyq, xyw, xez])
    self.assertListEqual(xy.leaves(), [xyq, xyw])

  def test_extract_multi_value_protopath_accessors_tree_fictive_nodes(self):
    accessor_list = [('', protopath.Protopath.parse(ppath).accessor())
                     for ppath in [f'a[:]/b[{i}]' for i in range(19)]]
    ppathes = [
        f'x_{i}[:]/y[{j}]' for i, j in itertools.product(range(15), range(11))
    ]
    accessor_list += [
        ('', protopath.Protopath.parse(ppath).accessor()) for ppath in ppathes
    ]
    _, root = multi_protopath.extract_multi_value_protopath_accessors(
        accessor_list)
    self.assertLen(root.children, 2)
    self.assertFalse(root.is_fictive())
    self.assertEqual(root.non_fictive_ancestor(), root)
    r1, r2 = root.children
    # r1 and r2 are both fake nodes with 8 children
    self.assertTrue(r1.is_fictive())
    self.assertEqual(r1.non_fictive_ancestor(), root)
    self.assertTrue(r2.is_fictive())
    self.assertEqual(r2.non_fictive_ancestor(), root)
    self.assertLen(r1.children, 8)
    self.assertLen(r2.children, 8)

    a = r1.children[0]
    self.assertIsNotNone(a.path_from_parent_multi)
    self.assertEqual(a.non_fictive_ancestor(), a)
    self.assertEqual(a.access_for_type('i'), 'i.a(0)')
    self.assertListEqual([c.is_fictive() for c in a.children], [True] * 3)
    self.assertListEqual([c.non_fictive_ancestor() for c in a.children],
                         [a] * 3)
    a1, a2, a3 = a.children
    self.assertLen(a1.children, 8)
    self.assertLen(a2.children, 8)
    self.assertLen(a3.children, 3)
    for i, c in enumerate(a1.children + a2.children + a3.children):
      self.assertIsNotNone(c.path_from_parent_single)
      self.assertFalse(c.is_fictive())
      self.assertEqual(c.non_fictive_ancestor(), c)
      self.assertEqual(c.access_for_type('i'), f'i.a(0).b({i})')

    for i, x in enumerate(r1.children[1:] + r2.children):
      self.assertIsNotNone(x.path_from_parent_multi)
      self.assertEqual(x.access_for_type('i'), f'i.x_{i}(0)')
      self.assertListEqual([c.is_fictive() for c in x.children], [True] * 2)
      self.assertListEqual([c.non_fictive_ancestor() for c in x.children],
                           [x] * 2)
      x1, x2 = x.children
      self.assertLen(x1.children, 8)
      self.assertLen(x2.children, 3)
      for j, y in enumerate(x1.children + x2.children):
        self.assertIsNotNone(y.path_from_parent_single)
        self.assertFalse(y.is_fictive())
        self.assertEqual(y.non_fictive_ancestor(), y)
        self.assertEqual(y.access_for_type('i'), f'i.x_{i}(0).y({j})')

  def test_intermediate_start_node_collect_parent_values_and_sizes(self):
    collect_values = multi_protopath.IntermediateCollectionInfo.COLLECT_VALUES
    collect_parent_values_and_sizes = (
        multi_protopath.IntermediateCollectionInfo
        .COLLECT_PARENT_VALUES_AND_SIZES)
    accessor_list = [
        ('', protopath.Protopath.parse(ppath).accessor())
        for ppath in ['a/b[:]', 'b[:]/z[:]', 'x[:]/y[:]/q', 'r[:]/w']
    ]
    _, root = multi_protopath.extract_multi_value_protopath_accessors(
        accessor_list)
    a, b, x, r = root.children
    (ab,) = a.children
    (bz,) = b.children
    (xy,) = x.children
    (xyq,) = xy.children
    (rw,) = r.children
    self.assertEqual(ab.intermediate_start_node(),
                     (ab, collect_parent_values_and_sizes))
    self.assertEqual(bz.intermediate_start_node(),
                     (bz, collect_parent_values_and_sizes))
    self.assertEqual(xyq.intermediate_start_node(),
                     (xy, collect_parent_values_and_sizes))
    self.assertEqual(rw.intermediate_start_node(),
                     (r, collect_parent_values_and_sizes))

    nodes_for_intermediate_collection = (
        multi_protopath.nodes_for_intermediate_collection(root))
    self.assertListEqual(  # with order
        list(nodes_for_intermediate_collection.items()),
        [
            (a, collect_values),
            (bz, collect_parent_values_and_sizes),
            (b, collect_values),
            (xy, collect_parent_values_and_sizes),
            (x, collect_values),
            # (r, collect_parent_values_and_sizes)
            # we compute top level size on the fly
        ])
    self.assertDictEqual(  # no order
        multi_protopath.nodes_with_descendant_for_intermediate_collection(
            root, nodes_for_intermediate_collection),
        {
            root: collect_values,
            a: collect_values,
            b: collect_values,
            bz: collect_parent_values_and_sizes,
            x: collect_values,
            xy: collect_parent_values_and_sizes,
        })

  def test_intermediate_start_node_collect_values(self):
    collect_values = multi_protopath.IntermediateCollectionInfo.COLLECT_VALUES

    protopathes = [
        'a/b[:]/x',
        'a/b[:]/y',  # a/b[:]
        'x[:]/y[:]/w/z',
        'x[:]/y[:]/w/t',  # x[:]/y[:] since w access is cheap
    ] + [  # x[:]/y[:]/q since we have too many leaves to access
        f'x[:]/y[:]/q/z[{i}]' for i in range(10)
    ]
    accessor_list = [('', protopath.Protopath.parse(ppath).accessor())
                     for ppath in protopathes]
    new_list, root = multi_protopath.extract_multi_value_protopath_accessors(
        accessor_list)
    self.assertEmpty(new_list)
    a, x = root.children
    (ab,) = a.children
    (abx, aby) = ab.children
    (xy,) = x.children
    (xyw, xyq) = xy.children
    (xywz, xywt) = xyw.children
    self.assertEqual(abx.intermediate_start_node(), (ab, collect_values))
    self.assertEqual(aby.intermediate_start_node(), (ab, collect_values))
    self.assertEqual(xywz.intermediate_start_node(), (xy, collect_values))
    self.assertEqual(xywt.intermediate_start_node(), (xy, collect_values))
    for xyqz in xyq.leaves():
      self.assertEqual(xyqz.intermediate_start_node(), (xyq, collect_values))
    nodes_for_intermediate_collection = (
        multi_protopath.nodes_for_intermediate_collection(root))
    self.assertListEqual(  # with order
        list(nodes_for_intermediate_collection.items()), [(ab, collect_values),
                                                          (xyq, collect_values),
                                                          (xy, collect_values)])
    self.assertDictEqual(  # no order
        multi_protopath.nodes_with_descendant_for_intermediate_collection(
            root, nodes_for_intermediate_collection),
        {
            root: collect_values,
            a: collect_values,
            ab: collect_values,
            x: collect_values,
            xy: collect_values,
            xyq: collect_values,
        })

  def test_intermediate_start_node_collect_values_map(self):
    collect_values = multi_protopath.IntermediateCollectionInfo.COLLECT_VALUES

    protopathes = [  # x[:] since we have only one leaf
        'x[:]/map["a"]/z'
    ] + [  # x[:]/map["b"] since map is very expensive
        'x[:]/map["b"]/z', 'x[:]/map["b"]/t'
    ]
    accessor_list = [('', protopath.Protopath.parse(ppath).accessor())
                     for ppath in protopathes]
    new_list, root = multi_protopath.extract_multi_value_protopath_accessors(
        accessor_list)
    self.assertEmpty(new_list)
    (x,) = root.children
    xa, xb = x.children
    (xaz,) = xa.children
    xbz, xbt = xb.children
    self.assertEqual(xaz.intermediate_start_node(), (x, collect_values))
    self.assertEqual(xbz.intermediate_start_node(), (xb, collect_values))
    self.assertEqual(xbt.intermediate_start_node(), (xb, collect_values))

  def test_intermediate_start_node_collect_values_extension(self):
    collect_values = multi_protopath.IntermediateCollectionInfo.COLLECT_VALUES

    protopathes = [  # x[:]/Ext::abc::ext since extension is expensive
        'x[:]/Ext::abc::a/z', 'x[:]/Ext::abc::a/t'
    ]
    accessor_list = [('', protopath.Protopath.parse(ppath).accessor())
                     for ppath in protopathes]
    new_list, root = multi_protopath.extract_multi_value_protopath_accessors(
        accessor_list)
    self.assertEmpty(new_list)
    (x,) = root.children
    (xa,) = x.children
    xaz, xat = xa.children
    self.assertEqual(xaz.intermediate_start_node(), (xa, collect_values))
    self.assertEqual(xat.intermediate_start_node(), (xa, collect_values))


if __name__ == '__main__':
  absltest.main()
