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

"""Tests for arolla.codegen.accessors."""

from absl.testing import absltest

from arolla.codegen.io import accessors
from arolla.codegen.io import cpp


class AccessorsTest(absltest.TestCase):

  def test_accessor(self):
    a = accessors.Accessor('[](const auto& i) { return i; }', [])
    self.assertEqual(a.required_includes, set())
    self.assertEqual(a.lambda_str, '[](const auto& i) { return i; }')
    with self.assertRaisesRegex(ValueError, 'name is not specified'):
      _ = a.default_name

    a = accessors.Accessor(
        '[](const auto& i) { return i; }',
        [cpp.Include('a.h'), cpp.Include('b.h')],
        'identity',
    )
    self.assertEqual(
        a.required_includes, {cpp.Include('a.h'), cpp.Include('b.h')}
    )
    self.assertEqual(a.default_name, 'identity')

  def test_path_accessor(self):
    a = accessors.path_accessor('.a')
    self.assertEqual(a.required_includes, set())
    self.assertEqual(a.lambda_str, '[](const auto& input) { return input.a; }')
    with self.assertRaisesRegex(ValueError, '.*name is not specified.*'):
      _ = a.default_name
    b = accessors.path_accessor('.b', 'b')
    self.assertEqual(b.default_name, 'b')

  def test_accessor_list_errors(self):
    a = accessors.Accessor(
        'return input;', [cpp.Include('b.h'), cpp.Include('a.h')]
    )
    b = accessors.Accessor(
        'return input.b;', [cpp.Include('b.h'), cpp.Include('a.h')]
    )
    with self.assertRaisesRegex(ValueError, 'Duplicate.*b'):
      accessors.AccessorsList(
          [('a', a), ('b', b), ('a', a), ('c', a), ('b', a)]
      )
    with self.assertRaisesRegex(ValueError, 'name is not specified'):
      accessors.AccessorsList([('', a)])

  def test_sorted_accessor_list(self):
    a = accessors.Accessor(
        '[](const auto& input) { return input.a(); }',
        [cpp.Include('b.h'), cpp.Include('a.h')],
    )
    b = accessors.Accessor(
        '[](const auto& input) { return input.b(); }',
        [cpp.Include('c.h'), cpp.Include('a.h')],
        'b',
    )
    self.assertListEqual(
        accessors.sorted_accessor_list([('x', a), ('b', b)]),
        [('b', b), ('x', a)],
    )

  def test_sorted_accessor_list_duplicates_no_error(self):
    a = accessors.Accessor(
        'return input;', [cpp.Include('b.h'), cpp.Include('a.h')]
    )
    b = accessors.Accessor(
        'return input.b;', [cpp.Include('b.h'), cpp.Include('a.h')]
    )
    self.assertListEqual(
        accessors.sorted_accessor_list(
            [('c', a), ('a', a), ('b', b), ('a', a), ('b', b)]
        ),
        [('a', a), ('b', b), ('c', a)],
    )

  def test_sorted_accessor_list_errors(self):
    a = accessors.Accessor(
        'return input;', [cpp.Include('b.h'), cpp.Include('a.h')]
    )
    b = accessors.Accessor(
        'return input.b;', [cpp.Include('b.h'), cpp.Include('a.h')]
    )
    with self.assertRaisesRegex(ValueError, 'Duplicate.*b'):
      accessors.sorted_accessor_list(
          [('a', a), ('b', b), ('a', a), ('c', a), ('b', a)]
      )

  def test_split_accessors_into_shards(self):
    a = accessors.Accessor(
        '[](const auto& input) { return input.a(); }',
        [cpp.Include('b.h'), cpp.Include('a.h')],
    )
    b = accessors.Accessor(
        '[](const auto& input) { return input.b(); }',
        [cpp.Include('c.h'), cpp.Include('a.h')],
        'b',
    )
    two_accessors = [('x', a), ('b', b)]
    with self.subTest('zero_shards'):
      self.assertListEqual(
          list(accessors.split_accessors_into_shards(two_accessors, 0)),
          [two_accessors],
      )
    with self.subTest('one_shard'):
      self.assertListEqual(
          list(accessors.split_accessors_into_shards(two_accessors, 1)),
          [two_accessors],
      )
    with self.subTest('two_shard'):
      self.assertListEqual(
          list(accessors.split_accessors_into_shards(two_accessors * 2, 2)),
          [two_accessors, two_accessors],
      )
    with self.subTest('three_shard'):
      self.assertListEqual(
          list(accessors.split_accessors_into_shards(two_accessors * 5, 3)),
          [
              two_accessors * 2,
              two_accessors + [('x', a)],
              [('b', b)] + two_accessors,
          ],
      )
    with self.subTest('too_many_shards'):
      self.assertListEqual(
          list(accessors.split_accessors_into_shards(two_accessors, 100)),
          [[a] for a in two_accessors],
      )

  def test_accessor_list(self):
    a = accessors.Accessor(
        '[](const auto& input) { return input.a(); }',
        [cpp.Include('b.h'), cpp.Include('a.h')],
    )
    b = accessors.Accessor(
        '[](const auto& input) { return input.b(); }',
        [cpp.Include('c.h'), cpp.Include('a.h')],
        'b',
    )
    accessors_list = accessors.AccessorsList([('a', a), ('', b)])
    self.assertLen(accessors_list, 2)
    self.assertEqual(accessors_list.names, ['a', 'b'])
    self.assertEqual(
        accessors_list.required_includes,
        [cpp.Include('a.h'), cpp.Include('b.h'), cpp.Include('c.h')],
    )
    self.assertEqual(
        [acc.lambda_str for acc in accessors_list.accessors],
        [
            '[](const auto& input) { return input.a(); }',
            '[](const auto& input) { return input.b(); }',
        ],
    )

  def test_accessor_list_shard(self):
    name_fn = lambda i: 'a%d' % (i + 10000)
    accessors_list = accessors.AccessorsList([
        ('a', accessors.Accessor(';', [])),
        ('b', accessors.Accessor(';', [])),
        ('c', accessors.Accessor(';', [])),
    ])
    self.assertLen(accessors_list.shards(2), 2)
    self.assertEqual(accessors_list.shards(2)[0].names, ['a', 'b'])
    self.assertEqual(accessors_list.shards(2)[1].names, ['c'])

    accessors_list = accessors.AccessorsList(
        [
            (name_fn(i), accessors.Accessor('return input.a();', []))
            for i in range(999)
        ]
    )
    self.assertLen(accessors_list, 999)
    self.assertLen(accessors_list.shards(1), 999)
    self.assertLen(accessors_list.shards(2), 500)

    self.assertLen(accessors_list.shards(999), 1)
    self.assertEqual(
        accessors_list.shards(999)[0].names, [name_fn(j) for j in range(999)]
    )

    self.assertLen(accessors_list.shards(10000), 1)
    self.assertEqual(
        accessors_list.shards(999)[0].names, [name_fn(j) for j in range(999)]
    )

    self.assertLen(accessors_list.shards(100), 10)
    for i, accessors_shard in enumerate(accessors_list.shards(100)):
      self.assertEqual(
          accessors_shard.names,
          [name_fn(j) for j in range(i * 100, min(i * 100 + 100, 999))],
      )


if __name__ == '__main__':
  absltest.main()
