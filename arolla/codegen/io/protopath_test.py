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

"""Tests for arolla.codegen.protopath."""

import itertools

from absl.testing import absltest
from absl.testing import parameterized

from arolla.codegen.io import array_generator
from arolla.codegen.io import cpp
from arolla.codegen.io import protopath
from arolla.codegen.io import table
from arolla.proto import test_extension_pb2
from arolla.proto import test_pb2
from arolla.proto import test_proto3_pb2

OPTIONAL_INCLUDES = frozenset({
    cpp.Include('arolla/util/map.h'),
    cpp.Include('arolla/proto/types.h'),
})

DENSE_ARRAY_INCLUDES = frozenset({
    cpp.Include('arolla/util/map.h'),
    cpp.Include('arolla/dense_array/dense_array.h'),
    cpp.Include('arolla/dense_array/qtype/types.h'),
    cpp.Include('arolla/proto/types.h'),
    cpp.Include('arolla/qtype/qtype.h')
})


class ProtopathTest(parameterized.TestCase):

  def assertEqualIgnoringSpaces(self, a, b):

    def normalize(x):
      x = ' '.join(x.split())
      # ignore spaces after symbol where clang-format break often
      # this helps to have golden tests clang-formatted
      for left_non_break in '(){}<>[]:':
        x = x.replace(left_non_break + ' ', left_non_break)
      # separate by new lines to simplify reading the diff
      return '\n'.join(x.split())

    self.assertEqual(normalize(a), normalize(b))

  def test_original_container_input_errors(self):
    for path in ['a', 'a[0]', 'a["a"]', 'a[:]@key', 'a[:]@value', 'a[:]']:
      self.assertFalse(
          protopath._OriginalInputContainerElement.is_this_type(path))
      with self.assertRaisesRegex(ValueError, 'Not a original container.*'):
        protopath._OriginalInputContainerElement(path)

  def test_original_container_input(self):
    path = '[:]'
    self.assertTrue(protopath._OriginalInputContainerElement.is_this_type(path))
    slice_el = protopath._OriginalInputContainerElement(path)
    self.assertEqual(slice_el, protopath._OriginalInputContainerElement(path))
    self.assertNotEqual(slice_el, protopath._RangeSliceElement('a[:]'))

    self.assertTrue(slice_el.support_mutable)
    self.assertEqual(slice_el.access_for_type('i'), 'i[0]')
    self.assertEqual(slice_el.protopath_element(), '[:]')
    self.assertEqualIgnoringSpaces(
        slice_el.open_loop('x', 'inp'), 'for (const auto& x : inp) {')
    self.assertEqualIgnoringSpaces(
        slice_el.open_loop('x', 'inp', is_mutable=True),
        'for (auto& x : inp) {')
    self.assertEqual(slice_el.access_for_size, '.size()')
    self.assertEqual(
        slice_el.child_table_path(table.TablePath()), table.TablePath())
    self.assertEqual(
        slice_el.size_column_path(table.TablePath()),
        table.TablePath().Size(''))

  def test_range_slice_element(self):
    for path in ['a', 'a[0]', 'a["a"]', 'a[:]@key', 'a[:]@value']:
      self.assertFalse(protopath._RangeSliceElement.is_this_type(path))
      with self.assertRaisesRegex(ValueError, 'Not a range.*'):
        protopath._RangeSliceElement(path)
    path = 'abc[:]'
    self.assertTrue(protopath._RangeSliceElement.is_this_type(path))
    slice_el = protopath._RangeSliceElement(path)
    self.assertTrue(slice_el.support_mutable)
    self.assertEqual(slice_el.access_for_type('i'), 'i.abc(0)')
    self.assertEqual(slice_el.protopath_element(), 'abc[:]')
    self.assertEqualIgnoringSpaces(
        slice_el.open_loop('x', 'inp'), 'for (const auto& x : inp.abc()) {')
    self.assertEqualIgnoringSpaces(
        slice_el.open_loop('x', 'inp', is_mutable=True),
        'for (auto& x : *inp.mutable_abc()) {')
    self.assertEqual(slice_el.access_for_size, '.abc().size()')
    self.assertEqual(
        slice_el.child_table_path(table.TablePath()),
        table.TablePath().Child('abc'))

  def test_map_keys_access_element_errors(self):
    for path in ['a', 'a[0]', 'a["a"]', 'a[:]', 'a[:]@value']:
      self.assertFalse(protopath._MapKeysAccessElement.is_this_type(path))
      with self.assertRaisesRegex(ValueError, 'Not a map key.*'):
        protopath._MapKeysAccessElement(path)

  def test_map_keys_access_element(self):
    path = 'abc[:]@key'
    self.assertTrue(protopath._MapKeysAccessElement.is_this_type(path))
    map_key_el = protopath._MapKeysAccessElement(path)
    self.assertEqual(map_key_el, protopath._MapKeysAccessElement(path))
    self.assertNotEqual(map_key_el, protopath._RangeSliceElement('a[:]'))
    self.assertFalse(map_key_el.support_mutable)
    self.assertEqual(map_key_el.access_for_type('i'), 'i.abc().begin()->first')
    self.assertEqual(map_key_el.protopath_element(), 'abc[:]@key')
    self.assertEqualIgnoringSpaces(
        map_key_el.open_loop('x', 'inp'),
        'for (const auto& x : ::arolla::SortedMapKeys(inp.abc())) {',
    )
    self.assertEqual(map_key_el.access_for_size, '.abc().size()')
    self.assertEqual(
        map_key_el.child_table_path(table.TablePath()),
        table.TablePath().Child('abc').MapKeys())

  def test_map_values_access_element_errors(self):
    for path in ['a', 'a[0]', 'a["a"]', 'a[:]', 'a[:]@key']:
      self.assertFalse(protopath._MapValuesAccessElement.is_this_type(path))
      with self.assertRaisesRegex(ValueError, 'Not a map value.*'):
        protopath._MapValuesAccessElement(path)

  def test_map_values_access_element(self):
    path = 'abc[:]@value'
    self.assertTrue(protopath._MapValuesAccessElement.is_this_type(path))
    map_value_el = protopath._MapValuesAccessElement(path)
    self.assertEqual(map_value_el, protopath._MapValuesAccessElement(path))
    self.assertNotEqual(map_value_el, protopath._RangeSliceElement('a[:]'))
    self.assertTrue(map_value_el.support_mutable)
    self.assertEqual(
        map_value_el.access_for_type('i'), 'i.abc().begin()->second')
    self.assertEqual(map_value_el.protopath_element(), 'abc[:]@value')
    self.assertEqualIgnoringSpaces(
        map_value_el.open_loop('x', 'inp'),
        """
for (const auto& x_key : ::arolla::SortedMapKeys(inp.abc())) {
  const auto& x = inp.abc().at(x_key);""",
    )
    self.assertEqualIgnoringSpaces(
        map_value_el.open_loop('x', 'inp', is_mutable=True),
        """
for (auto& x_key : ::arolla::SortedMapKeys(inp.abc())) {
  auto& x = inp.mutable_abc()->at(x_key);""",
    )
    self.assertEqual(map_value_el.access_for_size, '.abc().size()')
    self.assertEqual(
        map_value_el.child_table_path(table.TablePath()),
        table.TablePath().Child('abc').MapValues())

  def test_parse_multi_value_element(self):
    self.assertIsNone(protopath._parse_multi_value_element('a["key"]'))
    el = protopath._parse_multi_value_element('a[:]')
    self.assertIsNotNone(el)
    if el is not None:  # pytype require an if
      self.assertTrue(el.support_mutable)
      self.assertEqual(el.access_for_type('i'), 'i.a(0)')
    el = protopath._parse_multi_value_element('a[:]@key')
    self.assertIsNotNone(el)
    if el is not None:  # pytype require an if
      self.assertFalse(el.support_mutable)
      self.assertEqual(el.access_for_type('i'), 'i.a().begin()->first')
    el = protopath._parse_multi_value_element('a[:]@value')
    self.assertIsNotNone(el)
    if el is not None:  # pytype require an if
      self.assertTrue(el.support_mutable)
      self.assertEqual(el.access_for_type('i'), 'i.a().begin()->second')

  def test_regular_element_errors(self):
    for path in ['a[:]', 'a[0]', 'a@k', 'a[:]@key', 'a[:]@key@value']:
      with self.assertRaisesRegex(ValueError, 'Not a regular.*'):
        protopath._RegularElement(path)

  def test_regular_element(self):
    for path in ['abc', 'AbC', 'ABC']:
      el = protopath._RegularElement(path)
      self.assertEqual(el, protopath._RegularElement(path))
      self.assertNotEqual(el, protopath._RegularElement('xyz'))
      self.assertEqual(el.access('i'), 'i.abc()')
      self.assertEqual(el.protopath_element(), path)
      self.assertEqual(el.mutable_access('x'), '*x.mutable_abc()')
      self.assertEqual(el.set_value_access('x', 'y'), 'x.set_abc(y)')
      self.assertEqual(
          el.has_access('x'), 'AROLLA_PROTO3_COMPATIBLE_HAS(x, abc)')
      self.assertEqual(
          el.child_table_path(table.TablePath()),
          table.TablePath().Child(path))
      self.assertEqual(
          el.child_column_path(table.TablePath()),
          table.TablePath().Column(path))

  def test_struct_field(self):
    for field in ['abc', 'AbC', 'ABC']:
      path = f'&::{field}'
      el = protopath._StructFieldAccess(path)
      self.assertEqual(el, protopath._StructFieldAccess(path))
      self.assertNotEqual(el, protopath._StructFieldAccess('&::xyz'))
      self.assertEqual(el.access('i'), 'i.abc')
      self.assertEqual(el.mutable_access('i'), 'i.abc')
      self.assertEqual(el.set_value_access('i', 'x'), 'i.abc = x')
      self.assertEqual(el.protopath_element(), path)
      self.assertEqual(el.has_access('x'), 'true')
      self.assertEqual(
          el.child_table_path(table.TablePath()),
          table.TablePath().Child(field))
      self.assertEqual(
          el.child_column_path(table.TablePath()),
          table.TablePath().Column(field))

  def test_dereference(self):
    path = '*'
    el = protopath._DereferenceAccess(path)
    self.assertEqual(el, protopath._DereferenceAccess(path))
    self.assertEqual(el, protopath._DereferenceAccess('*'))
    self.assertEqual(el.access('i'), '(*(i))')
    self.assertEqual(el.mutable_access('i'), '(*(i))')
    self.assertEqual(el.set_value_access('i', 'x'), '(*(i)) = x')
    self.assertEqual(el.protopath_element(), path)
    self.assertEqual(el.has_access('x'), 'x != nullptr')
    self.assertEqual(el.child_table_path(table.TablePath()), table.TablePath())
    self.assertEqual(
        el.child_column_path(table.TablePath('a')),
        table.TablePath().Column('a'))

  def test_address_of(self):
    path = '&'
    el = protopath._AddressOfAccess(path)
    self.assertEqual(el, protopath._AddressOfAccess(path))
    self.assertEqual(el, protopath._AddressOfAccess('&'))
    self.assertEqual(el.access('i'), '(&(i))')
    with self.assertRaises(NotImplementedError):
      el.mutable_access('i')
    with self.assertRaises(NotImplementedError):
      el.set_value_access('i', 'x')
    self.assertEqual(el.protopath_element(), path)
    self.assertEqual(el.has_access('x'), 'true')
    self.assertEqual(el.child_table_path(table.TablePath()), table.TablePath())
    self.assertEqual(
        el.child_column_path(table.TablePath('a')),
        table.TablePath().Column('a'))

  def test_index_access_element_errors(self):
    for path in ['a[:]', 'a', 'a["abc"]']:
      with self.assertRaisesRegex(ValueError, 'Not a index.*'):
        protopath._IndexAccessElement(path)

  def test_index_access_element(self):
    for element in ['abc', 'AbC', 'ABC']:
      path = element + '[5]'
      el = protopath._IndexAccessElement(path)
      self.assertEqual(el, protopath._IndexAccessElement(path))
      self.assertNotEqual(el, protopath._RegularElement('xyz'))
      self.assertNotEqual(el, protopath._IndexAccessElement(element + '[3]'))
      self.assertEqual(el.access('i'), 'i.abc(5)')
      self.assertEqual(el.protopath_element(), path)
      self.assertEqual(el.mutable_access('x'), 'x.mutable_abc()->at(5)')
      self.assertFalse(el.can_continue_on_miss(is_mutable=False))
      self.assertFalse(el.can_continue_on_miss(is_mutable=True))
      self.assertEqual(el.set_value_access('x', 'y'), 'x.set_abc(5, y)')
      self.assertEqual(el.has_access('x'), 'x.abc().size() > 5')
      self.assertEqual(
          el.child_table_path(table.TablePath()),
          table.TablePath().Child(table.ArrayAccess(element, 5)))
      self.assertEqual(
          el.child_column_path(table.TablePath()),
          table.TablePath().Column(table.ArrayAccess(element, 5)))

  def test_map_access_element_errors(self):
    for path in ['a[:]', 'a', 'a[12]', 'a[]']:
      with self.assertRaisesRegex(ValueError, 'Not a map.*'):
        protopath._MapAccessElement(path)

  def test_map_access_element(self):
    for path, expected_map in [('abc["a"]', 'abc'), ("abc['a']", 'abc'),
                               ('Abc["a"]', 'Abc')]:
      el = protopath._MapAccessElement(path)
      self.assertEqual(el, protopath._MapAccessElement(path))
      self.assertNotEqual(el, protopath._RegularElement('xyz'))
      self.assertNotEqual(el, protopath._MapAccessElement('abc["b"]'))
      self.assertFalse(el.is_wildcard)
      self.assertEqual(el.access('i'), 'i.abc().at("a")')
      self.assertEqual(el.protopath_element(), path.replace("'", '"'))
      self.assertEqual(el.mutable_access('z'), '(*z.mutable_abc())["a"]')
      self.assertEqual(el.has_access('x'), 'x.abc().count("a") > 0')
      self.assertEqual(
          el.set_value_access('z', 'q'), '(*z.mutable_abc())["a"] = q')
      self.assertEqual(
          el.child_table_path(table.TablePath()),
          table.TablePath().Child(table.MapAccess(expected_map, 'a')))
      self.assertEqual(
          el.child_column_path(table.TablePath()),
          table.TablePath().Column(table.MapAccess(expected_map, 'a')))

  def test_wildcard_map_access_element(self):
    for path, expected_map in [('abc[*]', 'abc'), ('Abc[*]', 'Abc')]:
      el = protopath._MapAccessElement(path)
      self.assertTrue(el.is_wildcard)
      self.assertEqual(el.access('i'), 'i.abc().at(feature_key)')
      self.assertEqual(el.protopath_element(), path)
      self.assertEqual(el.has_access('x'), 'x.abc().count(feature_key) > 0')
      self.assertEqual(
          el.child_table_path(table.TablePath()),
          table.TablePath().Child(table.MapAccess(expected_map, '%s')))
      self.assertEqual(
          el.child_column_path(table.TablePath()),
          table.TablePath().Column(table.MapAccess(expected_map, '%s')))

  def test_extension_element_errors(self):
    for path in ['a', 'a[:]', 'a[0]', 'a@k', 'a[:]@key', 'a[:]@key@value']:
      with self.assertRaisesRegex(ValueError, 'Not an extension.*'):
        protopath._ExtensionSingleElement(path, is_final_access=False)

  def test_extension_element(self):
    path = 'Ext::package.x_int32'
    el = protopath._ExtensionSingleElement(path, is_final_access=False)
    final_el = protopath._ExtensionSingleElement(path, is_final_access=True)
    self.assertEqual(
        el, protopath._ExtensionSingleElement(path, is_final_access=False))
    self.assertNotEqual(el, protopath._RegularElement('xyz'))
    self.assertNotEqual(
        el,
        protopath._ExtensionSingleElement(
            'Ext::package.y_int32', is_final_access=False))
    self.assertNotEqual(el, final_el)
    self.assertEqual(el.access('i'), 'i.GetExtension(::package::x_int32)')
    self.assertEqual(el.protopath_element(), path)
    self.assertTrue(el.can_continue_on_miss(is_mutable=False))
    self.assertTrue(el.can_continue_on_miss(is_mutable=True))
    self.assertTrue(final_el.can_continue_on_miss(is_mutable=True))
    self.assertFalse(final_el.can_continue_on_miss(is_mutable=False))
    self.assertEqual(el.has_access('x'), 'x.HasExtension(::package::x_int32)')
    self.assertEqual(
        el.mutable_access('x'), '*x.MutableExtension(::package::x_int32)')
    self.assertEqual(
        el.set_value_access('x', 'y'), 'x.SetExtension(::package::x_int32, y)')
    access = table.ProtoExtensionAccess('package.x_int32')
    self.assertEqual(
        el.child_table_path(table.TablePath()),
        table.TablePath().Child(access))
    self.assertEqual(
        el.child_column_path(table.TablePath()),
        table.TablePath().Column(access))

  def test_size_element_errors(self):
    for path in [
        'a', 'a[:]', 'a[0]', 'a@k', 'a[:]@key', 'count(a)', 'count(a[0])'
    ]:
      with self.assertRaisesRegex(ValueError, 'Not a size access.*'):
        protopath._SizeElement(path)

  def test_size_element(self):
    path = 'count(a[:])'
    el = protopath._SizeElement(path)
    self.assertEqual(el, protopath._SizeElement(path))
    self.assertNotEqual(el, protopath._RegularElement('xyz'))
    self.assertNotEqual(el, protopath._SizeElement('count(b[:])'))
    self.assertEqual(el.access('i'), 'i.a().size()')
    self.assertEqual(el.protopath_element(), path)
    self.assertTrue(el.can_continue_on_miss(is_mutable=False))
    self.assertTrue(el.can_continue_on_miss(is_mutable=True))
    self.assertEqual(el.has_access('x'), 'true')
    self.assertEqual(
        el.child_column_path(table.TablePath()),
        table.TablePath().Size('a'))

  def test_single_value_element(self):
    self.assertEqual(
        protopath._single_value_element('abc').access('i'), 'i.abc()')
    self.assertEqual(
        protopath._single_value_element('&::abc').access('i'), 'i.abc')
    self.assertEqual(protopath._single_value_element('*').access('i'), '(*(i))')
    self.assertEqual(
        protopath._single_value_element('aBC').access('i'), 'i.abc()')
    self.assertEqual(
        protopath._single_value_element('a[3]').access('i'), 'i.a(3)')
    self.assertEqual(
        protopath._single_value_element('A[3]').access('i'), 'i.a(3)')
    self.assertEqual(
        protopath._single_value_element('a["k"]').access('i'), 'i.a().at("k")')
    self.assertEqual(
        protopath._single_value_element('A["k"]').access('i'), 'i.a().at("k")')
    for path in ['a[:]', 'a@size']:
      with self.assertRaisesRegex(ValueError, 'Not.*single'):
        protopath._single_value_element(path)

  def test_single_value_elements_list(self):
    empty_lst = protopath._SingleValueElementsList([])
    self.assertTrue(empty_lst.is_empty)
    self.assertEqual(empty_lst.access_for_type('i'), 'i')
    with self.assertRaisesRegex(ValueError, 'empty'):
      empty_lst.set_path_to_field('tmp', 'input', 'output', 'continue')

    el_lst = protopath._SingleValueElementsList(
        [protopath._RegularElement('abc')])
    self.assertFalse(el_lst.is_empty)
    self.assertEqual(el_lst.access_for_type('i'), 'i.abc()')
    self.assertEqualIgnoringSpaces(
        el_lst.set_path_to_field('tmp', 'input', 'result', 'continue'), """
        if (!(AROLLA_PROTO3_COMPATIBLE_HAS(input, abc))) { continue; }
        const auto& result = input.abc();
        """)
    self.assertEqualIgnoringSpaces(
        el_lst.set_path_to_field(
            'tmp', 'input', 'result', 'continue', is_mutable=True), """
        auto& result = *input.mutable_abc();
        """)

    el_lst = protopath._SingleValueElementsList([
        protopath._RegularElement('abc'),
        protopath._IndexAccessElement('xyz[2]')
    ])
    self.assertEqual(
        el_lst.child_table_path(table.TablePath()),
        table.TablePath().Child('abc').Child(table.ArrayAccess('xyz', 2)))
    self.assertEqual(
        el_lst.child_column_path(table.TablePath()),
        table.TablePath().Child('abc').Column(table.ArrayAccess('xyz', 2)))
    self.assertFalse(el_lst.is_empty)
    self.assertEqual(el_lst.access_for_type('i'), 'i.abc().xyz(2)')
    self.assertEqualIgnoringSpaces(
        el_lst.set_path_to_field('tmp', 'input', 'result', 'continue'), """
        if (!(AROLLA_PROTO3_COMPATIBLE_HAS(input, abc))) { continue; }
        const auto& tmp_0 = input.abc();
        if (!(tmp_0.xyz().size() > 2)) { continue; }
        const auto& result = tmp_0.xyz(2);
        """)
    self.assertEqualIgnoringSpaces(
        el_lst.set_path_to_field(
            'tmp', 'input', 'result', 'continue', is_mutable=True), """
        auto& tmp_0 = *input.mutable_abc();
        if (!(tmp_0.xyz().size() > 2)) { continue; }
        auto& result = tmp_0.mutable_xyz()->at(2);
        """)

  def test_single_range_slice_path(self):
    single_range = protopath._SingleMultiElemPath(
        protopath._SingleValueElementsList([]),
        protopath._RangeSliceElement('qwe[:]'))
    self.assertTrue(single_range.support_mutable)
    self.assertEqual(single_range.access_for_type('i'), 'i.qwe(0)')
    self.assertEqualIgnoringSpaces(
        single_range.open_loop('item', 'input'),
        'for (const auto& item : input.qwe()) {')
    self.assertEqualIgnoringSpaces(
        single_range.open_loop('item', 'input', is_mutable=True),
        'for (auto& item : *input.mutable_qwe()) {')
    self.assertEqualIgnoringSpaces(
        single_range.gen_loop_size('size', 'input'),
        'size_t size = [&]() { return input.qwe().size(); }();')

    single_range = protopath._SingleMultiElemPath(
        protopath._SingleValueElementsList([protopath._RegularElement('abc')]),
        protopath._RangeSliceElement('qwe[:]'))
    self.assertTrue(single_range.support_mutable)
    self.assertEqual(
        single_range.child_table_path(table.TablePath()),
        table.TablePath().Child('abc').Child('qwe'))
    self.assertEqual(single_range.access_for_type('i'), 'i.abc().qwe(0)')
    self.assertEqualIgnoringSpaces(
        single_range.gen_loop_size('size', 'input'), """
        size_t size = [&]() {
          if (!(AROLLA_PROTO3_COMPATIBLE_HAS(input, abc))) { return 0; }
          const auto& size_last = input.abc();
          return size_last.qwe().size();
        }();""")
    self.assertEqualIgnoringSpaces(
        single_range.open_loop('item', 'input'), """
        if (!(AROLLA_PROTO3_COMPATIBLE_HAS(input, abc))) { continue; }
        const auto& item_last = input.abc();
        for (const auto& item : item_last.qwe()) {
        """)
    self.assertEqualIgnoringSpaces(
        single_range.open_loop('item', 'input', is_mutable=True), """
        if (!(AROLLA_PROTO3_COMPATIBLE_HAS(input, abc))) { continue; }
        auto& item_last = *input.mutable_abc();
        for (auto& item : *item_last.mutable_qwe()) {
        """)

    single_range = protopath._SingleMultiElemPath(
        protopath._SingleValueElementsList([
            protopath._RegularElement('abc'),
            protopath._IndexAccessElement('xyz[2]')
        ]), protopath._RangeSliceElement('qwe[:]'))
    self.assertTrue(single_range.support_mutable)
    self.assertEqual(single_range.access_for_type('i'), 'i.abc().xyz(2).qwe(0)')
    self.assertEqualIgnoringSpaces(
        single_range.gen_loop_size('size', 'input'), """
        size_t size = [&]() {
          if (!(AROLLA_PROTO3_COMPATIBLE_HAS(input, abc))) { return 0; }
          const auto& size_tmp_0 = input.abc();
          if (!(size_tmp_0.xyz().size() > 2)) { return 0; }
          const auto& size_last = size_tmp_0.xyz(2);
          return size_last.qwe().size();
        }();""")
    self.assertEqualIgnoringSpaces(
        single_range.open_loop('item', 'input', is_mutable=True), """
        if (!(AROLLA_PROTO3_COMPATIBLE_HAS(input, abc))) { continue; }
        auto& item_tmp_0 = *input.mutable_abc();
        if (!(item_tmp_0.xyz().size() > 2)) { continue; }
        auto& item_last = item_tmp_0.mutable_xyz()->at(2);
        for (auto& item : *item_last.mutable_qwe()) {
        """)

    single_range = protopath._SingleMultiElemPath(
        protopath._SingleValueElementsList([]),
        protopath._MapKeysAccessElement('qwe[:]@key'))
    self.assertFalse(single_range.support_mutable)

  def test_protopath_parse_error(self):
    with self.assertRaisesRegex(ValueError, r'double-slash is not supported'):
      protopath.Protopath.parse('//foo/bar')
    with self.assertRaisesRegex(ValueError, r'empty element in the path'):
      protopath.Protopath.parse('/a/b//c')
    with self.assertRaisesRegex(ValueError, r'empty element in the path'):
      protopath.Protopath.parse('/a/b/c/')
    with self.assertRaisesRegex(ValueError, r'empty element in the path'):
      protopath.Protopath.parse('/')
    with self.assertRaisesRegex(ValueError, r'empty element in the path'):
      protopath.Protopath.parse('')

  def test_protopath_accessor_empty(self):
    with self.assertRaisesRegex(ValueError, 'Empty'):
      protopath.Protopath([],
                          protopath._SingleValueElementsList([]),
                          protopath='')

  def test_protopath_accessor_single_access(self):
    ppath = protopath.Protopath.parse('/abc', input_type='MyInput')
    for accessor in [
        ppath.accessor(),
        # array_generator is not used
        ppath.accessor(array_generator.create_generator('DenseArray'))
    ]:
      self.assertIsInstance(accessor, protopath.ProtopathAccessor)
      self.assertEqual(accessor.protopath, '/abc')
      self.assertSetEqual(accessor.required_includes, OPTIONAL_INCLUDES)
      self.assertEqual(accessor.default_name,
                       str(table.TablePath().Column('abc')))

      self.assertEqualIgnoringSpaces(
          accessor.lambda_str,
          """
[](const MyInput& input, ::arolla::proto::arolla_optional_value_t<
      decltype(input.abc())>* output) {
  output->present = false;
  if (!(AROLLA_PROTO3_COMPATIBLE_HAS(input, abc))) { return; }
  const auto& final_result = input.abc();
  output->present = true;
  output->value = final_result;
}""",
      )
    for accessor in [
        ppath.accessor(cpp_type='::arolla::Text'),
        # array_generator is not used
        ppath.accessor(
            array_generator.create_generator('DenseArray'),
            cpp_type='::arolla::Text',
        ),
    ]:
      self.assertSetEqual(accessor.required_includes, OPTIONAL_INCLUDES)
      self.assertEqual(accessor.default_name,
                       str(table.TablePath().Column('abc')))

      self.assertEqualIgnoringSpaces(
          accessor.lambda_str,
          """
[](const MyInput& input, ::arolla::proto::arolla_optional_value_t<
      ::arolla::Text>* output) {
  output->present = false;
  if (!(AROLLA_PROTO3_COMPATIBLE_HAS(input, abc))) { return; }
  const auto& final_result = input.abc();
  output->present = true;
  output->value = final_result;
}""",
      )

  def test_protopath_scalar_accessor_with_default_value_one_field(self):
    ppath = protopath.Protopath.parse('/abc', input_type='MyInput')
    accessor = ppath.scalar_accessor_with_default_value(
        cpp_type='int32_t', default_value_cpp='int32_t{0}')
    self.assertIsInstance(accessor, protopath.ProtopathAccessor)
    self.assertEqual(accessor.protopath, '/abc')
    self.assertSetEqual(accessor.required_includes, OPTIONAL_INCLUDES)
    self.assertEqual(
        accessor.default_name,
        str(table.TablePath().Column('abc')))

    self.assertEqualIgnoringSpaces(
        accessor.lambda_str, """
[](const MyInput& input, int32_t* output) {
  *output = int32_t{0};
  if (!(AROLLA_PROTO3_COMPATIBLE_HAS(input, abc))) { return; }
  const auto& final_result = input.abc();
  *output = final_result;
}""")

  def test_protopath_accessor_access_after_repeated(self):
    ppath = protopath.Protopath.parse('/qwe[:]/abc')
    accessor = ppath.accessor()
    self.assertIsInstance(accessor, protopath.ProtopathAccessor)
    self.assertEqual(accessor.protopath, '/qwe[:]/abc')
    self.assertEqual(accessor.default_name,
                     str(table.TablePath().Child('qwe').Column('abc')))
    self.assertSetEqual(accessor.required_includes, DENSE_ARRAY_INCLUDES)
    self.assertEqualIgnoringSpaces(
        accessor.lambda_str,
        """
[](const auto& input, ::arolla::RawBufferFactory* factory) {
  using proto_value_type = std::decay_t<decltype(input.qwe(0).abc())>;
  using value_type = ::arolla::proto::arolla_single_value_t<proto_value_type>;
  size_t total_size = [](const auto& input) {
    size_t total_size = 0;
    size_t final_size = [&]() { return input.qwe().size(); }();
    total_size += final_size;
    return total_size;
  }(input);
  using result_type = ::arolla::DenseArray<value_type>;
  typename ::arolla::Buffer<value_type>::Builder bldr(total_size, factory);
  auto inserter = bldr.GetInserter();
  int64_t id = 0;
  ::arolla::bitmap::AlmostFullBuilder bitmap_bldr(total_size, factory);
  for (int _fake_var = 0; _fake_var != 1; ++_fake_var) {
    for (const auto& value_0 : input.qwe()) {
      if (!(AROLLA_PROTO3_COMPATIBLE_HAS(value_0, abc))) {
        bitmap_bldr.AddMissed(id++);
        inserter.SkipN(1);
        continue;
      }
      const auto& final_item = value_0.abc();
      id++;
      inserter.Add(final_item);
    }
  }
  return result_type{std::move(bldr).Build(), std::move(bitmap_bldr).Build()};
}""",
    )

  def test_protopath_accessor_general_case_repeated_at_the_end(self):
    ppath_str = '/abc/qwe[:]/xyz[2]/tre/sdf[:]'
    ppath = protopath.Protopath.parse(ppath_str)
    accessor = ppath.accessor()
    self.assertIsInstance(accessor, protopath.ProtopathAccessor)
    self.assertEqual(accessor.protopath, ppath_str)
    self.assertSetEqual(accessor.required_includes, DENSE_ARRAY_INCLUDES)
    table_path = table.TablePath().Child('abc').Child('qwe').Child(
        table.ArrayAccess('xyz', 2)).Child('tre')
    self.assertEqual(accessor.default_name, str(table_path.Child('sdf')))
    self.assertEqualIgnoringSpaces(
        accessor.lambda_str,
        """
[](const auto& input, ::arolla::RawBufferFactory* factory) {
  using proto_value_type = std::decay_t<decltype(input.abc().qwe(0).xyz(2).tre().sdf(0))>;
  using value_type = ::arolla::proto::arolla_single_value_t<proto_value_type>;
  size_t total_size = [](const auto& input) {
    size_t total_size = 0;
    for (int _fake_var = 0; _fake_var != 1; ++_fake_var) {
      if (!(AROLLA_PROTO3_COMPATIBLE_HAS(input, abc))) {
        continue;
      }
      const auto& value_0_last = input.abc();
      for (const auto& value_0 : value_0_last.qwe()) {
        size_t final_size = [&]() {
          if (!(value_0.xyz().size() > 2)) {
            return 0;
          }
          const auto& final_size_tmp_0 = value_0.xyz(2);
          if (!(AROLLA_PROTO3_COMPATIBLE_HAS(final_size_tmp_0, tre))) {
            return 0;
          }
          const auto& final_size_last = final_size_tmp_0.tre();
          return final_size_last.sdf().size();
        }();
        total_size += final_size;
      }
    }
    return total_size;
  }(input);
  using result_type = ::arolla::DenseArray<value_type>;
  typename ::arolla::Buffer<value_type>::Builder bldr(total_size, factory);
  auto inserter = bldr.GetInserter();
  int64_t id = 0;
  ::arolla::bitmap::AlmostFullBuilder bitmap_bldr(total_size, factory);
  for (int _fake_var = 0; _fake_var != 1; ++_fake_var) {
    if (!(AROLLA_PROTO3_COMPATIBLE_HAS(input, abc))) {
      continue;
    }
    const auto& value_0_last = input.abc();
    for (const auto& value_0 : value_0_last.qwe()) {
      if (!(value_0.xyz().size() > 2)) {
        continue;
      }
      const auto& value_1_tmp_0 = value_0.xyz(2);
      if (!(AROLLA_PROTO3_COMPATIBLE_HAS(value_1_tmp_0, tre))) {
        continue;
      }
      const auto& value_1_last = value_1_tmp_0.tre();
      for (const auto& value_1 : value_1_last.sdf()) {
        id++;
        inserter.Add(value_1);
      }
    }
  }
  return result_type{std::move(bldr).Build(), std::move(bitmap_bldr).Build()};
}""",
    )

  def test_protopath_accessor_general_case_single_at_the_end(self):
    ppath_str = '/abc/qwe[:]/xyz/tre/sdf[:]/abc/def'
    ppath = protopath.Protopath.parse(ppath_str)
    accessor = ppath.accessor()
    self.assertIsInstance(accessor, protopath.ProtopathAccessor)
    self.assertEqual(accessor.protopath, ppath_str)
    self.assertSetEqual(accessor.required_includes, DENSE_ARRAY_INCLUDES)
    self.assertEqualIgnoringSpaces(
        accessor.lambda_str,
        """
[](const auto& input, ::arolla::RawBufferFactory* factory) {
  using proto_value_type = std::decay_t<
      decltype(input.abc().qwe(0).xyz().tre().sdf(0).abc().def())>;
  using value_type = ::arolla::proto::arolla_single_value_t<proto_value_type>;
  size_t total_size = [](const auto& input) {
    size_t total_size = 0;
    for (int _fake_var = 0; _fake_var != 1; ++_fake_var) {
      if (!(AROLLA_PROTO3_COMPATIBLE_HAS(input, abc))) {
        continue;
      }
      const auto& value_0_last = input.abc();
      for (const auto& value_0 : value_0_last.qwe()) {
        size_t final_size = [&]() {
          if (!(AROLLA_PROTO3_COMPATIBLE_HAS(value_0, xyz))) {
            return 0;
          }
          const auto& final_size_tmp_0 = value_0.xyz();
          if (!(AROLLA_PROTO3_COMPATIBLE_HAS(final_size_tmp_0, tre))) {
            return 0;
          }
          const auto& final_size_last = final_size_tmp_0.tre();
          return final_size_last.sdf().size();
        }();
        total_size += final_size;
      }
    }
    return total_size;
  }(input);
  using result_type = ::arolla::DenseArray<value_type>;
  typename ::arolla::Buffer<value_type>::Builder bldr(total_size, factory);
  auto inserter = bldr.GetInserter();
  int64_t id = 0;
  ::arolla::bitmap::AlmostFullBuilder bitmap_bldr(total_size, factory);
  for (int _fake_var = 0; _fake_var != 1; ++_fake_var) {
    if (!(AROLLA_PROTO3_COMPATIBLE_HAS(input, abc))) {
      continue;
    }
    const auto& value_0_last = input.abc();
    for (const auto& value_0 : value_0_last.qwe()) {
      if (!(AROLLA_PROTO3_COMPATIBLE_HAS(value_0, xyz))) {
        continue;
      }
      const auto& value_1_tmp_0 = value_0.xyz();
      if (!(AROLLA_PROTO3_COMPATIBLE_HAS(value_1_tmp_0, tre))) {
        continue;
      }
      const auto& value_1_last = value_1_tmp_0.tre();
      for (const auto& value_1 : value_1_last.sdf()) {
        if (!(AROLLA_PROTO3_COMPATIBLE_HAS(value_1, abc))) {
          bitmap_bldr.AddMissed(id++);
          inserter.SkipN(1);
          continue;
        }
        const auto& value_1_tmp_0 = value_1.abc();
        if (!(AROLLA_PROTO3_COMPATIBLE_HAS(value_1_tmp_0, def))) {
          bitmap_bldr.AddMissed(id++);
          inserter.SkipN(1);
          continue;
        }
        const auto& final_item = value_1_tmp_0.def();
        id++;
        inserter.Add(final_item);
      }
    }
  }
  return result_type{std::move(bldr).Build(), std::move(bitmap_bldr).Build()};
}""",
    )

  def test_protopath_errors(self):
    for ppath in ['/abc[*]/qwe', '/abc[:]/qwe[*]', '/abc[:]/qwe[*]/asd[:]/tyu']:
      with self.assertRaisesRegex(ValueError, 'Wildcards.*'):
        protopath.Protopath.parse(ppath)

  def test_single_range_slice_path_dense_array_single_at_the_end(self):
    ppath_str = '/qwe[:]/abc'
    ppath = protopath.Protopath.parse(ppath_str)
    accessor = ppath.accessor(array_generator.create_generator('DenseArray'))
    self.assertIsInstance(accessor, protopath.ProtopathAccessor)
    self.assertEqual(accessor.protopath, ppath_str)
    self.assertEqual(accessor.default_name,
                     str(table.TablePath().Child('qwe').Column('abc')))
    self.assertSetEqual(accessor.required_includes, DENSE_ARRAY_INCLUDES)
    self.assertEqualIgnoringSpaces(
        accessor.lambda_str,
        """
[](const auto& input, ::arolla::RawBufferFactory* factory) {
  using proto_value_type = std::decay_t<decltype(input.qwe(0).abc())>;
  using value_type = ::arolla::proto::arolla_single_value_t<
      proto_value_type>;
  size_t total_size = [](const auto& input) {
    size_t total_size = 0;
    size_t final_size = [&]() { return input.qwe().size(); }();
    total_size += final_size;
    return total_size;
  }(input);
  using result_type = ::arolla::DenseArray<value_type>;
  typename ::arolla::Buffer<value_type>::Builder bldr(total_size, factory);
  auto inserter = bldr.GetInserter();
  int64_t id = 0;
  ::arolla::bitmap::AlmostFullBuilder bitmap_bldr(total_size, factory);
  for (int _fake_var = 0; _fake_var != 1; ++_fake_var) {
  for (const auto& value_0 : input.qwe()) {
    if (!(AROLLA_PROTO3_COMPATIBLE_HAS(value_0, abc))) {
      bitmap_bldr.AddMissed(id++);
      inserter.SkipN(1);
      continue;
    }
    const auto& final_item = value_0.abc();
    id++;
    inserter.Add(final_item);
  }}
  return result_type{std::move(bldr).Build(), std::move(bitmap_bldr).Build()};
}""",
    )

  def test_single_range_slice_path_dense_array_fixed_cpptype(self):
    ppath_str = '/qwe[:]/abc'
    ppath = protopath.Protopath.parse(ppath_str)
    accessor = ppath.accessor(
        array_generator.create_generator('DenseArray'),
        cpp_type='::arolla::Text',
    )
    self.assertIsInstance(accessor, protopath.ProtopathAccessor)
    self.assertEqual(accessor.protopath, ppath_str)
    self.assertEqual(accessor.default_name,
                     str(table.TablePath().Child('qwe').Column('abc')))
    self.assertSetEqual(accessor.required_includes, DENSE_ARRAY_INCLUDES)
    self.assertEqualIgnoringSpaces(
        accessor.lambda_str,
        """
[](const auto& input, ::arolla::RawBufferFactory* factory) {
  using value_type = ::arolla::Text;
  size_t total_size = [](const auto& input) {
    size_t total_size = 0;
    size_t final_size = [&]() { return input.qwe().size(); }();
    total_size += final_size;
    return total_size;
  }(input);
  using result_type = ::arolla::DenseArray<value_type>;
  typename ::arolla::Buffer<value_type>::Builder bldr(total_size, factory);
  auto inserter = bldr.GetInserter();
  int64_t id = 0;
  ::arolla::bitmap::AlmostFullBuilder bitmap_bldr(total_size, factory);
  for (int _fake_var = 0; _fake_var != 1; ++_fake_var) {
  for (const auto& value_0 : input.qwe()) {
    if (!(AROLLA_PROTO3_COMPATIBLE_HAS(value_0, abc))) {
      bitmap_bldr.AddMissed(id++);
      inserter.SkipN(1); continue;
    }
    const auto& final_item = value_0.abc();
    id++;
    inserter.Add(final_item);
  }}
  return result_type{std::move(bldr).Build(), std::move(bitmap_bldr).Build()};
}""",
    )

  def test_protopath_size_accessor_errors(self):
    for ppath in ['/esd/count(abc[:])/sde', '/count(abc[:])/ret']:
      with self.assertRaisesRegex(ValueError,
                                  'count is only allowed at the end'):
        protopath.Protopath.parse(ppath)

  def test_protopath_size_accessor(self):
    for accessor in [
        protopath.Protopath.parse('/count(abc[:])').accessor(),
    ]:
      self.assertSetEqual(accessor.required_includes, DENSE_ARRAY_INCLUDES)
      self.assertEqualIgnoringSpaces(
          accessor.lambda_str,
          """
[](const auto& input) {
  const auto& final_result = input.abc().size();
  return ::arolla::DenseArrayShape{static_cast<int64_t>(final_result)};
}""",
      )
    for accessor in [
        protopath.Protopath.parse('/qwe/count(abc[:])').accessor(),
    ]:
      self.assertSetEqual(accessor.required_includes, DENSE_ARRAY_INCLUDES)
      self.assertEqualIgnoringSpaces(
          accessor.lambda_str,
          """
[](const auto& input) {
  if (!(AROLLA_PROTO3_COMPATIBLE_HAS(input, qwe))) {
    return ::arolla::DenseArrayShape{0};
  }
  const auto& val_0 = input.qwe();
  const auto& final_result = val_0.abc().size();
  return ::arolla::DenseArrayShape{static_cast<int64_t>(final_result)};
}""",
      )

    for accessor in [
        protopath.Protopath.parse('/ijk[:]/qwe/count(abc[:])').accessor(),
    ]:
      self.assertSetEqual(accessor.required_includes, DENSE_ARRAY_INCLUDES)
      column_path = table.TablePath().Child('ijk').Child('qwe').Size('abc')
      self.assertEqual(accessor.default_name, str(column_path))
      self.assertEqualIgnoringSpaces(
          accessor.lambda_str,
          """
[](const auto& input, ::arolla::RawBufferFactory* factory) {
  using value_type = ::arolla::proto::arolla_size_t;
  size_t total_size = [](const auto& input) {
    size_t total_size = 0;
    size_t final_size = [&]() { return input.ijk().size(); }();
    total_size += final_size;
    return total_size;
  }(input);
  using result_type = ::arolla::DenseArray<value_type>;
  typename ::arolla::Buffer<value_type>::Builder bldr(total_size, factory);
  auto inserter = bldr.GetInserter();
  int64_t id = 0;
  ::arolla::bitmap::AlmostFullBuilder bitmap_bldr(total_size, factory);
  for (int _fake_var = 0; _fake_var != 1; ++_fake_var) {
    for (const auto& value_0 : input.ijk()) {
      if (!(AROLLA_PROTO3_COMPATIBLE_HAS(value_0, qwe))) {
        id++;
        inserter.Add(0);
        continue;
      }
      const auto& value_0_tmp_0 = value_0.qwe();
      const auto& final_item = value_0_tmp_0.abc().size();
      id++;
      inserter.Add(final_item);
    }
  }
  return result_type{std::move(bldr).Build(), std::move(bitmap_bldr).Build()};
}""",
      )

    for accessor in [
        protopath.Protopath.parse('/tre/ijk[:]/qwe/count(abc[:])').accessor(),
    ]:
      self.assertSetEqual(accessor.required_includes, DENSE_ARRAY_INCLUDES)
      self.assertEqualIgnoringSpaces(
          accessor.lambda_str,
          """
[](const auto& input, ::arolla::RawBufferFactory* factory) {
  using value_type = ::arolla::proto::arolla_size_t;
  size_t total_size = [](const auto& input) {
    size_t total_size = 0;
    size_t final_size = [&]() {
      if (!(AROLLA_PROTO3_COMPATIBLE_HAS(input, tre))) {
        return 0;
      }
      const auto& final_size_last = input.tre();
      return final_size_last.ijk().size();
    }();
    total_size += final_size;
    return total_size;
  }(input);
  using result_type = ::arolla::DenseArray<value_type>;
  typename ::arolla::Buffer<value_type>::Builder bldr(total_size, factory);
  auto inserter = bldr.GetInserter();
  int64_t id = 0;
  ::arolla::bitmap::AlmostFullBuilder bitmap_bldr(total_size, factory);
  for (int _fake_var = 0; _fake_var != 1; ++_fake_var) {
    if (!(AROLLA_PROTO3_COMPATIBLE_HAS(input, tre))) {
      continue;
    }
    const auto& value_0_last = input.tre();
    for (const auto& value_0 : value_0_last.ijk()) {
      if (!(AROLLA_PROTO3_COMPATIBLE_HAS(value_0, qwe))) {
        id++;
        inserter.Add(0);
        continue;
      }
      const auto& value_0_tmp_0 = value_0.qwe();
      const auto& final_item = value_0_tmp_0.abc().size();
      id++;
      inserter.Add(final_item);
    }
  }
  return result_type{std::move(bldr).Build(), std::move(bitmap_bldr).Build()};
}""",
      )

  def test_collect_extensions_per_containing_type(self):
    type2extensions = protopath.collect_extensions_per_containing_type(
        [test_extension_pb2.DESCRIPTOR])
    self.assertCountEqual(type2extensions.keys(),
                          ['testing_namespace.Inner', 'testing_namespace.Root'])
    self.assertCountEqual(
        [x.full_name for x in type2extensions['testing_namespace.Root']], [
            'testing_extension_namespace.extension_x_int32',
            'testing_extension_namespace.repeated_extension_x_int32',
            'testing_extension_namespace.root_reference'
        ])
    self.assertCountEqual(
        [x.full_name for x in type2extensions['testing_namespace.Inner']],
        ['testing_extension_namespace.InnerExtension.inner_ext'])

  @parameterized.parameters(
      itertools.product(
          ['::arolla::Text', '::arolla::Bytes'],
          [False, True],
          [False, True],
          [False, True],
          [False, True],
      )
  )
  def test_accessors_from_descriptor(self, text_cpp_type: str,
                                     use_extensions: bool, use_repeated: bool,
                                     skip_repeated_via_fn: bool, mutable: bool):
    test_descriptor = test_pb2.Root().DESCRIPTOR
    array_gen = array_generator.create_generator(
        'DenseArray') if use_repeated else None
    type2extensions = []
    if use_extensions:
      type2extensions = protopath.collect_extensions_per_containing_type(
          [test_extension_pb2.DESCRIPTOR])

    skip_field_fn = (
        protopath.is_repeated_field
        if skip_repeated_via_fn else protopath.no_skip_fn)
    names_and_accessors = protopath.accessors_from_descriptor(
        test_descriptor,
        array_gen,
        type2extensions=type2extensions,
        skip_field_fn=skip_field_fn,
        text_cpp_type=text_cpp_type,
        mutable=mutable)
    names, accessors = zip(*names_and_accessors)

    expected_default_type_paths = [
        # go/keep-sorted start
        'BrOkEn_CaSe',
        'inner/a',
        'inner/inner2/z',
        'inner/raw_bytes',
        'private',
        'proto3/non_optional_i32',
        'raw_bytes',
        'x',
        'x0',
        'x1',
        'x2',
        'x3',
        'x4',
        'x5',
        'x6',
        'x7',
        'x8',
        'x9',
        'x_double',
        'x_enum',
        'x_fixed64',
        'x_float',
        'x_int64',
        'x_uint32',
        'x_uint64',
        # go/keep-sorted end
    ]
    expected_text_type_paths = [
        # go/keep-sorted start
        'inner/str',
        'str',
        # go/keep-sorted end
    ]
    expected_repeated_protopaths = [
        # go/keep-sorted start
        'inner/inners2[:]/z',
        'inners[:]/a',
        'inners[:]/inner2/z',
        'inners[:]/inners2[:]/z',
        'inners[:]/raw_bytes',
        # go/keep-sorted end
    ]
    if not mutable:
      expected_repeated_protopaths += [
          # go/keep-sorted start
          'inner/as[:]',
          'inner/inner2/zs[:]',
          'inner/inners2[:]/zs[:]',
          'inners[:]/as[:]',
          'inners[:]/inner2/zs[:]',
          'inners[:]/inners2[:]/zs[:]',
          'repeated_bools[:]',
          'repeated_doubles[:]',
          'repeated_enums[:]',
          'repeated_floats[:]',
          'repeated_int32s[:]',
          'repeated_int64s[:]',
          'repeated_raw_bytes[:]',
          'repeated_uint32s[:]',
          'repeated_uint64s[:]',
          'ys[:]'
          # go/keep-sorted end
      ]
    expected_size_protopaths = []
    expected_non_primitive_size_protopaths = [
        'count(inners[:])', 'inners[:]/count(inners2[:])',
        'inner/count(inners2[:])'
    ]
    expected_size_protopaths += expected_non_primitive_size_protopaths
    expected_primitive_size_protopaths = [
        'inner/count(as[:])', 'inners[:]/count(as[:])',
        'inner/inner2/count(zs[:])', 'inners[:]/inner2/count(zs[:])',
        'inners[:]/inners2[:]/count(zs[:])', 'inner/inners2[:]/count(zs[:])',
        'count(repeated_bools[:])', 'count(repeated_doubles[:])',
        'count(repeated_str[:])', 'count(repeated_floats[:])',
        'count(repeated_int32s[:])', 'count(repeated_int64s[:])',
        'count(repeated_raw_bytes[:])', 'count(repeated_uint32s[:])',
        'count(repeated_uint64s[:])', 'count(ys[:])', 'count(repeated_enums[:])'
    ]
    if not mutable:
      expected_size_protopaths += expected_primitive_size_protopaths
    expected_repeated_text_protopaths = [
        'inners[:]/str',
    ]
    if not mutable:
      expected_repeated_text_protopaths += ['repeated_str[:]']
    expected_extension_protopaths = [
        'Ext::testing_extension_namespace.extension_x_int32',
        'inner/Ext::testing_extension_namespace.InnerExtension.inner_ext/' +
        'inner_extension_x_int32',
    ]
    expected_repeated_extension_protopaths = [
        'inners[:]/Ext::testing_extension_namespace.InnerExtension.inner_ext/' +
        'inner_extension_x_int32',
    ]
    if not mutable:
      expected_repeated_extension_protopaths += [
          'inner/Ext::testing_extension_namespace.InnerExtension.inner_ext/' +
          'repeated_inner_extension_x_int32[:]',
          'inners[:]/Ext::testing_extension_namespace.InnerExtension.' +
          'inner_ext/repeated_inner_extension_x_int32[:]'
      ]
    expected_extension_size_protopaths = [
        'inner/Ext::testing_extension_namespace.InnerExtension.inner_ext/' +
        'count(repeated_inner_extension_x_int32[:])',
        'inners[:]/Ext::testing_extension_namespace.InnerExtension.inner_ext/' +
        'count(repeated_inner_extension_x_int32[:])'
    ]
    if use_extensions:
      expected_default_type_paths += expected_extension_protopaths
      expected_repeated_protopaths += expected_repeated_extension_protopaths
      if not mutable:
        expected_size_protopaths += expected_extension_size_protopaths

    expected_names = expected_default_type_paths + expected_text_type_paths
    if use_repeated and not skip_repeated_via_fn:
      expected_names += [
          path.replace('[:]', '') for path in expected_repeated_protopaths +
          expected_repeated_text_protopaths
      ]
      expected_names += [
          # a[:]/count(b[:]) -> a/b/@size
          path.replace('[:]', '').replace('count(', '')[:-1] + '/@size'
          for path in expected_size_protopaths
      ]
    expected_names = ['/' + s for s in expected_names]

    def get_accessor(pp, array_gen, cpp_type=None):
      return (pp.accessor(array_gen, cpp_type=cpp_type) if not mutable else
              pp.mutable_accessor(array_gen=array_gen, cpp_type=cpp_type))

    expected_accessors = [
        get_accessor(protopath.Protopath.parse(s), array_gen)
        for s in expected_default_type_paths
    ]
    expected_accessors += [
        get_accessor(protopath.Protopath.parse(s), array_gen, text_cpp_type)
        for s in expected_text_type_paths
    ]
    if use_repeated and not skip_repeated_via_fn:
      expected_accessors += [
          get_accessor(protopath.Protopath.parse(s), array_gen)
          for s in expected_repeated_protopaths
      ]
      expected_accessors += [
          get_accessor(protopath.Protopath.parse(s), array_gen, text_cpp_type)
          for s in expected_repeated_text_protopaths
      ]
      expected_accessors += [
          get_accessor(protopath.Protopath.parse(s), array_gen)
          for s in expected_size_protopaths
      ]
    expected_accessors = sorted(
        expected_accessors, key=lambda a: a.default_name)

    self.assertCountEqual(names, expected_names)
    self.assertCountEqual([a.default_name for a in expected_accessors],
                          [a.default_name for a in accessors])
    for expected, actual in zip(expected_accessors, accessors):
      self.assertMultiLineEqual(actual.lambda_str, expected.lambda_str)

  def test_accessors_from_descriptor_with_prefix(self):
    test_descriptor = test_proto3_pb2.Proto3().DESCRIPTOR
    names_and_accessors = protopath.accessors_from_descriptor(
        test_descriptor, array_gen=None, protopath_prefix='proto3')
    names, accessors = zip(*names_and_accessors)
    self.assertCountEqual(names, ['/proto3/non_optional_i32'])
    expected_accessor = protopath.Protopath.parse(
        'proto3/non_optional_i32').accessor()
    for expected, actual in zip([expected_accessor], accessors):
      self.assertMultiLineEqual(actual.lambda_str, expected.lambda_str)

  def test_protopath_wildcard_errors(self):
    for ppath in ['abc/qwe', 'abc[*]/qwe[*]', 'abc[:]/qwe[*]/asd[:]/tyu[*]']:
      with self.assertRaisesRegex(ValueError, '.*one wildcard.*'):
        protopath.Protopath.parse_wildcard(ppath)

  def test_protopath_wildcard_accessor(self):
    for ppath in ['abc[*]']:
      accessor = protopath.Protopath.parse_wildcard(
          ppath, input_type='MyInput').accessor()
      self.assertSetEqual(accessor.required_includes, OPTIONAL_INCLUDES)
      self.assertEqual(accessor.default_name,
                       table.TablePath().Child(table.MapAccess('abc', '%s')))
      self.assertEqualIgnoringSpaces(
          accessor.lambda_str,
          """
[](const MyInput& input, absl::string_view feature_key,
   ::arolla::proto::arolla_optional_value_t<
       decltype(input.abc().at(feature_key))>* output) {
  output->present = false;
  if (!(input.abc().count(feature_key) > 0)) { return; }
  const auto& final_result = input.abc().at(feature_key);
  output->present = true;
  output->value = final_result;
}""",
      )
    for ppath in ['qwe[:]/abc[*]']:
      accessor = protopath.Protopath.parse_wildcard(
          ppath, input_type='MyInput').accessor(
              array_generator.create_generator('DenseArray'))
      self.assertSetEqual(accessor.required_includes, DENSE_ARRAY_INCLUDES)
      self.assertEqual(
          accessor.default_name,
          table.TablePath('qwe').Child(table.MapAccess('abc', '%s')))
      self.assertEqualIgnoringSpaces(
          accessor.lambda_str,
          """
[](const MyInput& input,
   absl::string_view feature_key,
   ::arolla::RawBufferFactory* factory) {
  using proto_value_type = std::decay_t<decltype(input.qwe(0).abc().at(feature_key))>;
  using value_type = ::arolla::proto::arolla_single_value_t<proto_value_type>;
  size_t total_size = [&](const auto& input) {
    size_t total_size = 0;
    size_t final_size = [&]() { return input.qwe().size(); }();
    total_size += final_size;
    return total_size;
  }(input);
  using result_type = ::arolla::DenseArray<value_type>;
  typename ::arolla::Buffer<value_type>::Builder bldr(total_size, factory);
  auto inserter = bldr.GetInserter();
  int64_t id = 0;
  ::arolla::bitmap::AlmostFullBuilder bitmap_bldr(total_size, factory);
  for (int _fake_var = 0; _fake_var != 1; ++_fake_var) {
    for (const auto& value_0 : input.qwe()) {
      if (!(value_0.abc().count(feature_key) > 0)) {
        bitmap_bldr.AddMissed(id++);
        inserter.SkipN(1);
        continue;
      }
      const auto& final_item = value_0.abc().at(feature_key);
      id++;
      inserter.Add(final_item);
    }
  }
  return result_type{std::move(bldr).Build(), std::move(bitmap_bldr).Build()};
}""",
      )

  def test_protopath_mutable_accessor_errors(self):
    array_gen = array_generator.create_generator('DenseArray')
    for ppath in ['abc/qwe[:]', 'abc[:]', 'abc[:]/@key', 'abc[:]/@value']:
      with self.assertRaisesRegex(ValueError,
                                  '.*supported for repeated primitive.*'):
        protopath.Protopath.parse(ppath).mutable_accessor(array_gen=array_gen)
    for ppath in ['abc/qwe[:]', 'abc[:]', 'abc[:]/@key', 'abc[:]/@value']:
      with self.assertRaisesRegex(ValueError,
                                  '.*supported for repeated primitive.*'):
        protopath.Protopath.parse(ppath).mutable_accessor(array_gen=array_gen)

  def test_protopath_optional_mutable_accessor(self):
    for path in ['abc', 'AbC', 'ABC']:
      accessor = protopath.Protopath.parse(path).mutable_accessor()
      self.assertSetEqual(accessor.required_includes, OPTIONAL_INCLUDES)
      self.assertEqual(accessor.default_name, table.TablePath().Child(path))
      self.assertEqualIgnoringSpaces(
          accessor.lambda_str,
          """
[](auto output_type_meta_fn) constexpr {
  using output_type = typename decltype(output_type_meta_fn)::type;
  using proto_value_type = std::decay_t<decltype(std::declval<output_type>().abc())>;
  using value_type = ::arolla::proto::arolla_single_value_t<proto_value_type>;
  using input_type = ::arolla::proto::arolla_optional_value_t<value_type>;
  return [](const input_type& input, output_type* output_ptr) {
    if (!input.present) {
      return;
    }
    auto& final_mutable_field = *output_ptr;
    final_mutable_field.set_abc(proto_value_type(input.value));
  };
}""",
      )
    for path in ['abc', 'AbC', 'ABC']:
      accessor = protopath.Protopath.parse(path).mutable_accessor(
          cpp_type='::arolla::Text'
      )
      self.assertSetEqual(accessor.required_includes, OPTIONAL_INCLUDES)
      self.assertEqual(accessor.default_name, table.TablePath().Child(path))
      self.assertEqualIgnoringSpaces(
          accessor.lambda_str,
          """
[](auto output_type_meta_fn) constexpr {
  using output_type = typename decltype(output_type_meta_fn)::type;
  using proto_value_type = std::decay_t<decltype(std::declval<output_type>().abc())>;
  using value_type = ::arolla::Text;
  using input_type = ::arolla::proto::arolla_optional_value_t<value_type>;
  return [](const input_type& input, output_type* output_ptr) {
    if (!input.present) {
      return;
    }
    auto& final_mutable_field = *output_ptr;
    final_mutable_field.set_abc(proto_value_type(input.value));
  };
}""",
      )
    for path0, path1 in [('abc', 'xyz'), ('AbC', 'XyZ'), ('ABC', 'Xyz')]:
      path = '%s/%s' % (path0, path1)
      accessor = protopath.Protopath.parse(path).mutable_accessor()
      self.assertSetEqual(accessor.required_includes, OPTIONAL_INCLUDES)
      self.assertEqual(accessor.default_name,
                       table.TablePath().Child(path0).Child(path1))
      self.assertEqualIgnoringSpaces(
          accessor.lambda_str,
          """
[](auto output_type_meta_fn) constexpr {
  using output_type = typename decltype(output_type_meta_fn)::type;
  using proto_value_type = std::decay_t<decltype(std::declval<output_type>().abc().xyz())>;
  using value_type = ::arolla::proto::arolla_single_value_t<proto_value_type>;
  using input_type = ::arolla::proto::arolla_optional_value_t<value_type>;
  return [](const input_type& input, output_type* output_ptr) {
    if (!input.present) {
      return;
    }
    auto& final_mutable_field = *(*output_ptr).mutable_abc();
    final_mutable_field.set_xyz(proto_value_type(input.value));
  };
}""",
      )
    for path0, path1 in [('abc', 'xyz'), ('AbC', 'XyZ'), ('ABC', 'Xyz')]:
      path = '%s/%s[2]' % (path0, path1)
      accessor = protopath.Protopath.parse(path).mutable_accessor()
      self.assertSetEqual(accessor.required_includes, OPTIONAL_INCLUDES)
      self.assertEqual(
          accessor.default_name,
          table.TablePath().Child(path0).Child(table.ArrayAccess(path1, 2)))
      self.assertEqualIgnoringSpaces(
          accessor.lambda_str,
          """
[](auto output_type_meta_fn) constexpr {
  using output_type = typename decltype(output_type_meta_fn)::type;
  using proto_value_type = std::decay_t<decltype(std::declval<output_type>().abc().xyz(2))>;
  using value_type = ::arolla::proto::arolla_single_value_t<proto_value_type>;
  using input_type = ::arolla::proto::arolla_optional_value_t<value_type>;
  return [](const input_type& input, output_type* output_ptr) {
    if (!input.present) {
      return;
    }
    auto& final_mutable_field = *(*output_ptr).mutable_abc();
    if (!(final_mutable_field.xyz().size() > 2)) { return; }
    final_mutable_field.set_xyz(2, proto_value_type(input.value));
  };
}""",
      )
    for path0, path1 in [('abc', 'xyz'), ('AbC', 'XyZ'), ('ABC', 'Xyz')]:
      path = '%s[2]/%s' % (path0, path1)
      accessor = protopath.Protopath.parse(path).mutable_accessor()
      self.assertSetEqual(accessor.required_includes, OPTIONAL_INCLUDES)
      self.assertEqual(
          accessor.default_name,
          table.TablePath().Child(table.ArrayAccess(path0, 2)).Child(path1))
      self.assertEqualIgnoringSpaces(
          accessor.lambda_str,
          """
[](auto output_type_meta_fn) constexpr {
  using output_type = typename decltype(output_type_meta_fn)::type;
  using proto_value_type = std::decay_t<decltype(std::declval<output_type>().abc(2).xyz())>;
  using value_type = ::arolla::proto::arolla_single_value_t<proto_value_type>;
  using input_type = ::arolla::proto::arolla_optional_value_t<value_type>;
  return [](const input_type& input, output_type* output_ptr) {
    if (!input.present) {
      return;
    }
    if (!((*output_ptr).abc().size() > 2)) { return; }
    auto& final_mutable_field = (*output_ptr).mutable_abc()->at(2);
    final_mutable_field.set_xyz(proto_value_type(input.value));
  };
}""",
      )

  @parameterized.parameters(['abc', 'AbC', 'ABC'])
  def test_protopath_size_mutable_accessor(self, name):
    path = f'count({name}[:])'
    accessor = protopath.Protopath.parse(path).mutable_accessor(
        array_gen=array_generator.create_generator('DenseArray'))
    self.assertSetEqual(accessor.required_includes, OPTIONAL_INCLUDES)
    self.assertEqual(accessor.default_name, table.TablePath().Size(name))
    self.assertEqualIgnoringSpaces(
        accessor.lambda_str,
        """
[](auto output_type_meta_fn) constexpr {
  using output_type = typename decltype(output_type_meta_fn)::type;
  using proto_value_type = std::decay_t<decltype(std::declval<output_type>().abc().size())>;
  using value_type = ::arolla::DenseArrayShape;
  using input_type = ::arolla::proto::arolla_optional_value_t<value_type>;
  return [](const input_type& input, output_type* output_ptr) {
    if (!input.present) {
      return;
    }
    auto& final_mutable_field = *output_ptr;
    ::arolla::codegen::io::ResizeRepeatedProtoField(
        final_mutable_field.mutable_abc(), proto_value_type(input.value));
  };
}""",
    )

  @parameterized.parameters(['abc', 'AbC', 'ABC'])
  def test_protopath_array_mutable_accessor(self, prefix):
    path = f'{prefix}[:]/x'
    accessor = protopath.Protopath.parse(path).mutable_accessor(
        array_gen=array_generator.create_generator('DenseArray'))
    self.assertSetEqual(accessor.required_includes, DENSE_ARRAY_INCLUDES)
    self.assertEqual(accessor.default_name,
                     table.TablePath().Child(prefix).Column('x'))
    self.assertEqualIgnoringSpaces(
        accessor.lambda_str,
        """
[](auto output_type_meta_fn) constexpr {
  using output_type = typename decltype(output_type_meta_fn)::type;
  using proto_value_type = std::decay_t<decltype(std::declval<output_type>().abc(0).x())>;
  using value_type = ::arolla::proto::arolla_single_value_t<proto_value_type>;
  using input_type = ::arolla::DenseArray<value_type>;
  return [](const input_type& input, output_type* output_ptr) -> absl::Status {
    size_t total_size = [](const auto& input) {
      size_t total_size = 0;
      size_t final_size = [&]() { return input.abc().size(); }();
      total_size += final_size;
      return total_size;
    }(*output_ptr);

    if (total_size != input.size()) {
      return absl::FailedPreconditionError(absl::StrFormat(
        "unexpected array size for " R"raw_name(%s:)raw_name"
        " proto size %%d vs array size %%d", total_size, input.size()));
    }

    size_t id = 0;
    for (int _fake_var = 0; _fake_var != 1; ++_fake_var) {
      for (auto& value_0 : *(*output_ptr).mutable_abc()) {
        if (input.present(id)) {
          value_0.set_x(proto_value_type(input.values[id]));
        }
        ++id;
      }
    }
    return absl::OkStatus();
  };
}""" % (f'/{prefix}/x'),
    )

  @parameterized.parameters(['abc', 'AbC', 'ABC'])
  def test_protopath_array_mutable_accessor_long_path(self, prefix):
    path = f'{prefix}[:]/x/y'
    accessor = protopath.Protopath.parse(path).mutable_accessor(
        array_gen=array_generator.create_generator('DenseArray'))
    self.assertSetEqual(accessor.required_includes, DENSE_ARRAY_INCLUDES)
    self.assertEqual(accessor.default_name,
                     table.TablePath().Child(prefix).Child('x').Column('y'))
    self.assertEqualIgnoringSpaces(
        accessor.lambda_str,
        """
[](auto output_type_meta_fn) constexpr {
  using output_type = typename decltype(output_type_meta_fn)::type;
  using proto_value_type = std::decay_t<decltype(std::declval<output_type>().abc(0).x().y())>;
  using value_type = ::arolla::proto::arolla_single_value_t<proto_value_type>;
  using input_type = ::arolla::DenseArray<value_type>;
  return [](const input_type& input, output_type* output_ptr) -> absl::Status {
    size_t total_size = [](const auto& input) {
      size_t total_size = 0;
      size_t final_size = [&]() { return input.abc().size(); }();
      total_size += final_size;
      return total_size;
    }(*output_ptr);

    if (total_size != input.size()) {
      return absl::FailedPreconditionError(absl::StrFormat(
        "unexpected array size for " R"raw_name(%s:)raw_name"
        " proto size %%d vs array size %%d", total_size, input.size()));
    }

    size_t id = 0;
    for (int _fake_var = 0; _fake_var != 1; ++_fake_var) {
      for (auto& value_0 : *(*output_ptr).mutable_abc()) {
        if (input.present(id)) {
          auto& final_item = *value_0.mutable_x();
          final_item.set_y(proto_value_type(input.values[id]));
        }
        ++id;
      }
    }
    return absl::OkStatus();
  };
}""" % (f'/{prefix}/x/y'),
    )

  @parameterized.parameters(['abc', 'AbC', 'ABC'])
  def test_protopath_array_mutable_accessor_start_with_single(self, prefix):
    path = f'z/{prefix}[:]/x'
    accessor = protopath.Protopath.parse(path).mutable_accessor(
        array_gen=array_generator.create_generator('DenseArray'))
    self.assertSetEqual(accessor.required_includes, DENSE_ARRAY_INCLUDES)
    self.assertEqual(accessor.default_name,
                     table.TablePath('z').Child(prefix).Column('x'))
    self.assertEqualIgnoringSpaces(
        accessor.lambda_str,
        """
[](auto output_type_meta_fn) constexpr {
  using output_type = typename decltype(output_type_meta_fn)::type;
  using proto_value_type = std::decay_t<decltype(std::declval<output_type>().z().abc(0).x())>;
  using value_type = ::arolla::proto::arolla_single_value_t<proto_value_type>;
  using input_type = ::arolla::DenseArray<value_type>;
  return [](const input_type& input, output_type* output_ptr) -> absl::Status {
    size_t total_size = [](const auto& input) {
      size_t total_size = 0;
      size_t final_size = [&](){
        if (!(AROLLA_PROTO3_COMPATIBLE_HAS(input, z))) { return 0; }
        const auto& final_size_last = input.z();
        return final_size_last.abc().size();
      }();
      total_size += final_size;
      return total_size;
    }(*output_ptr);

    if (total_size != input.size()) {
      return absl::FailedPreconditionError(absl::StrFormat(
        "unexpected array size for " R"raw_name(%s:)raw_name"
        " proto size %%d vs array size %%d", total_size, input.size()));
    }

    size_t id = 0;
    for (int _fake_var = 0; _fake_var != 1; ++_fake_var) {
      if (!(AROLLA_PROTO3_COMPATIBLE_HAS((*output_ptr), z))) {
        continue;
      }
      auto& value_0_last = *(*output_ptr).mutable_z();
      for (auto& value_0 : *value_0_last.mutable_abc()) {
        if (input.present(id)) {
          value_0.set_x(proto_value_type(input.values[id]));
        }
        ++id;
      }
    }
    return absl::OkStatus();
  };
}""" % (f'/z/{prefix}/x'),
    )

  def test_import_proto_class(self):
    self.assertEqual(
        protopath.import_proto_class(
            'arolla.proto.test_pb2.Root'), test_pb2.Root)
    self.assertEqual(
        protopath.import_proto_class(
            'arolla.proto.test_pb2.Inner.Inner2'),
        test_pb2.Inner.Inner2)
    self.assertRaises(ValueError, protopath.import_proto_class,
                      'arolla.proto.test_pb2.NotFound')
    self.assertRaises(ValueError, protopath.import_proto_class,
                      'module.not.found_pb2.MessageName')
    self.assertRaisesRegex(ValueError, '"_pb2."', protopath.import_proto_class,
                           'itertools.Tuple')

  def test_accessor_equality(self):
    cases = [
        protopath.Protopath.parse('/a'),
        protopath.Protopath.parse('/a[:]'),
        protopath.Protopath.parse('/a/b'),
        protopath.Protopath.parse('/[:]/a')
    ]
    for i, x in enumerate(cases):
      for j, y in enumerate(cases):
        if i == j:
          self.assertEqual(x, y)
        else:
          self.assertNotEqual(x, y)


if __name__ == '__main__':
  absltest.main()
