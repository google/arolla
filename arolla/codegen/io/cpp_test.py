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

"""Tests for arolla.codegen.cpp."""

from absl.testing import absltest

from arolla.codegen.io import cpp


class CppTest(absltest.TestCase):

  def test_include(self):
    self.assertEqual(
        cpp.Include('x/y/z.hpp').include_str, '#include "x/y/z.hpp"')
    self.assertEqual(
        cpp.Include('"x/y/z.hpp"').include_str, '#include "x/y/z.hpp"')
    self.assertEqual(cpp.Include('<tuple>').include_str, '#include <tuple>')
    self.assertEqual(cpp.Include('x/y/z.hpp'), cpp.Include('"x/y/z.hpp"'))
    self.assertLen({cpp.Include('x/y/z.hpp'), cpp.Include('"x/y/z.hpp"')}, 1)
    self.assertLess(cpp.Include('x/y/z.hpp'), cpp.Include('"x/z/y.hpp"'))
    self.assertListEqual(
        list(sorted([cpp.Include('"x/z/y.hpp"'),
                     cpp.Include('x/y/z.hpp')])),
        [cpp.Include('x/y/z.hpp'),
         cpp.Include('"x/z/y.hpp"')])

  def test_cpp_name_no_namespace(self):
    for name in [
        cpp.CppName('', 'Class'),
        cpp.CppName.from_full_name('Class'),
        cpp.CppName.from_full_name('::Class')
    ]:
      self.assertEqual(name.namespace, '')
      self.assertEqual(name.name, 'Class')
      self.assertEqual(name.full_name, 'Class')
      self.assertEqual(name.open_namespace_str, '')
      self.assertEqual(name.close_namespace_str, '')

  def test_cpp_name_with_namespace(self):
    for name in [
        cpp.CppName('::one::two::three', 'Class'),
        cpp.CppName('one::two::three', 'Class'),
        cpp.CppName.from_full_name('::one::two::three::Class'),
        cpp.CppName.from_full_name('one::two::three::Class')
    ]:
      self.assertEqual(name.namespace, 'one::two::three')
      self.assertEqual(name.name, 'Class')
      self.assertEqual(name.full_name, '::one::two::three::Class')
      self.assertEqual(name.open_namespace_str, 'namespace one::two::three {')
      self.assertEqual(name.close_namespace_str,
                       '}  // namespace one::two::three')


if __name__ == '__main__':
  absltest.main()
