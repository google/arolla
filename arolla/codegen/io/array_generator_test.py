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

"""Tests for arolla.codegen.array_generator."""

from absl.testing import absltest

from arolla.codegen.io import array_generator


class ArrayGeneratorTest(absltest.TestCase):

  def assertEqualIgnoringSpaces(self, a, b):
    def normalize(x):
      return '\n'.join(x.split())

    self.assertEqual(normalize(a), normalize(b))

  def test_dense_array(self):
    g = array_generator._DenseArrayCodeGenerator('xxx_')
    self.assertEqualIgnoringSpaces(
        g.define_array('int32_t', 'size_var', 'factory'),
        """
        using xxx_result_type = ::arolla::DenseArray<int32_t>;
        typename ::arolla::Buffer<int32_t>::Builder xxx_bldr(
            size_var, factory);
        auto xxx_inserter = bldr.GetInserter();
        int64_t xxx_id = 0;
        ::arolla::bitmap::AlmostFullBuilder xxx_bitmap_bldr(
            size_var, factory);
        """,
    )
    self.assertEqualIgnoringSpaces(
        g.push_back_empty_item(),
        """
        xxx_bitmap_bldr.AddMissed(xxx_id++);
        xxx_inserter.SkipN(1);
    """,
    )
    self.assertEqualIgnoringSpaces(
        g.push_back_item('val'),
        """
          xxx_id++;
          xxx_inserter.Add(val);
        """,
    )
    self.assertEqualIgnoringSpaces(
        g.return_array(),
        """
          return xxx_result_type{std::move(xxx_bldr).Build(),
                                 std::move(xxx_bitmap_bldr).Build()};""",
    )
    self.assertEqualIgnoringSpaces(
        g.create_shape('xxx'), '::arolla::DenseArrayShape{xxx}'
    )

  def test_create_generator(self):
    self.assertIsNone(array_generator.create_generator('', 'x_'))
    array_gen = array_generator.create_generator('DenseArray', 'x_')
    self.assertIsNotNone(array_gen)
    if array_gen:
      self.assertEqualIgnoringSpaces(
          array_gen.push_back_empty_item(),
          'x_bitmap_bldr.AddMissed(x_id++);\nx_inserter.SkipN(1);\n',
      )


if __name__ == '__main__':
  absltest.main()
