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

"""Tests for codegen_function."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.codegen import codegen_function
from arolla.codegen.testing import example_codegen_function_extensions_pb2
from arolla.codegen.testing import example_codegen_function_models
from arolla.codegen.testing import example_codegen_function_pb2


M = arolla.M
L = arolla.L


class CodegenFunctionTest(parameterized.TestCase):

  def test_scalar_proto_argument(self):
    pa = codegen_function.CodegenFunctionSpec.Argument(
        name='foo',
        is_repeated=False,
        table_path='/foo/bar',
        cc_class='::test_namespace::FooInput',
        io=codegen_function.CodegenFunctionSpec.Argument.ProtoIO(
            proto=example_codegen_function_pb2.FooInput.DESCRIPTOR,
            proto_extension_files=(
                example_codegen_function_extensions_pb2.DESCRIPTOR,
            ),
        ),
    )
    self.assertEqual(pa.cc_class, '::test_namespace::FooInput')
    self.assertEqual(
        pa.cc_input_loader_getter('package_name_target_name'),
        '::package_name_target_name::FooInput_InputLoader',
    )
    self.assertEqual(
        pa.cc_slot_listener_getter('package_name_target_name'),
        '::package_name_target_name::FooInput_SlotListener',
    )
    self.assertEqual(
        pa.hdrs,
        (
            'py/arolla/codegen/testing/example_codegen_function.pb.h',
            'py/arolla/codegen/testing/example_codegen_function_extensions.pb.h',
        ),
    )
    self.assertEqual(
        pa.deps,
        (
            '//py/arolla/codegen/testing:example_codegen_function_cc_proto',
            '//py/arolla/codegen/testing:example_codegen_function_extensions_cc_proto',
        ),
    )

  def test_repeated_proto_argument(self):
    pa = codegen_function.CodegenFunctionSpec.Argument(
        name='foo',
        is_repeated=True,
        table_path='/foo/bar',
        cc_class='std::vector<::test_namespace::FooInput>',
        io=codegen_function.CodegenFunctionSpec.Argument.ProtoIO(
            proto=example_codegen_function_pb2.FooInput.DESCRIPTOR,
            proto_extension_files=(
                example_codegen_function_extensions_pb2.DESCRIPTOR,
            ),
        ),
    )
    self.assertEqual(pa.cc_class, 'std::vector<::test_namespace::FooInput>')
    self.assertEqual(
        pa.cc_input_loader_getter('package_name_target_name'),
        '::package_name_target_name::FooInput_Repeated_InputLoader',
    )
    self.assertEqual(
        pa.cc_slot_listener_getter('package_name_target_name'),
        '::package_name_target_name::FooInput_Repeated_SlotListener',
    )

  def test_weird_proto(self):
    class FakeMessageDescriptor:
      """Fake proto descriptor with names discouraged by the style guide."""

      class _FakeFileDescriptor:
        name = 'foo/bar/weird-proto-file_name.proto'
        package = ''  # NOTE: Testing empty package name.

      file = _FakeFileDescriptor()
      name = 'FooMessage'
      full_name = 'FooMessage'
      containing_type = None

    class FakeExtensionFileDescriptor:
      name = 'foo/bar/weird-extension-proto-file_name.proto'
      package = ''

    pa = codegen_function.CodegenFunctionSpec.Argument(
        name='foo',
        is_repeated=False,
        table_path='/foo/bar',
        cc_class='::FooMessage',
        io=codegen_function.CodegenFunctionSpec.Argument.ProtoIO(  # pytype: disable=wrong-arg-types
            proto=FakeMessageDescriptor,
            proto_extension_files=(FakeExtensionFileDescriptor,),
        ),
    )
    self.assertEqual(pa.cc_class, '::FooMessage')
    self.assertEqual(
        pa.cc_input_loader_getter('package_name_target_name'),
        '::package_name_target_name::FooMessage_InputLoader',
    )
    self.assertEqual(
        pa.cc_slot_listener_getter('package_name_target_name'),
        '::package_name_target_name::FooMessage_SlotListener',
    )
    self.assertEqual(
        pa.hdrs,
        (
            'foo/bar/weird-proto-file_name.pb.h',
            'foo/bar/weird-extension-proto-file_name.pb.h',
        ),
    )
    self.assertEqual(
        pa.deps,
        (
            '//foo/bar:weird-proto-file_name_cc_proto',
            '//foo/bar:weird-extension-proto-file_name_cc_proto',
        ),
    )

  def test_collect_export_annotation_tags(self):
    expr = M.annotation.export_value(
        M.annotation.export(L.x, 'foo'),
        'bar',
        M.annotation.export_value(L.y, 'baz', L.z),
    )
    self.assertEqual(
        codegen_function._collect_export_annotation_tags(expr),
        [
            'bar',
            'baz',
            'foo',
        ],
    )

  def test_generate_proto_accessors(self):
    used_names = [
        '/Ext::test_namespace.FooExtension.extension_foo/foo/a',
        '/Ext::test_namespace.FooExtension.extension_foo/foo/string_field',
        '/c',
        '/nested_foo/@size',
        '/nested_foo/a',
        '/nested_foo/string_field',
    ]
    accessors = codegen_function._generate_proto_accessors(
        proto=example_codegen_function_pb2.FooInput.BarInput.DESCRIPTOR,
        proto_extension_files=(
            example_codegen_function_extensions_pb2.DESCRIPTOR,
        ),
        protopath_prefix='[:]',
        is_mutable=False,
        used_names=set(used_names),
    )
    self.assertEqual([n for n, _ in accessors], used_names)

  def test_generate_input_loaders_spec(self):
    spec = codegen_function.generate_input_loaders_spec(
        'package_name_target_name',
        {
            'first': example_codegen_function_models.first_function_spec(),
            'second': example_codegen_function_models.second_function_spec(),
        },
    )
    self.assertEqual(
        list(spec.keys()),
        [
            '::package_name_target_name::FooInput_InputLoader',
            '::package_name_target_name::FooInput_Repeated_InputLoader',
            '::package_name_target_name::FooInput_BarInput_Repeated_InputLoader',
        ],
    )

    with self.subTest('foo_input_loader'):
      foo_input_loader = spec[
          '::package_name_target_name::FooInput_InputLoader'
      ]
      self.assertEqual(
          foo_input_loader['input_cls'], '::test_namespace::FooInput'
      )
      self.assertEqual(
          [x[0] for x in foo_input_loader['accessors']],
          ['/a', '/string_field'],
      )
      self.assertEqual(
          foo_input_loader['hdrs'],
          [
              'absl/types/span.h',
              'py/arolla/codegen/testing/example_codegen_function.pb.h',
          ],
      )

    with self.subTest('foo_vector_input_loader'):
      foo_vector_input_loader = spec[
          '::package_name_target_name::FooInput_Repeated_InputLoader'
      ]
      self.assertEqual(
          foo_vector_input_loader['input_cls'],
          'absl::Span<const ::test_namespace::FooInput>',
      )
      self.assertEqual(
          [x[0] for x in foo_vector_input_loader['accessors']],
          ['/a'],
      )
      self.assertEqual(
          foo_vector_input_loader['hdrs'],
          [
              'absl/types/span.h',
              'py/arolla/codegen/testing/example_codegen_function.pb.h',
          ],
      )

    with self.subTest('bar_vector_input_loader'):
      bar_vector_input_loader = spec[
          '::package_name_target_name::FooInput_BarInput_Repeated_InputLoader'
      ]
      self.assertEqual(
          bar_vector_input_loader['input_cls'],
          'std::vector<::test_namespace::FooInput::BarInput>',
      )
      self.assertEqual(
          [x[0] for x in bar_vector_input_loader['accessors']],
          [
              '/Ext::test_namespace.FooExtension.extension_foo/foo/a',
              '/c',
              '/nested_foo/@size',
              '/nested_foo/a',
          ],
      )
      self.assertEqual(
          bar_vector_input_loader['hdrs'],
          [
              'absl/types/span.h',
              'py/arolla/codegen/testing/example_codegen_function.pb.h',
              'py/arolla/codegen/testing/example_codegen_function_extensions.pb.h',
          ],
      )

  def test_generate_slot_listeners_spec(self):
    spec = codegen_function.generate_slot_listeners_spec(
        'package_name_target_name',
        {
            'first': example_codegen_function_models.first_function_spec(),
            'second': example_codegen_function_models.second_function_spec(),
        },
    )
    self.assertEqual(
        list(spec.keys()),
        [
            '::package_name_target_name::Output_SlotListener',
            '::package_name_target_name::Output_Repeated_SlotListener',
        ],
    )

    with self.subTest('output_listener'):
      output_listener = spec['::package_name_target_name::Output_SlotListener']
      self.assertEqual(
          output_listener['output_cls'], '::test_namespace::Output'
      )
      self.assertEqual(
          [x[0] for x in output_listener['accessors']],
          [
              '/Ext::test_namespace.OutputExtension.extension_output/extra_result',
              '/result',
          ],
      )
      self.assertEqual(
          output_listener['hdrs'],
          [
              'absl/types/span.h',
              'py/arolla/codegen/testing/example_codegen_function.pb.h',
              'py/arolla/codegen/testing/example_codegen_function_extensions.pb.h',
          ],
      )

    with self.subTest('output_vector_slot_listener'):
      output_vector_slot_listener = spec[
          '::package_name_target_name::Output_Repeated_SlotListener'
      ]
      self.assertEqual(
          output_vector_slot_listener['output_cls'],
          'std::vector<::test_namespace::Output>',
      )
      self.assertEqual(
          [x[0] for x in output_vector_slot_listener['accessors']],
          ['/result'],
      )
      self.assertEqual(
          output_vector_slot_listener['hdrs'],
          [
              'absl/types/span.h',
              'py/arolla/codegen/testing/example_codegen_function.pb.h',
          ],
      )


if __name__ == '__main__':
  absltest.main()
