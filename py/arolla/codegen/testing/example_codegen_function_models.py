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

"""Test configuration for end2end function codegen."""

from arolla import arolla
from arolla.codegen import codegen_function
from arolla.codegen.testing import example_codegen_function_extensions_pb2
from arolla.codegen.testing import example_codegen_function_pb2

M = arolla.M
L = arolla.L


def first_expr():
  """Returns test expr for end2end function codegen."""
  # TODO(b/313619343) Populate types automatically?
  a = M.annotation.qtype(L['/scalar_input/a'], arolla.OPTIONAL_FLOAT32)
  string_field = M.annotation.qtype(
      L['/scalar_input/string_field'], arolla.OPTIONAL_BYTES
  )
  array_1_a = M.annotation.qtype(
      L['/arrays/array_input_1/a'], arolla.DENSE_ARRAY_FLOAT32
  )
  array_2_c = M.annotation.qtype(
      L['/arrays/array_input_2/c'], arolla.DENSE_ARRAY_FLOAT32
  )
  array_2_extension_foo_a = M.annotation.qtype(
      L[
          '/arrays/array_input_2/'
          'Ext::test_namespace.FooExtension.extension_foo/foo/a'
      ],
      arolla.DENSE_ARRAY_FLOAT32,
  )
  array_input_size = M.annotation.qtype(
      L['/arrays/@size'], arolla.types.DENSE_ARRAY_SHAPE
  )
  # Testing that repeated fields in the inputs are also supported.
  nested_foo_a = M.annotation.qtype(
      L['/arrays/array_input_2/nested_foo/a'], arolla.types.DENSE_ARRAY_FLOAT32
  )
  nested_foo_sizes = M.annotation.qtype(
      L['/arrays/array_input_2/nested_foo/@size'],
      arolla.types.DENSE_ARRAY_INT64,
  )

  nested_foo_to_array = M.edge.from_sizes_or_shape(nested_foo_sizes)
  array_to_scalar = M.edge.from_sizes_or_shape(array_input_size)

  mean_array_1_a = M.math.mean(array_1_a, array_to_scalar)
  mean_nested_foo_a = M.math.mean(nested_foo_a, nested_foo_to_array)

  scalar_output = (
      mean_array_1_a + a + M.core.to_float32(M.strings.length(string_field))
  )
  array_output_1 = (
      array_1_a + array_2_c + mean_nested_foo_a + array_2_extension_foo_a
  )
  # Nested array output
  array_output_2 = M.array.expand(array_1_a, nested_foo_to_array) + nested_foo_a

  return M.annotation.export_value(
      M.annotation.export_value(
          M.annotation.export_value(
              M.annotation.export_value(
                  M.annotation.export_value(
                      1.0, '/scalar_output/result', scalar_output
                  ),
                  '/arrays/array_output_1/result',
                  array_output_1,
              ),
              '/arrays/nested_foo/array_output_2/result',
              array_output_2,
          ),
          '/arrays/@size',
          array_input_size,
      ),
      '/arrays/nested_foo/@size',
      M.core.shape_of(nested_foo_a),
  )


def first_function_spec():
  """Constructs CodegenFunctionSpec for first_expr."""
  return codegen_function.CodegenFunctionSpec(
      cc_function='::test_namespace::FirstFunction',
      expr=first_expr(),
      inputs=(
          codegen_function.CodegenFunctionSpec.Argument(
              name='scalar_input',
              is_repeated=False,
              table_path='',
              cc_class='::test_namespace::FooInput',
              io=codegen_function.CodegenFunctionSpec.Argument.ProtoIO(
                  proto=example_codegen_function_pb2.FooInput.DESCRIPTOR,
              ),
          ),
          codegen_function.CodegenFunctionSpec.Argument(
              name='array_input_1',
              is_repeated=True,
              table_path='/arrays',
              # TODO(b/313619343) Providing another cc_class for the same proto
              # / is_repeated will cause a build error due to a collision.
              cc_class='absl::Span<const ::test_namespace::FooInput>',
              io=codegen_function.CodegenFunctionSpec.Argument.ProtoIO(
                  proto=example_codegen_function_pb2.FooInput.DESCRIPTOR,
              ),
          ),
          codegen_function.CodegenFunctionSpec.Argument(
              name='array_input_2',
              is_repeated=True,
              table_path='/arrays',
              cc_class='std::vector<::test_namespace::FooInput::BarInput>',
              io=codegen_function.CodegenFunctionSpec.Argument.ProtoIO(
                  proto=example_codegen_function_pb2.FooInput.BarInput.DESCRIPTOR,
                  proto_extension_files=(
                      example_codegen_function_extensions_pb2.DESCRIPTOR,
                  ),
              ),
          ),
      ),
      outputs=(
          codegen_function.CodegenFunctionSpec.Argument(
              name='scalar_output',
              is_repeated=False,
              table_path='',
              cc_class='::test_namespace::Output',
              io=codegen_function.CodegenFunctionSpec.Argument.ProtoIO(
                  proto=example_codegen_function_pb2.Output.DESCRIPTOR,
              ),
          ),
          codegen_function.CodegenFunctionSpec.Argument(
              name='array_output_1',
              is_repeated=True,
              table_path='/arrays',
              cc_class='std::vector<::test_namespace::Output>',
              io=codegen_function.CodegenFunctionSpec.Argument.ProtoIO(
                  proto=example_codegen_function_pb2.Output.DESCRIPTOR,
              ),
          ),
          codegen_function.CodegenFunctionSpec.Argument(
              name='array_output_2',
              is_repeated=True,
              cc_class='std::vector<::test_namespace::Output>',
              table_path='/arrays/nested_foo',
              io=codegen_function.CodegenFunctionSpec.Argument.ProtoIO(
                  proto=example_codegen_function_pb2.Output.DESCRIPTOR,
              ),
          ),
      ),
      extra_hdrs=('absl/types/span.h',),
      extra_deps=('//third_party/absl/types:span',),
  )


def first_function_spec_with_struct_input():
  """Same as first_function_spec, but replaces some protos with structs."""
  spec = first_function_spec()
  spec.cc_function = '::test_namespace::FirstFunctionOnStruct'
  spec.inputs[0].cc_class = '::test_namespace::FooInputStruct'
  spec.inputs[0].io = codegen_function.CodegenFunctionSpec.Argument.CustomIO(
      hdrs=(
          (
              'py/arolla/codegen/testing/'
              'example_codegen_function_struct.h'
          ),
          (
              'py/arolla/codegen/testing/'
              'example_codegen_function_struct_input_loader.h'
          ),
      ),
      deps=(
          (
              '//py/arolla/codegen/testing:'
              'example_codegen_function_struct'
          ),
          (
              '//py/arolla/codegen/testing:'
              'example_codegen_function_struct_input_loader'
          ),
      ),
      cc_input_loader_getter='::test_namespace::FooStructInputLoader',
  )
  spec.outputs[1].cc_class = (
      'std::vector<::test_namespace::ScoringOutputStruct>'
  )
  spec.outputs[1].io = codegen_function.CodegenFunctionSpec.Argument.CustomIO(
      hdrs=(
          (
              'py/arolla/codegen/testing/'
              'example_codegen_function_struct.h'
          ),
          (
              'py/arolla/codegen/testing/'
              'example_codegen_function_struct_vector_slot_listener.h'
          ),
      ),
      deps=(
          (
              '//py/arolla/codegen/testing:'
              'example_codegen_function_struct'
          ),
          (
              '//py/arolla/codegen/testing:'
              'example_codegen_function_struct_vector_slot_listener'
          ),
      ),
      cc_slot_listener_getter=(
          '::test_namespace::ScoringOutputStructVectorSlotListener'
      ),
  )
  return spec


def second_expr():
  """Returns test expr for end2end function codegen."""
  # TODO(b/313619343) Populate types automatically?
  a = M.annotation.qtype(L['/scalar_input/a'], arolla.OPTIONAL_FLOAT32)
  string_field = M.annotation.qtype(
      L['/scalar_input/string_field'], arolla.OPTIONAL_BYTES
  )
  aa = M.annotation.qtype(
      L['/arrays/array_input/a'], arolla.DENSE_ARRAY_FLOAT32
  )
  array_input_size = M.annotation.qtype(
      L['/arrays/@size'], arolla.types.DENSE_ARRAY_SHAPE
  )

  array_to_scalar = M.edge.from_sizes_or_shape(array_input_size)
  mean_aa = M.math.mean(aa, array_to_scalar)

  scalar_output = mean_aa + a
  array_output = (
      aa + mean_aa + M.core.to_float32(M.strings.length(string_field))
  )

  return M.annotation.export_value(
      M.annotation.export_value(
          M.annotation.export_value(
              1.0,
              '/scalar_output/Ext::test_namespace.OutputExtension.extension_output/extra_result',
              scalar_output,
          ),
          '/arrays/array_output/result',
          array_output,
      ),
      '/arrays/@size',
      array_input_size,
  )


def second_function_spec():
  """Constructs CodegenFunctionSpec for second_expr."""
  return codegen_function.CodegenFunctionSpec(
      cc_function='::test_namespace::SecondFunction',
      expr=second_expr(),
      inputs=(
          codegen_function.CodegenFunctionSpec.Argument(
              name='scalar_input',
              is_repeated=False,
              table_path='',
              cc_class='::test_namespace::FooInput',
              io=codegen_function.CodegenFunctionSpec.Argument.ProtoIO(
                  proto=example_codegen_function_pb2.FooInput.DESCRIPTOR
              ),
          ),
          # Different name from first_function_spec, yet the codegenerated
          # input_loader must be shared.
          codegen_function.CodegenFunctionSpec.Argument(
              name='array_input',
              is_repeated=True,
              table_path='/arrays',
              # TODO(b/313619343) Providing another cc_class for the same proto
              # / is_repeated will cause a build error due to a collision.
              cc_class='absl::Span<const ::test_namespace::FooInput>',
              io=codegen_function.CodegenFunctionSpec.Argument.ProtoIO(
                  proto=example_codegen_function_pb2.FooInput.DESCRIPTOR
              ),
          ),
      ),
      outputs=(
          codegen_function.CodegenFunctionSpec.Argument(
              name='scalar_output',
              is_repeated=False,
              table_path='',
              cc_class='::test_namespace::Output',
              io=codegen_function.CodegenFunctionSpec.Argument.ProtoIO(
                  proto=example_codegen_function_pb2.Output.DESCRIPTOR,
                  proto_extension_files=(
                      example_codegen_function_extensions_pb2.DESCRIPTOR,
                  ),
              ),
          ),
          # Different name from first_function_spec, yet the codegenerated
          # slot_listener must be shared.
          codegen_function.CodegenFunctionSpec.Argument(
              name='array_output',
              is_repeated=True,
              table_path='/arrays',
              cc_class='std::vector<::test_namespace::Output>',
              io=codegen_function.CodegenFunctionSpec.Argument.ProtoIO(
                  proto=example_codegen_function_pb2.Output.DESCRIPTOR
              ),
          ),
      ),
      extra_hdrs=('absl/types/span.h',),
      extra_deps=('//third_party/absl/types:span',),
  )
