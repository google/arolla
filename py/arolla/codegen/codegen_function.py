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

"""Python tools for arolla_codegen_function BUILD rule."""

import dataclasses
from typing import Iterable

from arolla import arolla

from google.protobuf import descriptor
from arolla.codegen.io import accessors as accessors_lib
from arolla.codegen.io import array_generator
from arolla.codegen.io import protopath


@dataclasses.dataclass
class CodegenFunctionSpec:
  """Specification for a function for end-to-end codegen."""

  @dataclasses.dataclass
  class Argument:
    """Specification of an input or an output of the function."""

    @dataclasses.dataclass
    class ProtoIO:
      # Proto that encodes the argument.
      proto: descriptor.Descriptor
      # List of proto file descriptors, where to look up extensions for
      # input_loader or slot_listener.
      proto_extension_files: tuple[descriptor.FileDescriptor, ...] = tuple()

    @dataclasses.dataclass
    class CustomIO:
      # Fully qualified input loader class name.
      cc_input_loader_getter: str | None = None
      # Fully qualified slot listener class name.
      cc_slot_listener_getter: str | None = None
      # Header names needed for the struct / input loader / slot listener.
      hdrs: tuple[str, ...] = tuple()
      # Deps needed for the struct / input loader / slot listener.
      deps: tuple[str, ...] = tuple()

    # Name of the argument.
    name: str
    # Fully qualified C++ class name.
    cc_class: str
    # Flag that indicates whether the argument is a single proto message or a
    # RepeatedPtrField of messages.
    is_repeated: bool
    # Path to the input/output. It must be consistent with the leaf/export names
    # in the CodegenFunctionSpec.expr.
    table_path: str
    # Specification of the input loader and/or slot listener.
    io: ProtoIO | CustomIO

    def _cc_io_name_prefix(self, cc_namespace: str) -> str:
      """Common name prefix for io getter functions."""
      assert isinstance(self.io, self.ProtoIO)
      class_name = self.io.proto.name
      parent = self.io.proto.containing_type
      while parent is not None:
        class_name = parent.name + '_' + class_name
        parent = parent.containing_type

      class_name = f'::{cc_namespace}::{class_name}'
      if self.is_repeated:
        class_name += '_Repeated'

      return class_name

    def cc_input_loader_getter(self, cc_namespace: str) -> str | None:
      """Returns fully qualified input loader class name."""
      if isinstance(self.io, self.ProtoIO):
        return self._cc_io_name_prefix(cc_namespace) + '_InputLoader'
      else:
        return self.io.cc_input_loader_getter

    def cc_slot_listener_getter(self, cc_namespace: str) -> str | None:
      """Returns fully qualified slot listener class name."""
      if isinstance(self.io, self.ProtoIO):
        return self._cc_io_name_prefix(cc_namespace) + '_SlotListener'
      else:
        return self.io.cc_slot_listener_getter

    @property
    def hdrs(self) -> tuple[str, ...]:
      """C++ header files to be included in generated I/O libraries."""
      if isinstance(self.io, self.ProtoIO):
        # We use ".pb.h" file to transitively include headers needed in child
        # fields. Ideally, we should only include ".proto.h" files for each
        # necessary field,excluding headers only needed in the subtrees of
        # skipped fields to reduce build time. See:
        # http://g3doc/eng/doc/devguide/proto/reference/cpp/cpp-internal.md#why-protoh
        result = [self.io.proto.file.name.removesuffix('.proto') + '.pb.h']
        for ext in self.io.proto_extension_files:
          result.append(ext.name.removesuffix('.proto') + '.pb.h')
        return tuple(result)
      else:
        return self.io.hdrs

    @property
    def deps(self) -> tuple[str, ...]:
      """Deps to be included in generated I/O libraries."""
      if isinstance(self.io, self.ProtoIO):
        result = [_proto_file_to_cc_library(self.io.proto.file)]
        for ext in self.io.proto_extension_files:
          result.append(_proto_file_to_cc_library(ext))
        return tuple(result)
      else:
        return self.io.deps

  # Fully qualified name of the generated C++ function.
  # TODO: Consider using arolla.codegen.io.cpp.CppName.
  cc_function: str
  # Expr that implements the function. Leaves/exports in the expr must be named
  # as '{input.table_path}/{input.name}/{relative_path_in_input_proto}' and
  # '{output.table_path}/{output.name}/{relative_path_in_output_proto}'
  # correspondingly.
  expr: arolla.Expr
  # Input argument details.
  inputs: tuple[Argument, ...]
  # Output argument details.
  outputs: tuple[Argument, ...]
  # Extra header files to be included in the generated code.
  extra_hdrs: tuple[str, ...] = tuple()
  extra_deps: tuple[str, ...] = tuple()


def _proto_file_to_cc_library(
    proto_file: descriptor.FileDescriptor,
) -> str:
  """Guesses cc_proto library name for a proto file."""
  name = '//' + proto_file.name.removesuffix('.proto') + '_cc_proto'
  return ':'.join(name.rsplit('/', 1))


def _generate_proto_accessors(
    proto: descriptor.Descriptor,
    proto_extension_files: Iterable[descriptor.FileDescriptor],
    protopath_prefix: str,
    is_mutable: bool,
    used_names: set[str],
) -> list[tuple[str, accessors_lib.Accessor]]:
  """Returns protopath accessors for the descriptor, filtered by used_names."""
  extension_descriptors = {proto.file, *proto_extension_files}

  array_gen = array_generator.create_generator('DenseArray')
  accessors = protopath.accessors_from_descriptor(
      proto,
      array_gen=array_gen,
      type2extensions=protopath.collect_extensions_per_containing_type(
          extension_descriptors
      ),
      # TODO: make configurable?
      text_cpp_type='::arolla::Bytes',
      protopath_prefix=protopath_prefix,
      mutable=is_mutable,
  )
  return [
      (name, accessor) for name, accessor in accessors if name in used_names
  ]


def generate_input_loaders_spec(cc_namespace: str,
                                fn_specs: dict[str, CodegenFunctionSpec]):
  """Generates a spec for input_loader_set build rule."""

  @dataclasses.dataclass
  class InputLoaderSpec:
    """Internal details of input loader."""

    proto: descriptor.Descriptor
    cc_class: str
    is_repeated: bool
    used_inputs: set[str] = dataclasses.field(default_factory=set)
    hdrs: set[str] = dataclasses.field(default_factory=set)
    proto_extension_files: set[descriptor.FileDescriptor] = dataclasses.field(
        default_factory=set
    )

  loaders = dict()

  for fn_spec in fn_specs.values():
    leaf_keys = set(arolla.get_leaf_keys(fn_spec.expr))
    for arg in fn_spec.inputs:
      if not isinstance(arg.io, CodegenFunctionSpec.Argument.ProtoIO):
        continue

      loader = loaders.setdefault(
          arg.cc_input_loader_getter(cc_namespace),
          InputLoaderSpec(
              proto=arg.io.proto,
              cc_class=arg.cc_class,
              is_repeated=arg.is_repeated,
          ),
      )

      input_path = f'{arg.table_path}/{arg.name}'
      for leaf_key in leaf_keys:
        if leaf_key.startswith(input_path):
          loader.used_inputs.add(leaf_key[len(input_path) :])

      loader.hdrs.update(arg.hdrs)
      loader.hdrs.update(fn_spec.extra_hdrs)
      loader.proto_extension_files.update(arg.io.proto_extension_files)

  result = dict()
  for cc_getter, loader in loaders.items():
    result[cc_getter] = {
        'accessors': _generate_proto_accessors(
            loader.proto,
            proto_extension_files=loader.proto_extension_files,
            protopath_prefix='[:]' if loader.is_repeated else '',
            is_mutable=False,
            used_names=loader.used_inputs,
        ),
        'input_cls': loader.cc_class,
        'hdrs': sorted(loader.hdrs),
    }
  return result


_ANNOTATION_EXPORT_OPS = frozenset((
    arolla.abc.decay_registered_operator('annotation.export'),
    arolla.abc.decay_registered_operator('annotation.export_value'),
))


def _collect_export_annotation_tags(expr: arolla.Expr) -> list[str]:
  """Returns tags used in annotation.export and export_value operators.

  NOTE: The function does not look inside lambdas and other lowerable operators,
  which is consistent with serving behavior.

  Args:
    expr: expr.
  """
  result = set()
  for node in arolla.abc.post_order(expr):
    if (
        node.is_operator
        and arolla.abc.decay_registered_operator(node.op)
        in _ANNOTATION_EXPORT_OPS
    ):
      assert isinstance(
          node.node_deps[1].qvalue, arolla.types.Text
      ), f'expected a text literal, got {node.node_deps[1]:verbose}'
      result.add(node.node_deps[1].qvalue.py_value())
  return sorted(result)


def generate_slot_listeners_spec(
    cc_namespace: str, fn_specs: dict[str, CodegenFunctionSpec]
):
  """Generates a spec for slot_listener_set build rule."""

  @dataclasses.dataclass
  class SlotListenerSpec:
    """Internal details of slot listener."""

    proto: descriptor.Descriptor
    cc_class: str
    is_repeated: bool
    used_outputs: set[str] = dataclasses.field(default_factory=set)
    hdrs: set[str] = dataclasses.field(default_factory=set)
    proto_extension_files: set[descriptor.FileDescriptor] = dataclasses.field(
        default_factory=set
    )

  listeners = dict()

  for fn_spec in fn_specs.values():
    exports = _collect_export_annotation_tags(fn_spec.expr)
    for arg in fn_spec.outputs:
      if not isinstance(arg.io, CodegenFunctionSpec.Argument.ProtoIO):
        continue

      listener = listeners.setdefault(
          arg.cc_slot_listener_getter(cc_namespace),
          SlotListenerSpec(
              proto=arg.io.proto,
              cc_class=arg.cc_class,
              is_repeated=arg.is_repeated,
          ),
      )

      input_path = f'{arg.table_path}/{arg.name}'
      for export in exports:
        if export.startswith(input_path):
          listener.used_outputs.add(export[len(input_path) :])

      listener.hdrs.update(arg.hdrs)
      listener.hdrs.update(fn_spec.extra_hdrs)
      listener.proto_extension_files.update(arg.io.proto_extension_files)

  result = dict()
  for cc_getter, listener in listeners.items():
    result[cc_getter] = {
        'accessors': _generate_proto_accessors(
            listener.proto,
            proto_extension_files=listener.proto_extension_files,
            protopath_prefix='[:]' if listener.is_repeated else '',
            is_mutable=True,
            used_names=listener.used_outputs,
        ),
        'output_cls': listener.cc_class,
        'hdrs': sorted(listener.hdrs),
    }
  return result


def get_expr(
    fn_specs: dict[str, CodegenFunctionSpec], model_name: str
) -> arolla.Expr:
  """Extracts expr with the given name from the spec."""
  assert fn_specs[model_name].expr is not None
  return fn_specs[model_name].expr


def get_hdrs(
    fn_specs: dict[str, CodegenFunctionSpec],
    hdrs: Iterable[str],
) -> tuple[str, ...]:
  """Extracts expr with the given name from the spec."""
  hdrs = set(hdrs)
  for fn_spec in fn_specs.values():
    hdrs.update(fn_spec.extra_hdrs)
    for arg in fn_spec.inputs:
      hdrs.update(arg.hdrs)
    for arg in fn_spec.outputs:
      hdrs.update(arg.hdrs)
  return tuple(sorted(hdrs))
