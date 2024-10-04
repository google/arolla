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

"""Functions for generation of accessors from bzl."""

import dataclasses
import importlib
from typing import Callable, Iterable, List, Optional, Tuple

from google.protobuf import text_format
from arolla.codegen.io import accessors
from arolla.codegen.io import array_generator
from arolla.codegen.io import protopath
from arolla.proto import io_pb2


@dataclasses.dataclass
class Config:
  """Configuration of InputLoader or SlotListener available for accessors."""
  # fully qualified C++ type of the input_cls or output_cls
  io_cpp_type: str
  # type of the array to use for accessors ('' or 'DenseArray')
  array_type: str
  # True iff accessor should mutate io_cpp_type (aka SlotListener)
  is_mutable: bool


def path_accessor(
    path: str,
    name: str) -> Callable[[Config], List[Tuple[str, accessors.Accessor]]]:
  """Returns callable returning single path accessor with the given name."""

  def gen(cfg: Config) -> List[Tuple[str, accessors.Accessor]]:
    del cfg  # unused
    return [(name, accessors.path_accessor(path, name))]

  return gen


def merge_accessor_list(
    *accessor_generators: Callable[[Config], List[Tuple[str,
                                                        accessors.Accessor]]]
) -> Callable[[Config], List[Tuple[str, accessors.Accessor]]]:
  """Returns callable returning single path accessor with the given name."""

  def gen(cfg: Config) -> List[Tuple[str, accessors.Accessor]]:
    return sum([gen(cfg) for gen in accessor_generators], [])

  return gen


def allowed_from_set(names: Iterable[str]) -> Callable[[str], bool]:
  """Returns functor that allow names from the given set."""
  names = set(names)
  return lambda name: name in names


def filtered_by_name_accessor(
    accessor_generator: Callable[[Config], List[Tuple[str,
                                                      accessors.Accessor]]],
    is_allowed: Callable[[str], bool]
) -> Callable[[Config], List[Tuple[str, accessors.Accessor]]]:
  """Returns callable returning accessors keeping only is_allowed(name)."""

  def gen(cfg: Config) -> List[Tuple[str, accessors.Accessor]]:
    return [(name, acc)
            for name, acc in accessor_generator(cfg)
            if is_allowed(name or acc.default_name)]

  return gen


def filtered_for_models_accessor(
    accessor_generator: Callable[
        [Config], List[Tuple[str, accessors.Accessor]]
    ],
    model_io_info_files: List[str],
    *,
    strip_prefix: str = ''
) -> Callable[[Config], List[Tuple[str, accessors.Accessor]]]:
  """Returns callable keeping accessors required for at least one model."""

  model_inputs = set()
  model_outputs = set()

  for fname in model_io_info_files:
    with open(fname, 'r') as f:
      info = io_pb2.ModelInputOutputInfo()
      text_format.Parse(f.read(), info)
      model_inputs |= {i.name for i in info.inputs}
      model_outputs |= {o.name for o in info.side_outputs}

  if strip_prefix:
    model_inputs = {
        i.removeprefix(strip_prefix)
        for i in model_inputs
        if i.startswith(strip_prefix)
    }
    model_outputs = {
        o.removeprefix(strip_prefix)
        for o in model_outputs
        if o.startswith(strip_prefix)
    }

  def gen(cfg: Config) -> List[Tuple[str, accessors.Accessor]]:
    allowed_names = model_outputs if cfg.is_mutable else model_inputs
    return [(name, acc)
            for name, acc in accessor_generator(cfg)
            if (name or acc.default_name) in allowed_names]

  return gen


def protopath_accessor(
    path: str, name: str,
    cpp_type: str) -> Callable[[Config], List[Tuple[str, accessors.Accessor]]]:
  """Returns callable returning single protopath accessor."""

  def gen(cfg: Config) -> List[Tuple[str, accessors.Accessor]]:
    ppath = protopath.Protopath.parse(path, input_type=cfg.io_cpp_type)
    array_gen = array_generator.create_generator(cfg.array_type)
    if cfg.is_mutable:
      accessor = ppath.mutable_accessor(array_gen=array_gen, cpp_type=cpp_type)
    else:
      accessor = ppath.accessor(array_gen=array_gen, cpp_type=cpp_type)
    return [(name, accessor)]

  return gen


def protopath_descriptor_accessors(
    proto_name: str,
    extension_modules: Iterable[str],
    text_cpp_type: str,
    skip_field_fn: Optional[str] = None,
    naming_policy: str = 'default',
    protopath_prefix: str = ''
) -> Callable[[Config], List[Tuple[str, accessors.Accessor]]]:
  """Returns callable returning accessors from proto descriptor.

  Args:
    proto_name: fully qualified python proto name.
    extension_modules: (list[str]) list of fully qualified python proto packages
      to find extensions from.
    text_cpp_type: fully qualified cpp type with constructor from
      std::string. Python proto library must be added as dependency to the
        script.
    skip_field_fn: fully qualified function name -- determines which fields to
      skip.
    naming_policy: one of ["default", "single_underscore", "double_underscore",
      "leaf_only"].
    protopath_prefix: prefix for all protopath's generated by the accessor.
  """

  proto = protopath.import_proto_class(proto_name)
  imported_skip_field_fn = protopath.no_skip_fn
  if skip_field_fn:
    skip_module_name, skip_name = skip_field_fn.rsplit('.', 1)
    skip_module = importlib.import_module(skip_module_name)
    imported_skip_field_fn = getattr(skip_module, skip_name)
  descriptor = proto.DESCRIPTOR
  extension_descriptors = {descriptor.file}
  for proto_module_name in extension_modules:
    extension_descriptors.add(
        importlib.import_module(proto_module_name).DESCRIPTOR)

  def gen(cfg: Config) -> List[Tuple[str, accessors.Accessor]]:
    array_gen = array_generator.create_generator(cfg.array_type)
    return protopath.accessors_from_descriptor(
        descriptor,
        array_gen=array_gen,
        type2extensions=protopath.collect_extensions_per_containing_type(
            extension_descriptors),
        default_name_modifier_fn=protopath
        .default_name_modifiers[naming_policy],
        text_cpp_type=text_cpp_type,
        skip_field_fn=imported_skip_field_fn,
        protopath_prefix=protopath_prefix,
        mutable=cfg.is_mutable)

  return gen


def wildcard_protopath_accessor(
    ppath: str, name: str,
    cpp_type: str) -> Callable[[Config], List[Tuple[str, accessors.Accessor]]]:
  """Returns callable returning single wildcard accessor."""

  def gen(cfg: Config) -> List[Tuple[str, accessors.Accessor]]:
    if cfg.is_mutable:
      raise ValueError('Mutable wildcard_protopath_accessor is not supported')
    array_gen = array_generator.create_generator(cfg.array_type)
    accessor = protopath.Protopath.parse_wildcard(
        ppath, input_type=cfg.io_cpp_type).accessor(
            array_gen, cpp_type=cpp_type or None)
    return [(name or accessor.default_name, accessor)]

  return gen
