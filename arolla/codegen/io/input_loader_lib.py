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

"""Codegeneration utilities for loading data from inputs."""

import collections
from typing import Any, Dict, List, Set, Tuple

from arolla.codegen.io import accessor_generator
from arolla.codegen.io import accessors
from arolla.codegen.io import cpp
from arolla.codegen.io import jinja_util
from arolla.codegen.io import multi_protopath
from arolla.codegen.io import protopath
import arolla.codegen.utils as codegen_utils


class _AccessorsCollection:
  """Collection of accessors in a different forms."""

  def __init__(self, accessors_list: List[Tuple[str, accessors.Accessor]]):
    accessors_list = accessors.sorted_accessor_list(accessors_list)

    accessors_list, self.single_value_protopath_tree = (
        multi_protopath.extract_single_value_protopath_accessors(accessors_list)
    )
    accessors_list, self.multi_value_protopath_tree = (
        multi_protopath.extract_multi_value_protopath_accessors(accessors_list)
    )
    self.accessors_list = accessors.AccessorsList(accessors_list)

  def required_includes(self) -> Set[cpp.Include]:
    extra_includes = set(self.accessors_list.required_includes)
    extra_includes = extra_includes.union(
        self.single_value_protopath_tree.required_includes
    )
    extra_includes = extra_includes.union(
        self.multi_value_protopath_tree.required_includes
    )
    return extra_includes


class _LoaderSpec:
  """Specification of an input loader."""

  def __init__(self, spec: Dict[str, Any]):
    input_cls_name = spec['input_cls']
    if codegen_utils.is_function_call(input_cls_name):
      input_cls_name = codegen_utils.call_function(input_cls_name)
    self.input_cls = cpp.CppName.from_full_name(input_cls_name)

    self.shard_count = (
        int(spec['sharding'].get('shard_count', 0)) if 'sharding' in spec else 0
    )
    self.array_type = (
        spec['array_type'] if 'array_type' in spec else 'DenseArray'
    )

    accessors_list = []
    config = accessor_generator.Config(
        io_cpp_type=input_cls_name, array_type=self.array_type, is_mutable=False
    )
    for acc in spec['accessors']:
      if isinstance(acc, tuple):
        accessors_list.append(acc)
      elif isinstance(acc, dict):
        gen_fn = codegen_utils.call_function(acc['generator_fn'])
        accessors_list.extend(gen_fn(config))
      else:
        raise ValueError('invalid_accessor: ' + str(acc))
    self.accessors_list = accessors.sorted_accessor_list(accessors_list)
    def is_wildcard(accessor):
      return isinstance(accessor[1], protopath.ProtopathAccessor) and (
          '[*]' in accessor[1].protopath
      )
    self.wildcard_accessors = [x for x in self.accessors_list if is_wildcard(x)]
    self.accessors_list = [x for x in self.accessors_list if not is_wildcard(x)]
    self.accessor_names = sorted([a[0] for a in self.accessors_list])

    hdrs = spec['hdrs']
    if codegen_utils.is_function_call(hdrs):
      hdrs = codegen_utils.call_function(hdrs)
    self.hdrs = {cpp.Include(h) for h in hdrs}

    self.accessors_collections = [
        _AccessorsCollection(lst)
        for lst in accessors.split_accessors_into_shards(
            self.accessors_list, self.shard_count
        )
    ]


class AccessorGenerator:
  """Accessors code generator for cc and h file."""

  def __init__(
      self,
      build_target: str,
      loaders_spec: Dict[str, Any],
  ):
    self._build_target = build_target
    self._loaders_spec = [
        (cpp.CppName.from_full_name(loader_name), _LoaderSpec(spec))
        for loader_name, spec in sorted(loaders_spec.items())
    ]
    self._hdrs = set()
    for _, spec in self._loaders_spec:
      self._hdrs.update(spec.hdrs)

    self._max_shard_count = 0
    for _, loader in self._loaders_spec:
      loader.shard_count = min(
          loader.shard_count, len(loader.accessors_collections)
      )
      self._max_shard_count = max(self._max_shard_count, loader.shard_count)

  def max_shard_count(self) -> int:
    return self._max_shard_count

  def header_content(self) -> str:
    h_template = jinja_util.jinja_template('input_loader.h.jinja2')
    return h_template.render(
        header_guard=cpp.generate_header_guard(self._build_target),
        build_target=self._build_target,
        loaders_spec=self._loaders_spec,
        hdrs=sorted(self._hdrs),
    )

  def cpp_content(self, shard_id=0) -> str:
    """Returns content of c++ file."""
    cc_template = jinja_util.jinja_template('input_loader.cc.jinja2')
    hdrs = set(self._hdrs)
    is_main_file = shard_id == 0
    for _, loader in self._loaders_spec:
      if shard_id >= len(loader.accessors_collections):
        continue
      hdrs.update(loader.accessors_collections[shard_id].required_includes())
      if not is_main_file:
        continue  # wilcards are placed into the main file
      for _, a in loader.wildcard_accessors:
        hdrs.update(a.required_includes)
    return cc_template.render(
        build_target=self._build_target,
        loaders_spec=[
            (name, spec)
            for name, spec in self._loaders_spec
            if is_main_file or shard_id < spec.shard_count
        ],
        multi_protopath=multi_protopath,
        requested_shard_id=shard_id,
        hdrs=sorted(hdrs),
    )


class WildcardAccessorGenerator:
  """Wildcard accessors code generator for cc and h file."""

  def __init__(
      self,
      *,
      accessors_list: List[Tuple[str, accessors.Accessor]],
      loader_names: List[str],
      build_target: str,
      input_cls: str,
      extra_includes: List[cpp.Include],
  ):
    """Constructs generator.

    Args:
      accessors_list: list of tuples (name pattern with single %s, accessor),
        e.g., [("/map_int['%s']", accessor1), ("%s", accessor2)]
      loader_names: list of cpp loader names corresponding to accessors_list,
        e.g., ["::my_namespace::GetLoader", "::rl::GetMyLoader"]
      build_target: build target used for code generation. Used for comments and
        header guard generation.
      input_cls: fully qualified c++ name of the input class, e.g.,
        "::my_namespace::proto::MyFeatures".
      extra_includes: list of includes to add to the generated files.

    Raises:
      ValueError: on duplicated loader_names or different length of
          accessors_list and loader_names
    """
    if len(loader_names) != len(accessors_list):
      raise ValueError(
          'loader_names and accessors_list must have '
          f'the same length: {len(loader_names)} vs {len(accessors_list)}'
      )
    count_names = collections.Counter(loader_names)
    duplicated_names = [k for k, v in count_names.items() if v > 1]
    if duplicated_names:
      raise ValueError('Duplicate loader names: %s' % sorted(duplicated_names))

    loader_names, accessors_list = zip(
        *sorted(zip(loader_names, accessors_list))  # sort by loader_name
    )

    self._accessors = accessors_list
    self._loader_names = [
        cpp.CppName.from_full_name(loader_name) for loader_name in loader_names
    ]
    self._build_target = build_target
    self._input_cls = cpp.CppName.from_full_name(input_cls)
    self._extra_includes = set(extra_includes)

  def header_content(self) -> str:
    h_template = jinja_util.jinja_template('wildcard_input_loaders.h.jinja2')
    return h_template.render(
        header_guard=cpp.generate_header_guard(self._build_target),
        build_target=self._build_target,
        loader_names=self._loader_names,
        input_cls=self._input_cls,
        accessors=self._accessors,
        hdrs=sorted(self._extra_includes),
    )

  def cpp_content(self) -> str:
    """Returns content of c++ file."""
    cc_template = jinja_util.jinja_template('wildcard_input_loaders.cc.jinja2')
    hdrs = self._extra_includes
    for _, accessor in self._accessors:
      hdrs = hdrs.union(accessor.required_includes)
    return cc_template.render(
        build_target=self._build_target,
        loader_names=self._loader_names,
        accessors=self._accessors,
        input_cls=self._input_cls,
        hdrs=sorted(hdrs),
    )
