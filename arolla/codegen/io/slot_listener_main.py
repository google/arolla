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

"""Codegeneration for ...."""

import json
import os
from typing import Any, Dict, List, Tuple

from absl import app
from absl import flags

from arolla.codegen.io import accessor_generator
from arolla.codegen.io import accessors
from arolla.codegen.io import cpp
from arolla.codegen.io import flag_utils
from arolla.codegen.io import jinja_util
from arolla.codegen.io import multi_protopath
import arolla.codegen.utils as codegen_utils


FLAGS = flags.FLAGS

flags.DEFINE_multi_string(
    'accessor_generator', [], help=flag_utils.AccessorGeneratorParser().help())
flags.DEFINE_string(
    'array_type',
    '',
    help='Type of the array to use. Supported: DenseArray or empty string.')
flags.DEFINE_multi_string(
    'hdrs', [], help='List of c++ headers to include into the generated code.')
flags.DEFINE_multi_string(
    'hdr_getters', [], help='Spec for Python function that generates hdrs.')

flags.DEFINE_string(
    'build_target',
    None,
    help=('Build target identifier to specify in comments '
          'and to use as header guard.'))
flags.DEFINE_string(
    'output_cls', None, help='Fully qualified typename of the output.')
flags.DEFINE_string(
    'output_cls_getter',
    None,
    help='Spec for Python function that generates '
    'output_cls.')
flags.DEFINE_string(
    'slot_listener_name',
    None,
    help=('Fully qualified name of the listener. '
          'C++ function with this name will be generated under'
          'namespace taken from the name. E.g. ::contrib::x::GetMyListener.'))
flags.DEFINE_string(
    'cc_out_file',
    None,
    help='Basename of the main output cc file. '
    'Extra files will be generated if cpp_extra_file_count > 0.')
flags.DEFINE_string('h_out_file', None, help='Basename of output h file.')
flags.DEFINE_string(
    'output_dir',
    None,
    help='Fullpath to the directory to put generated files.')
flags.DEFINE_list(
    'cc_extra_out_files', [],
    help='Additional c++ files to shard compilation. '
    'Visible effect can be achieved if two or more files passed.')
flags.DEFINE_string(
    'listeners_spec',
    None,
    help=(
        'Spec for Python function that generates slot listeners specification.'
    ),
)
SHARDING_INFO = flags.DEFINE_string(
    'sharding_info',
    '',
    help='JSON encoded dict with sharding information. Supported: shard_count.',
)
MAX_SHARD_COUNT = flags.DEFINE_integer(
    'max_shard_count',
    None,
    help='Maximum allowed shard count in the listeners_spec.',
)


class _AccessorsCollection:
  """Collection of accessors in different forms."""

  def __init__(self, accessors_list: List[Tuple[str, accessors.Accessor]]):
    self.accessors_list, self.single_value_protopath_tree = (
        multi_protopath.extract_single_value_protopath_accessors(accessors_list)
    )


class _ListenerSpec:
  """Specification of a slot listener."""

  def __init__(self, spec: Dict[str, Any]):
    output_cls_name = spec['output_cls']
    if codegen_utils.is_function_call(output_cls_name):
      output_cls_name = codegen_utils.call_function(output_cls_name)
    self.output_cls = cpp.CppName.from_full_name(output_cls_name)

    self.array_type = (
        spec['array_type'] if 'array_type' in spec else 'DenseArray'
    )

    accessors_list = []
    config = accessor_generator.Config(
        io_cpp_type=output_cls_name, array_type=self.array_type, is_mutable=True
    )
    for acc in spec['accessors']:
      if isinstance(acc, tuple):
        accessors_list.append(acc)
      elif codegen_utils.is_function_call(acc):
        gen_fn = codegen_utils.call_function(acc)
        accessors_list.extend(gen_fn(config))
      elif isinstance(acc, dict):
        gen_fn = codegen_utils.call_function(acc['generator_fn'])
        accessors_list.extend(gen_fn(config))
      else:
        raise ValueError('invalid_accessor: ' + str(acc))
    accessors_list = accessors.sorted_accessor_list(accessors_list)

    # we extract multi value accessors from accessors_list, process, and then
    # add back to the end of accessors_list.
    accessors_list, multi_value_protopath_tree = (
        multi_protopath.extract_multi_value_protopath_accessors(accessors_list)
    )
    accessors_list = accessors.AccessorsList(accessors_list)
    accessor_names = list(accessors_list.names)
    for leaf in multi_value_protopath_tree.leaves():
      accessor_names.append(leaf.leaf_accessor_name)
    self.accessor_names = sorted(accessor_names)

    accessors_list = list(zip(accessors_list.names, accessors_list.accessors))
    # we sort accessors by "real" depth to make sure that `count(field[:])`
    # accessors are always precede `field[:]/...` accessors.
    sorted_multi_value_leaves = sorted(
        multi_value_protopath_tree.leaves(),
        key=lambda l: (l.depth_without_fictive(), l.leaf_accessor_name),
    )
    accessors_list += [
        (leaf.leaf_accessor_name, leaf.leaf_accessor)
        for leaf in sorted_multi_value_leaves
    ]

    hdrs = spec['hdrs']
    if codegen_utils.is_function_call(hdrs):
      hdrs = codegen_utils.call_function(hdrs)
    self.hdrs = {cpp.Include(h) for h in hdrs}
    self.hdrs = sorted(
        {cpp.Include(h) for h in hdrs}
        | multi_value_protopath_tree.required_includes
    )

    self.shard_count = (
        int(spec['sharding'].get('shard_count', 0)) if 'sharding' in spec else 0
    )
    self.accessors_collections = [
        _AccessorsCollection(lst)
        for lst in accessors.split_accessors_into_shards(
            accessors_list, self.shard_count
        )
    ]


def get_listeners_spec():
  """Processes flags and returnes listeners spec dict."""
  if FLAGS.listeners_spec is not None:
    listeners_spec = json.loads(FLAGS.listeners_spec)
    if codegen_utils.is_function_call(listeners_spec):
      listeners_spec = codegen_utils.call_function(listeners_spec)
    return listeners_spec

  output_cls = (
      codegen_utils.call_function_from_json(FLAGS.output_cls_getter)
      if FLAGS.output_cls_getter else FLAGS.output_cls)
  headers = set(FLAGS.hdrs).union(
      *[codegen_utils.call_function_from_json(g) for g in FLAGS.hdr_getters])

  return {
      FLAGS.slot_listener_name: {
          'output_cls': output_cls,
          'accessors': [json.loads(x) for x in FLAGS.accessor_generator],
          'array_type': FLAGS.array_type,
          'sharding': json.loads(SHARDING_INFO.value),
          'hdrs': headers,
      }
  }


class AccessorGenerator:
  """Accessors code generator for cc and h file."""

  def __init__(
      self,
      build_target: str,
      listeners_spec: Dict[str, Any],
  ):
    self._build_target = build_target
    self._listeners_spec = [
        (cpp.CppName.from_full_name(loader_name), _ListenerSpec(spec))
        for loader_name, spec in sorted(listeners_spec.items())
    ]
    self._hdrs = set()
    for _, spec in self._listeners_spec:
      self._hdrs.update(spec.hdrs)

    self._max_shard_count = 0
    for _, listener in self._listeners_spec:
      listener.shard_count = min(
          listener.shard_count, len(listener.accessors_collections)
      )
      self._max_shard_count = max(self._max_shard_count, listener.shard_count)

  def max_shard_count(self) -> int:
    return self._max_shard_count

  def header_content(self) -> str:
    header_template = jinja_util.jinja_template('slot_listener.h.jinja2')
    return header_template.render(
        build_target=self._build_target,
        header_guard=cpp.generate_header_guard(FLAGS.build_target),
        hdrs=sorted(self._hdrs),
        listeners_spec=self._listeners_spec,
    )

  def cpp_content(self, shard_id=0) -> str:
    """Returns content of c++ file for given shard."""
    cc_template = jinja_util.jinja_template('slot_listener.cc.jinja2')
    is_main_file = shard_id == 0
    hdrs = set(self._hdrs)
    for _, listener in self._listeners_spec:
      if shard_id >= len(listener.accessors_collections):
        continue
      hdrs.update(
          listener.accessors_collections[
              shard_id
          ].single_value_protopath_tree.required_includes
      )
    return cc_template.render(
        build_target=self._build_target,
        listeners_spec=[
            (name, spec)
            for name, spec in self._listeners_spec
            if is_main_file or shard_id < spec.shard_count
        ],
        multi_protopath=multi_protopath,
        requested_shard_id=shard_id,
        hdrs=sorted(hdrs),
    )


def main(argv):
  if len(argv) > 1:
    raise app.UsageError('Too many command-line arguments.')

  generator = AccessorGenerator(FLAGS.build_target, get_listeners_spec())
  actual_shard_count = generator.max_shard_count()
  if actual_shard_count > max(MAX_SHARD_COUNT.value or actual_shard_count, 1):
    raise ValueError(
        f'slot_listener_set(max_shard_count={MAX_SHARD_COUNT.value}) must be'
        ' larger than maximum shard_count in the specifications:'
        f' specified:{MAX_SHARD_COUNT.value} <'
        f' needed:{actual_shard_count}'
    )

  with open(os.path.join(FLAGS.output_dir, FLAGS.h_out_file), 'w') as f:
    f.write(generator.header_content())

  with open(os.path.join(FLAGS.output_dir, FLAGS.cc_out_file), 'w') as f:
    f.write(generator.cpp_content(0))

  for i in range(1, MAX_SHARD_COUNT.value or actual_shard_count):
    fname = os.path.join(
        FLAGS.output_dir, FLAGS.cc_out_file[:-3] + '_' + str(i) + '.cc'
    )
    with open(fname, 'w') as f:
      if i < actual_shard_count:
        f.write(generator.cpp_content(i))


if __name__ == '__main__':
  app.run(main)
