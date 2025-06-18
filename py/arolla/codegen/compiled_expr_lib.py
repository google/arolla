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

"""Utility functions for code generation of operator."""

import os

from absl import flags
from arolla import arolla
from arolla.codegen import clib
import jinja2

from google.protobuf import text_format
from arolla.proto import io_pb2

flags.DEFINE_multi_string(
    'cpp_function_name',
    [],
    help=(
        'Fully qualified name of the generated function. '
        'C++ function with this name will be generated under'
        'namespace taken from the name. E.g. ::contrib::x::GetCompiledExpr.'
    ),
)
flags.DEFINE_multi_string(
    'expr_fn',
    [],
    help=(
        'Json-encoded call_python_function specification returning arolla.Expr.'
    ),
)
flags.DEFINE_string(
    'build_target',
    None,
    help=(
        'Build target identifier to specify in comments and to use as header'
        ' guard.'
    ),
)
flags.DEFINE_bool(
    'inputs_are_cheap_to_read',
    True,
    help=(
        'If true inputs considered to be stored in a global context (e.g.,'
        ' Frame). If false inputs are considered to be expensive to compute and'
        " shouldn't be reevaluated many times."
    ),
)
flags.DEFINE_string(
    'cc_out_file', None, help='Basename of the main output cc file.'
)
flags.DEFINE_string('h_out_file', None, help='Basename of output h file.')
flags.DEFINE_string('deps_out_file', None, help='Basename of file with deps.')
flags.DEFINE_string(
    'io_out_file', None, help='Basename of file with ModelInputOutputInfo.'
)
flags.DEFINE_string(
    'output_dir', None, help='Fullpath to the directory to put generated files.'
)
flags.DEFINE_integer(
    'recursion_limit',
    # jinja template recursion generates too big depth (~5 entries per call)
    5000,
    help='Python recursion limit.',
)
flags.DEFINE_integer(
    'shard_count',
    1,
    help='Number of shards to split the generated code into.',
)
flags.DEFINE_boolean(
    'export_named_outputs',
    False,
    help=(
        'If true, generates code for exporting named outputs for nodes'
        ' annotated with `annotation.export`.'
    ),
)
flags.DEFINE_boolean(
    'use_iwyu_private',
    False,
    help='Whether to add "// IWYU pragma: private" to the generated header.',
)


def jinja_templates():
  """Returns header and cc templates."""
  env = jinja2.Environment(
      # must be located in library since *_main file is copied to another dir.
      loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
      undefined=jinja2.StrictUndefined,
      extensions=[
          'jinja2.ext.loopcontrols',
          'jinja2.ext.do',
      ],
  )
  return (
      env.get_template('compiled_expr.h.jinja2'),
      env.get_template('compiled_expr.cc.jinja2'),
  )


_annotation_export_ops = {
    arolla.abc.decay_registered_operator('annotation.export'),
    arolla.abc.decay_registered_operator('annotation.export_value'),
}


def strip_export_annotations(expr):
  """Remove annotation.export from the entire expression."""

  def strip(node):
    op = node.op
    if (
        op
        and arolla.abc.decay_registered_operator(op) in _annotation_export_ops
    ):
      return node.node_deps[0]
    return node

  return arolla.abc.transform(expr, strip)


def filter_inlinable_assignments(
    assignment_ids: list[int], assignments
) -> list[int]:
  """Returns new list of non inlinable assignemt ids."""
  return [idx for idx in assignment_ids if not assignments[idx].is_inlinable]


def has_inlinable_status_arg(assignment_id: int, assignments) -> bool:
  """Returns True iff assignment_id has inlinable status_or dependency."""
  for dep_id in assignments[assignment_id].rvalue.argument_ids:
    if (
        assignments[dep_id].is_inlinable
        and assignments[dep_id].lvalue.is_local_expr_status_or
    ):
      return True
  return False


def gen_model_input_output_info(
    operator_datas: list[clib.OperatorCodegenData],
    build_target: str,
) -> str:
  """Returns textproto representation ModelInputOutputInfo."""
  info = io_pb2.ModelInputOutputInfo()
  inputs = set.union(
      *(set(operator_data.inputs.keys()) for operator_data in operator_datas)
  )
  for input_name in sorted(inputs):
    info.inputs.add(name=input_name)
  outputs = set.union(*(
      set(k for k, _ in operator_data.side_outputs)
      for operator_data in operator_datas
  ))
  for output_name in sorted(outputs):
    info.side_outputs.add(name=output_name)
  header = [
      '# proto-file: arolla/proto/io.proto\n',
      '# proto-message: ModelInputOutputInfo\n',
      '# THIS FILE IS AUTOGENERATED. DO NOT EDIT.\n',
      f'# Build target: {build_target}\n',
  ]
  return ''.join(header) + text_format.MessageToString(info)
