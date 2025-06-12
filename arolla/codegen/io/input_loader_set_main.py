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

"""Codegeneration for loading data from inputs.

Two files (`cc_out_file` and `h_out_file`) in `output_dir` will be created.
Header file will contain exactly one zero argument function with name
`loader_name` under corresponding namespace returning
`const ::arolla::InputLoader<{input_cls}>&`.

See description of flags for more details.
"""

import json
import os

from absl import app
from absl import flags

from arolla.codegen.io import input_loader_lib
import arolla.codegen.utils as codegen_utils

FLAGS = flags.FLAGS

flags.DEFINE_string(
    'build_target',
    None,
    help=('Build target identifier to specify in comments '
          'and to use as header guard.'))
flags.DEFINE_string(
    'loaders_spec',
    None,
    help='Spec for Python function that generates input loaders specification.')
flags.DEFINE_string('cc_out_file', None, help='Basename of the output cc file.')
flags.DEFINE_string('h_out_file', None, help='Basename of output h file.')
flags.DEFINE_string(
    'output_dir',
    None,
    help='Fullpath to the directory to put generated files.')
MAX_SHARD_COUNT = flags.DEFINE_integer(
    'max_shard_count',
    None,
    help='Maximum allowed shard count in the loaders_spec.',
)
GENERATE_OPERATORS_SET = flags.DEFINE_boolean(
    'generate_operators_set',
    False,
    help='If true, generate operators set instead of InputLoader.',
)


def main(argv):
  if len(argv) > 1:
    raise app.UsageError('Too many command-line arguments.')

  loaders_spec = json.loads(FLAGS.loaders_spec)
  if codegen_utils.is_function_call(loaders_spec):
    loaders_spec = codegen_utils.call_function(loaders_spec)

  generator = input_loader_lib.AccessorGenerator(
      FLAGS.build_target, loaders_spec
  )
  actual_shard_count = generator.max_shard_count()
  if actual_shard_count > max(MAX_SHARD_COUNT.value, 1):
    raise ValueError(
        f'input_loader_set(max_shard_count={MAX_SHARD_COUNT.value}) must be'
        ' larger than maximum shard_count in the specifications:'
        f' specified:{MAX_SHARD_COUNT.value} <'
        f' needed:{actual_shard_count}'
    )

  with open(os.path.join(FLAGS.output_dir, FLAGS.h_out_file), 'w') as f:
    header = (
        generator.operators_header_content()
        if GENERATE_OPERATORS_SET.value
        else generator.header_content()
    )
    f.write(header)

  relative_dir = FLAGS.build_target[2:FLAGS.build_target.find(':')]
  relative_hdr = f'{relative_dir}/{FLAGS.h_out_file}'

  for shard_id in range(max(MAX_SHARD_COUNT.value or 1, actual_shard_count)):
    base_name = FLAGS.cc_out_file
    if shard_id > 0:
      base_name = FLAGS.cc_out_file[:-3] + '_' + str(shard_id) + '.cc'
    with open(os.path.join(FLAGS.output_dir, base_name), 'w') as f:
      if shard_id >= actual_shard_count:
        continue
      cpp = (
          generator.operators_cpp_content(shard_id, relative_hdr)
          if GENERATE_OPERATORS_SET.value
          else generator.cpp_loader_content(shard_id)
      )
      f.write(cpp)


if __name__ == '__main__':
  app.run(main)
