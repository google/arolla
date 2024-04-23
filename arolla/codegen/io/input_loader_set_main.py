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
    f.write(generator.header_content())

  with open(os.path.join(FLAGS.output_dir, FLAGS.cc_out_file), 'w') as f:
    f.write(generator.cpp_content())

  for i in range(1, MAX_SHARD_COUNT.value):
    fname = os.path.join(
        FLAGS.output_dir, FLAGS.cc_out_file[:-3] + '_' + str(i) + '.cc'
    )
    with open(fname, 'w') as f:
      if i < actual_shard_count:
        f.write(generator.cpp_content(i))


if __name__ == '__main__':
  app.run(main)
