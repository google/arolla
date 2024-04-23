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

from arolla.codegen.io import accessor_generator
from arolla.codegen.io import flag_utils
from arolla.codegen.io import input_loader_lib
import arolla.codegen.utils as codegen_utils

FLAGS = flags.FLAGS

flags.DEFINE_multi_string(
    'accessor_generator', [], help=flag_utils.AccessorGeneratorParser().help())
flags.DEFINE_string(
    'array_type',
    '',
    help='Type of the array to use. Supported: DenseArray or empty string.')
flags.DEFINE_string(
    'sharding_info',
    '',
    help='JSON encoded dict with sharding information. Supported: shard_count')
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
    'input_cls', None, help='Fully qualified typename of the input.')
flags.DEFINE_string(
    'input_cls_getter',
    None,
    help='Spec for Python function that generates input_cls.')
flags.DEFINE_string(
    'loader_name',
    None,
    help=('Fully qualified name of the loader. '
          'C++ function with this name will be generated under'
          'namespace taken from the name. E.g. ::contrib::x::GetMyLoader.'))
flags.DEFINE_string('cc_out_file', None, help='Basename of the output cc file.')
flags.DEFINE_string('h_out_file', None, help='Basename of output h file.')
flags.DEFINE_string(
    'output_dir',
    None,
    help='Fullpath to the directory to put generated files.')


def main(argv):
  if len(argv) > 1:
    raise app.UsageError('Too many command-line arguments.')

  input_cls = (
      codegen_utils.call_function_from_json(FLAGS.input_cls_getter)
      if FLAGS.input_cls_getter else FLAGS.input_cls)
  config = accessor_generator.Config(
      io_cpp_type=input_cls, array_type=FLAGS.array_type, is_mutable=False)

  accessors_list = []
  for accessor_str in FLAGS.accessor_generator:
    gen_fn = flag_utils.AccessorGeneratorParser().parse(accessor_str)
    accessors_list.extend(gen_fn(config))

  hdrs = list(
      set(FLAGS.hdrs).union(*(
          codegen_utils.call_function_from_json(g) for g in FLAGS.hdr_getters)))

  sharding = json.loads(FLAGS.sharding_info)
  generator = input_loader_lib.AccessorGenerator(
      FLAGS.build_target,
      {
          FLAGS.loader_name: {
              'input_cls': input_cls,
              'accessors': accessors_list,
              'sharding': sharding,
              'array_type': FLAGS.array_type,
              'hdrs': hdrs,
          }
      },
  )
  actual_shard_count = generator.max_shard_count()

  with open(os.path.join(FLAGS.output_dir, FLAGS.h_out_file), 'w') as f:
    f.write(generator.header_content())

  with open(os.path.join(FLAGS.output_dir, FLAGS.cc_out_file), 'w') as f:
    f.write(generator.cpp_content(0))

  for i in range(1, sharding['shard_count']):
    fname = os.path.join(
        FLAGS.output_dir, FLAGS.cc_out_file[:-3] + '_' + str(i) + '.cc'
    )
    with open(fname, 'w') as f:
      if i < actual_shard_count:
        f.write(generator.cpp_content(i))


if __name__ == '__main__':
  app.run(main)
