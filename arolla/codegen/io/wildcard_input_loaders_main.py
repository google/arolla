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

import os

from absl import app
from absl import flags

from arolla.codegen.io import accessor_generator
from arolla.codegen.io import cpp
from arolla.codegen.io import flag_utils
from arolla.codegen.io import input_loader_lib

_ACCESSOR_GENERATOR = flags.DEFINE_multi_string(
    'accessor_generator', [], help=flag_utils.AccessorGeneratorParser().help())
_LOADER_NAME = flags.DEFINE_multi_string(
    'loader_name', [],
    help='List of fully qualified C++ name for each accessor generator. ' +
    'Must have the same length as --accessor_generator.')
_ARRAY_TYPE = flags.DEFINE_string(
    'array_type',
    'DenseArray',
    help='Type of the array to use. Supported: DenseArray or empty string.')
_HDRS = flags.DEFINE_multi_string(
    'hdrs', [], help='List of c++ headers to include into the generated code.')

_BUILD_TARGET = flags.DEFINE_string(
    'build_target',
    None,
    help=('Build target identifier to specify in comments '
          'and to use as header guard.'))
_INPUT_CLS = flags.DEFINE_string(
    'input_cls', None, help='Fully qualified typename of the input.')
_CC_OUT_FILE = flags.DEFINE_string(
    'cc_out_file', None, help='Basename of the output cc file.')
_H_OUT_FILE = flags.DEFINE_string(
    'h_out_file', None, help='Basename of output h file.')
_OUTPUT_DIR = flags.DEFINE_string(
    'output_dir',
    None,
    help='Fullpath to the directory to put generated files.')


def main(argv):
  if len(argv) > 1:
    raise app.UsageError('Too many command-line arguments.')

  accessors_list = []

  config = accessor_generator.Config(
      io_cpp_type=_INPUT_CLS.value,
      array_type=_ARRAY_TYPE.value,
      is_mutable=False)

  if len(_ACCESSOR_GENERATOR.value) != len(_LOADER_NAME.value):
    raise ValueError(
        f'Size mismatch --accessor_generator={len(_ACCESSOR_GENERATOR.value)} '
        + f'vs --loader_name={len(_LOADER_NAME.value)}')

  for accessor_str in _ACCESSOR_GENERATOR.value:
    gen_fn = flag_utils.AccessorGeneratorParser().parse(accessor_str)
    accessors_list.extend(gen_fn(config))

  generator = input_loader_lib.WildcardAccessorGenerator(
      accessors_list=accessors_list,
      loader_names=_LOADER_NAME.value,
      build_target=_BUILD_TARGET.value,
      input_cls=_INPUT_CLS.value,
      extra_includes={cpp.Include(h) for h in _HDRS.value})

  with open(os.path.join(_OUTPUT_DIR.value, _H_OUT_FILE.value), 'w') as f:
    f.write(generator.header_content())

  with open(os.path.join(_OUTPUT_DIR.value, _CC_OUT_FILE.value), 'w') as f:
    f.write(generator.cpp_content())


if __name__ == '__main__':
  app.run(main)
