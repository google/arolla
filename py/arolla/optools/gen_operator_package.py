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

"""An utility function to dump operators."""

import importlib
import sys

from absl import app
from absl import flags
from arolla import arolla
from arolla.optools import clib

FLAGS = flags.FLAGS
flags.DEFINE_multi_string(
    'import_modules', [], 'Modules responsible for the operator declarations.'
)
flags.DEFINE_string(
    'output_file', None, 'Output file name for the operator package.'
)
flags.mark_flag_as_required('output_file')
flags.DEFINE_string('name', None, 'Module name.')
flags.mark_flag_as_required('name')


def import_module(module):
  """Imports the module by its filename."""
  module_original = module
  if module.endswith('/__init__.py'):
    module = module[: -len('/__init__.py')]
  elif module.endswith('.py'):
    module = module[: -len('.py')]
  module = module.replace('/', '.')
  print('importing module:', module_original, '->', module, file=sys.stderr)
  return importlib.import_module(module)


def main(argv):
  del argv
  n = len(arolla.abc.list_registered_operators())
  for module in FLAGS.import_modules:
    import_module(module)
  op_names = arolla.abc.list_registered_operators()[n:]
  print(f'operator package {FLAGS.name!r}:', file=sys.stderr)
  for op_name in op_names:
    print(' ', op_name, file=sys.stderr)
  data = clib.dumps_operator_package(op_names)
  print('  total byte size:', len(data), file=sys.stderr)
  open(FLAGS.output_file, 'wb').write(data)


if __name__ == '__main__':
  app.run(main)
