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

"""Evaluate given `call_python_function` that suppose to produce file."""

from absl import app
from absl import flags

from arolla.codegen import utils

FLAGS = flags.FLAGS
_CALL_PYTHON_FUNCTION_SPEC = flags.DEFINE_string(
    'arolla_call_python_function_spec',
    None,
    help='json-encoded call_python_function specification.',
)


def main(argv: list[str]) -> None:
  if len(argv) > 1:
    raise app.UsageError('Too many command-line arguments.')
  utils.call_function_from_json(_CALL_PYTHON_FUNCTION_SPEC.value)


if __name__ == '__main__':
  app.run(main)
