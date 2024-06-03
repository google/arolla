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

"""Utility functions for code working with jinja."""

import os
import jinja2

from arolla.codegen import utils


def maybe_wrap_with_lambda(text: str, do_wrap: bool) -> str:
  """If requested wrap into always inline no arg C++ lambda."""
  text = text.lstrip('\n')
  if not do_wrap:
    return text
  indent_line = lambda line: '  ' + line if line else line
  indented_text = '\n'.join(indent_line(line) for line in text.split('\n'))
  return f'[&]() {{\n{indented_text}\n}}();'


_env = jinja2.Environment(
    # must be located in library since *_main files is copied to another dir.
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
    undefined=jinja2.StrictUndefined,
    extensions=['jinja2.ext.loopcontrols'],
)

_env.filters['maybe_wrap_with_lambda'] = maybe_wrap_with_lambda
_env.filters['remove_common_prefix_with_previous_string'] = (
    utils.remove_common_prefix_with_previous_string)

_env.globals['zip'] = zip


def jinja_template(name: str):
  """Returns jinja template located in the same directory as this file."""
  return _env.get_template(name)
