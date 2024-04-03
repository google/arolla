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

"""Render Jinja template with provided context dictionary."""

import os.path
from typing import Any, Mapping

import jinja2

import arolla.codegen.utils as codegen_utils


def _cescape(data):
  if isinstance(data, str):
    data = data.encode('utf8')
  return ''.join(f'\\x{byte:02x}' for byte in data)


def _as_c_identifier(name: str) -> str:
  """Generates a C++ identifier name that looks similarly to the argument."""
  only_alnums = ''.join(c if c.isalnum() else ' ' for c in name)
  identifier_name = '_'.join(only_alnums.split())

  if not identifier_name or identifier_name[0].isdigit():
    raise ValueError(f'unable to generate C++ identifier name from "{name}"')

  return identifier_name


def render_jinja2_template(template: str, context: Mapping[str, Any],
                           output: str):
  """Loads & renders and saves jinja2 template."""
  env = jinja2.Environment(
      loader=jinja2.FileSystemLoader('.'), undefined=jinja2.StrictUndefined,
      extensions=['jinja2.ext.loopcontrols'])
  env.filters['cescape'] = _cescape
  env.filters['as_c_identifier'] = _as_c_identifier
  env.filters['basename'] = os.path.basename
  template = env.get_template(template)
  context = codegen_utils.process_call_function_requests(context)
  rendered = template.render(context)
  with open(output, 'w') as f:
    f.write(rendered)
