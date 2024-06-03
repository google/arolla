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

"""Common utils for code generation."""

import importlib
import json
import os
from typing import Any, Callable, Dict, Iterable, Union


_CALL_FUNCTION_MAGIC_HEADER = '__call_python_func__'


def read_file(path: str) -> bytes:
  """Returns contents of the given file."""
  with open(path, 'rb') as f:
    return f.read()


def write_file(path: str, content: Union[str, bytes]):
  """Writes content to the given file."""
  if isinstance(content, str):
    content = content.encode('utf-8')
  with open(path, 'wb') as f:
    f.write(content)


def is_function_call(func_spec: Dict[str, Any]):
  """Returns True if func_spec is created by call_python_function bzl macro."""
  return isinstance(func_spec,
                    dict) and _CALL_FUNCTION_MAGIC_HEADER in func_spec


def load_function(function_path: str) -> Callable[..., Any]:
  module_name, fn_name = function_path.rsplit('.', maxsplit=1)
  try:
    module = importlib.import_module(module_name)
  except ModuleNotFoundError as e:
    raise ImportError(f'Couldn\'t load function: {function_path!r}') from e
  fn = getattr(module, fn_name)
  if not callable(fn):
    raise TypeError(
        f'The object specified by {function_path!r} should be a callable, but '
        f'was actually a {type(fn)}.')
  return fn


def call_function(func_spec: Dict[str, Any]):
  """Executes a function from the given spec.

  Args:
    func_spec: dict created by call_python_function bzl macro.

  Returns:
    result of the function.
  """
  assert is_function_call(func_spec)
  fn = load_function(func_spec['fn'])
  args = [
      process_call_function_requests(arg)
      for arg in func_spec.get('args', [])
  ]
  kwargs = {
      name: process_call_function_requests(arg)
      for name, arg in func_spec.get('kwargs', {}).items()
  }
  return fn(*args, **kwargs)


def call_function_from_json(func_spec: str):
  return call_function(json.loads(func_spec))


def process_call_function_requests(context):
  """Recursively processes all call_python_function requests in the context.

  Args:
    context: dict/list, generally parsed from json via a build rule. It may
      contain one or several call_python_function bzl macro invocations.

  Returns:
    The same context, but with all call_python_function bzl macro invocations
    replaces with the function results.
  """
  if isinstance(context, dict):
    if is_function_call(context):
      return call_function(context)
    else:
      return {k: process_call_function_requests(v) for k, v in context.items()}
  elif isinstance(context, list):
    return [process_call_function_requests(v) for v in context]
  else:
    return context


def merge_dicts(*args: dict[str, Any]) -> dict[str, Any]:
  """Merges dicts to a single dict. Duplicate keys are overwritten."""
  merged = {}
  for d in args:
    merged.update(d)
  return merged


def remove_common_prefix_with_previous_string(
    str_list: Iterable[str],
) -> list[str]:
  """Removes common prefix with previous string from each string in the list."""
  str_list = list(str_list)
  if len(str_list) < 2:
    return str_list
  previous = str_list[0]
  result = [previous]
  for s in str_list[1:]:
    prefix = os.path.commonprefix([previous, s])
    result.append(s[len(prefix):])
    previous = s
  return result
