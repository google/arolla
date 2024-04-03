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

"""Utilities to process common flags."""

from typing import Callable, List, Tuple

from arolla.codegen import utils as codegen_utils
from arolla.codegen.io import accessor_generator
from arolla.codegen.io import accessors


class AccessorGeneratorParser:
  """Helper to parse accessors_generator."""

  def help(self) -> str:
    return """List of accessor generator functions as call_python_function."""

  def parse(
      self, accessor_str: str
  ) -> Callable[[accessor_generator.Config], List[accessors.Accessor]]:
    """Parse flag value into function generating list of accessors."""
    return codegen_utils.call_function_from_json(accessor_str)


class WildcardProtopathAccessorParser:
  """Helper to parse wildcard_protopath_accessors."""

  def help(self) -> str:
    return ('List of wildcard protopath accessors with loader names and name '
            'pattern in the format: `name~~path~~cpp_type~~loader_name`. '
            'loader_name is the name of the function to generate. '
            'name could be empty, default name for accessor will be used. '
            'cpp_type could be empty, auto type for accessor will be used. '
            'See wildcard_protopath_accessor in io.bzl for more details.')

  def parse(self, accessor: str) -> Tuple[str, str, str, str]:
    """Parse flag value into name, path, cpp_type and loader_name."""
    res = accessor.split('~~')
    if len(res) != 4:
      raise ValueError(
          f'Unable to parse wildcard_protopath_accessors {accessor}')
    return tuple(res)
