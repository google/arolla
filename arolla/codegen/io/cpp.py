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

"""Common utility classes and functions for generating c++ code."""


class Include:
  """Represents C++ include."""

  def __init__(self, header: str):
    """Constructs from header. <> used for std and nothing or "" for others."""
    if (header.startswith('<') and
        header.endswith('>')) or (header.startswith('"') and
                                  header.endswith('"')):
      self._include = header
    else:
      self._include = '"%s"' % header

  def __str__(self):
    return self.include_str

  def __repr__(self):
    return 'Include(' + self.include_str + ')'

  @property
  def include_str(self) -> str:
    return '#include %s' % self._include

  def __lt__(self, other: 'Include') -> bool:
    return self.include_str < other.include_str

  def __eq__(self, other: 'Include') -> bool:
    return self.include_str == other.include_str

  def __hash__(self) -> int:
    return hash(self._include)


def generate_header_guard(build_target: str) -> str:
  """Returns header ifdef guard based on build target."""
  return build_target[2:].replace('/', '_').replace(':', '_').upper() + '_H_'


class CppName:
  """Represents fully qualified c++ name."""

  def __init__(self, namespace: str, name: str):
    """Constructs from fully qualified namespace and name."""
    namespaces_list = namespace.split('::')
    if namespaces_list and not namespaces_list[0]:
      namespaces_list = namespaces_list[1:]
    self._namespace = '::'.join(namespaces_list)

    if ':' in name:
      raise ValueError('name must have no namespaces')
    self._name = name

  @staticmethod
  def from_full_name(full_name: str) -> 'CppName':
    """Constructs from fully qualified name. Starting :: could be omitted."""
    parts = full_name.rsplit('::', 1)
    if len(parts) == 1:
      parts = [''] + parts
    return CppName(*parts)

  @property
  def namespace(self) -> str:
    """Returns namespace without leading :: or empty string."""
    return self._namespace

  @property
  def name(self) -> str:
    """Returns name without namespace."""
    return self._name

  @property
  def full_name(self) -> str:
    """Returns fully qualified name with leading :: or name if no namespace."""
    prefix = '::' + self._namespace + '::' if self._namespace else ''
    return prefix + self._name

  @property
  def open_namespace_str(self) -> str:
    """Returns string with opening namespace or empty string."""
    return 'namespace %s {' % self.namespace if self.namespace else ''

  @property
  def close_namespace_str(self) -> str:
    """Returns string with closing namespace or empty string."""
    return '}  // namespace %s' % self.namespace if self.namespace else ''
