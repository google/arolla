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

"""Fork of c++ arolla/naming library.

TODO: move and compile python bindings for original library.
"""

import os


def FieldAccess(field_name: str) -> str:
  return field_name


def MapAccess(field_name: str, key: str) -> str:
  return f'{field_name}["{key}"]'


def ArrayAccess(field_name: str, idx: int) -> str:
  return f'{field_name}[{idx}]'


def ProtoExtensionAccess(ext_name: str) -> str:
  """Access to proto extension by fully qualified extension field name."""
  return f'Ext::{ext_name}'


class ColumnPath:

  def __init__(self, name: str):
    self.name = name

  def __repr__(self) -> str:
    return self.name

  def __eq__(self, other) -> bool:
    return str(self) == str(other)


class TablePath:
  """Class incapsulating naming used for the Table."""

  def __init__(self, name: str = ''):
    self.name = os.path.join('/', name)

  def Child(self, name: str) -> 'TablePath':
    return TablePath(os.path.join(self.name, name))

  def Column(self, name: str) -> ColumnPath:
    return ColumnPath(os.path.join(self.name, name))

  def Size(self, name: str) -> ColumnPath:
    return ColumnPath(os.path.join(self.name, name, '@size'))

  def MapKeys(self) -> 'TablePath':
    return self.Child('@key')

  def MapValues(self) -> 'TablePath':
    return self.Child('@value')

  def __eq__(self, other: 'TablePath') -> bool:
    return str(self) == str(other)

  def __repr__(self) -> str:
    return self.name
