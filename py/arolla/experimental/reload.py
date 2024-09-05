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

"""Experimental module that reloads of all arolla python modules."""

import importlib
import sys


def reload():
  """Reloads arolla.arolla module."""
  module_type = type(importlib)
  arolla_module = sys.modules.get('arolla.arolla')
  if arolla_module is None:
    raise RuntimeError('No arolla.arolla module loaded.')
  visited_modules = set()
  reloaded_modules = set()

  def reload_impl(module):
    if module in reloaded_modules:
      return
    if module in visited_modules:
      raise RuntimeError('Cyclic dependency at:', module.__name__)
    visited_modules.add(module)
    for dependency in module.__dict__.values():
      # Skip non-modules.
      if not isinstance(dependency, module_type):
        continue
      # Skip non-arolla modules.
      if not dependency.__name__.startswith('arolla.'):
        continue
      # Avoid reloading modules that don't match with sys.modules records,
      # reloading of such modules trigger ImportError.
      # NOTE: The cases where it was important were related to CLIF and
      # StaticMetaImporter.
      if sys.modules.get(dependency.__name__) is not dependency:
        continue
      reload_impl(dependency)
    importlib.reload(module)
    reloaded_modules.add(module)

  reload_impl(arolla_module)
