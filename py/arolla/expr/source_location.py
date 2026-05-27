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

"""Source location utilities for Arolla expressions."""

from arolla.abc import abc as arolla_abc


def strip_source_locations(expr: arolla_abc.Expr) -> arolla_abc.Expr:
  """Removes all `annotation.source_location` nodes from the expression."""

  source_location_op = arolla_abc.decay_registered_operator(
      'annotation.source_location'
  )

  def visitor(node):
    if (
        node.is_operator
        and arolla_abc.decay_registered_operator(node.op) == source_location_op
        and node.node_deps
    ):
      return node.node_deps[0]
    return node

  return arolla_abc.transform(expr, visitor)
