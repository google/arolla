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

"""Public python API for jagged shapes."""

from arolla.jagged_shape import types as _types


def get_namespaces() -> list[str]:
  """Returns the namespaces for the jagged operators."""
  return ['jagged']


# QType
# go/keep-sorted start block=yes
JAGGED_ARRAY_SHAPE = _types.JAGGED_ARRAY_SHAPE
JAGGED_DENSE_ARRAY_SHAPE = _types.JAGGED_DENSE_ARRAY_SHAPE
is_jagged_shape_qtype = _types.is_jagged_shape_qtype
# go/keep-sorted end


# QValue
# go/keep-sorted start block=yes
JaggedArrayShape = _types.JaggedArrayShape
JaggedDenseArrayShape = _types.JaggedDenseArrayShape
# go/keep-sorted end
