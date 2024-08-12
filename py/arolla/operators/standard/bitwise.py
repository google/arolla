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

"""Declaration of M.bitwise.* operators."""

from arolla import arolla
from arolla.operators.standard import qtype as M_qtype

P = arolla.P
constraints = arolla.optools.constraints


_like_unary_bitwise = {
    'qtype_constraints': [constraints.expect_integers(P.x)],
    'qtype_inference_expr': P.x,
}

_like_binary_bitwise = {
    'qtype_constraints': [
        constraints.expect_integers(P.x),
        constraints.expect_integers(P.y),
        constraints.expect_implicit_cast_compatible(P.x, P.y),
    ],
    'qtype_inference_expr': M_qtype.common_qtype(P.x, P.y),
}


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'bitwise.bitwise_and', **_like_binary_bitwise
)
def bitwise_and(x, y):
  """Computes bitwise AND of the given inputs."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'bitwise.bitwise_or', **_like_binary_bitwise
)
def bitwise_or(x, y):
  """Computes bitwise OR of the given inputs."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'bitwise.bitwise_xor', **_like_binary_bitwise
)
def bitwise_xor(x, y):
  """Computes bitwise XOR of the given inputs."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('bitwise.invert', **_like_unary_bitwise)
def invert(x):
  """Computes bitwise NOT of the given input."""
  raise NotImplementedError('provided by backend')
