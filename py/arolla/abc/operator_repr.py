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

"""Customized operator representations."""

from arolla.abc import clib

# Repr with precedence.
#
# class ReprToken:
#
#   Methods defined here:
#
#     __init__(self)
#       Constructs an instance of the class.
#
#   Data descriptors defined here:
#
#     text
#       (str) repr-string.
#
#     precedence:
#       left- and right-precedence. Describes how tightly the left and right
#       parts of the string are "bound" with the middle.
#
#   Readonly properties defined here:
#
#     PRECEDENCE_OP_SUBSCRIPTION
#       subscription operator representation precedence: `x.y`, `x[y]`, etc.
#
ReprToken = clib.ReprToken


# Read-only view of a dict[Expr, ReprToken].
#
# class NodeTokenView:
#
#   Methods defined here:
#
#     __getitem__(self, node: Expr) -> ReprToken
#       Returns a pre-rendered token for the node.
#
NodeTokenView = clib.NodeTokenView


# Registers a custom `op_repr_fn` for an operator family.
register_op_repr_fn_by_qvalue_specialization_key = (
    clib.register_op_repr_fn_by_qvalue_specialization_key
)


# Registers a custom `op_repr_fn` for a registered operator.
register_op_repr_fn_by_registration_name = (
    clib.register_op_repr_fn_by_registration_name
)
