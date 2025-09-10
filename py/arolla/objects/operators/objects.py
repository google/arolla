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

"""Declaration of M.objects.* operators."""

from arolla import arolla
from arolla.objects.operators import binding_policies
from arolla.objects.operators import clib as _


M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints

# Defined in C++.
get_object_attr = arolla.abc.lookup_operator('objects.get_object_attr')
get_object_attr_qtype = arolla.abc.lookup_operator(
    'objects.get_object_attr_qtype'
)
make_object_qtype = arolla.abc.lookup_operator('objects.make_object_qtype')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'objects.make_object',
    qtype_constraints=[
        (
            (P.prototype == arolla.UNSPECIFIED)
            | (P.prototype == make_object_qtype()),
            f'expected OBJECT, got {constraints.name_type_msg(P.prototype)}',
        ),
        (
            M.qtype.is_namedtuple_qtype(P.attrs),
            f'expected a NamedTuple, got {constraints.name_type_msg(P.attrs)}',
        ),
    ],
    qtype_inference_expr=make_object_qtype(),
    experimental_aux_policy=binding_policies.MAKE_OBJECT_BINDING_POLICY,
)
def make_object(prototype, attrs):  # pylint: disable=unused-argument
  """Constructs an Object with the provided `attrs` and `prototype`."""
  raise NotImplementedError('implemented in the backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('objects.has_object_attr')
def has_object_attr(obj, attr):
  """Returns present iff `attr` is an attribute of `obj`."""
  return get_object_attr_qtype(obj, attr) != arolla.NOTHING
