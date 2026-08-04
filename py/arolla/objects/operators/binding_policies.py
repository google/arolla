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

"""Binding policies for M.objects.* operators."""

from arolla import arolla

M = arolla.M


def _make_object_binding_policy(prototype=None, attrs=None, /, **kwargs):
  """Binding policy for M.objects.make_object."""
  prototype = arolla.types.as_qvalue_or_expr(prototype)
  if attrs is None:
    has_expr = False
    for k, v in kwargs.items():
      qv = arolla.types.as_qvalue_or_expr(v)
      kwargs[k] = qv
      has_expr = has_expr or isinstance(qv, arolla.Expr)
    if has_expr:
      attrs = M.namedtuple.make(**kwargs)
    else:
      attrs = arolla.namedtuple(**kwargs)
  elif kwargs:
    raise ValueError(
        'arguments `attrs` and `**kwargs` cannot be specified together'
    )
  return prototype, attrs


MAKE_OBJECT_BINDING_POLICY = 'objects_make_object_binding_policy'

arolla.abc.register_adhoc_aux_binding_policy(
    MAKE_OBJECT_BINDING_POLICY, _make_object_binding_policy
)
