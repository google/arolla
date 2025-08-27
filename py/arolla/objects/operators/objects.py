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
from arolla.objects.operators import operators_qexpr_clib as _


M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'objects.make_object_qtype',
    qtype_inference_expr=arolla.QTYPE,
)
def make_object_qtype():
  """Returns an OBJECT QType."""
  raise NotImplementedError('provided by backend')
