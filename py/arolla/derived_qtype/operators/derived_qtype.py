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

"""Declaration of M.derived_qtype.* operators."""

from arolla import arolla

M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints

# go/keep-sorted start
downcast = arolla.abc.lookup_operator('derived_qtype.downcast')
get_labeled_qtype = arolla.abc.lookup_operator(
    'derived_qtype.get_labeled_qtype'
)
get_qtype_label = arolla.abc.lookup_operator('derived_qtype.get_qtype_label')
upcast = arolla.abc.lookup_operator('derived_qtype.upcast')
# go/keep-sorted end
