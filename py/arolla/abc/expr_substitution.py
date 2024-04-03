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

"""Expression substitution utilities."""

from arolla.abc import clib

# Replaces subexpressions specified by fingerprint.
sub_by_fingerprint = clib.sub_by_fingerprint

# Replaces subexpressions specified by names.
sub_by_name = clib.sub_by_name

# Replaces leaf nodes specified by keys.
sub_leaves = clib.sub_leaves

# Replaces placeholder nodes specified by keys.
sub_placeholders = clib.sub_placeholders
