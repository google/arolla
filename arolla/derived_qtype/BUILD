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

# Package with additional tooling for Derived QTypes
#
# The fundamental implementation of derived QTypes is currently part of the core library, primarily
# because:
# - it's used for the implementation of weak_float.
# - it's used for the implementation of namedtuples.
# - the derived upcasting/downcasting has custom compiler support to achieve zero-copy.
#
# However, it's preferable to add any new derived QType implementations and features here rather
# than in the core library, to limit its complexity. Additionally, if it's possible to refactor some
# core code and move more logic, it might also be preferable to move it here. Importantly, the core
# libraries shouldn't depend on this package (otherwise, it becomes a part of the core itself).

package(default_visibility = ["//arolla:internal"])

licenses(["notice"])
