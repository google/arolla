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

load("//py/arolla/dynamic_deps:build_defs.bzl", "arolla_py_cc_deps")

package(default_visibility = ["//visibility:private"])

licenses(["notice"])

arolla_py_cc_deps(
    name = "operators_clib",
    visibility = ["//arolla:internal"],
    deps = [
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//py/arolla/operators/py",
    ],
)
