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

load("@rules_python//python:defs.bzl", "py_library")
load("//py/arolla/dynamic_deps:build_defs.bzl", "arolla_py_cc_deps")
load("//py/arolla/optools:optools.bzl", "arolla_operator_package_snapshot")

package(default_visibility = [
    "//visibility:public",
])

licenses(["notice"])

arolla_operator_package_snapshot(
    name = "operator_package.pb2",
    imports = ["arolla.jagged_shape.operators.jagged"],
    visibility = ["//arolla:internal"],
    deps = [":jagged_py"],
)

py_library(
    name = "jagged_py",
    srcs = ["jagged.py"],
    deps = [
        ":operators_qexpr_clib",
        "//py/arolla",
    ],
)

arolla_py_cc_deps(
    name = "operators_clib",
    visibility = ["//arolla:internal"],
    deps = [
        "//arolla/jagged_shape/array/qexpr",
        "//arolla/jagged_shape/array/serialization_codecs",
        "//arolla/jagged_shape/dense_array/qexpr",
        "//arolla/jagged_shape/dense_array/serialization_codecs",
        "//arolla/jagged_shape/expr",
        "//arolla/jagged_shape/qexpr",
    ],
)

# Note: This target partially duplicates ":operators_clib",
# but it is used exclusively for the operator package declaration. Both
# targets are never loaded together.
arolla_py_cc_deps(
    name = "operators_qexpr_clib",
    deps = ["//arolla/jagged_shape/qexpr"],
)
