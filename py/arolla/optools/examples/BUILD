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

load("@rules_python//python:defs.bzl", "py_library", "py_test")
load(
    "//py/arolla/dynamic_deps:build_defs.bzl",
    "arolla_py_cc_deps",
)
load(
    "//py/arolla/optools:optools.bzl",
    "arolla_cc_operator_package",
    "arolla_operator_package_snapshot",
)

package(default_visibility = ["//arolla:internal"])

licenses(["notice"])

# We wrap the C++ library "solve_quadratic_equation_cc" using
# arolla_py_cc_deps() for dynamic loading within a Python module.
arolla_py_cc_deps(
    name = "solve_quadratic_equation_clib",
    deps = [":solve_quadratic_equation_cc"],
)

arolla_cc_operator_package(
    name = "solve_quadratic_equation_cc",
    snapshot = ":solve_quadratic_equation.operator_package.pb2",
)

arolla_operator_package_snapshot(
    name = "solve_quadratic_equation.operator_package.pb2",
    imports = ["arolla.optools.examples.solve_quadratic_equation"],
    deps = [":solve_quadratic_equation_py"],
)

py_library(
    name = "solve_quadratic_equation_py",
    srcs = ["solve_quadratic_equation.py"],
    deps = ["//py/arolla"],
)

# Note: We use ":cc_deps_solve_quadratic_equation" to demonstrate loading
# operators from a snapshot, though direct import of ":solve_quadratic_equation_py" is also possible.
py_test(
    name = "solve_quadratic_equation_test",
    srcs = ["solve_quadratic_equation_test.py"],
    deps = [
        ":solve_quadratic_equation_clib",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)
