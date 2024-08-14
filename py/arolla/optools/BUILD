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

load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//py/arolla/dynamic_deps:build_defs.bzl", "arolla_pybind_extension")

package(default_visibility = ["//visibility:private"])

licenses(["notice"])

py_library(
    name = "optools",
    srcs = [
        "decorators.py",
        "helpers.py",
        "optools.py",
    ],
    visibility = ["//arolla:internal"],
    deps = [
        "//py/arolla/abc",
        "//py/arolla/optools/constraints",
        "//py/arolla/types",
    ],
)

py_test(
    name = "decorators_test",
    srcs = ["decorators_test.py"],
    deps = [
        ":optools",
        "//py/arolla/abc",
        "//py/arolla/expr",
        "//py/arolla/operators:operators_clib",
        "//py/arolla/testing",
        "//py/arolla/types",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "helpers_test",
    srcs = ["helpers_test.py"],
    deps = [
        ":optools",
        "//py/arolla/abc",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_library(
    name = "gen_operator_package",
    srcs = ["gen_operator_package.py"],
    visibility = ["//visibility:public"],
    deps = [
        ":clib",
        "//py/arolla:arolla_without_predeclared_operators",
        "@com_google_absl_py//absl:app",
        "@com_google_absl_py//absl/flags",
    ],
)

arolla_pybind_extension(
    name = "clib",
    srcs = ["clib.cc"],
    pytype_srcs = ["clib.pyi"],
    deps = [
        "//arolla/codegen/operator_package",
        "//py/arolla/abc:pybind11_utils",
        "@com_google_absl//absl/strings:string_view",
        "@pybind11_abseil//pybind11_abseil:absl_casters",
    ],
)

exports_files(["gen_operator_package.py"])  # for arolla_operator_package_snapshot

bzl_library(
    name = "optools_bzl",
    srcs = ["optools.bzl"],
    visibility = ["//visibility:public"],
    deps = [
        "//arolla/codegen:utils_bzl",
        "//arolla/codegen/operator_package:operator_package_bzl",
    ],
)
