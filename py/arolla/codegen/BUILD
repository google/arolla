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

# Codegeneration utilities for evaluation.

load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load("@pip_deps//:requirements.bzl", "requirement")
load("@rules_python//python:defs.bzl", "py_library", "py_test")
load(
    "//py/arolla/dynamic_deps:build_defs.bzl",
    "arolla_pybind_extension",
)

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

arolla_pybind_extension(
    name = "clib",
    srcs = ["clib.cc"],
    dynamic_deps = [
        "//py/arolla/dynamic_deps:arolla_standard_operators_so",
    ],
    deps = [
        "//py/arolla/abc:pybind11_utils",
        # TODO: Remove the dependency on /all, keeping only all:metadata.
        "//arolla/qexpr/operators/all",
        "//arolla/qexpr/operators/all:metadata",
        "//arolla/codegen/dict",
        "//arolla/codegen/expr",
        "//arolla/expr",
    ],
)

exports_files([
    "compiled_expr_main.py",
    "codegen_function.cc.jinja2",
    "codegen_function.h.jinja2",
])

py_library(
    name = "compiled_expr_main",
    srcs = [
        "compiled_expr_lib.py",
        "compiled_expr_main.py",
    ],
    data = [
        "compiled_expr.cc.jinja2",
        "compiled_expr.h.jinja2",
        "compiled_expr_impl.cc.jinja2",
    ],
    deps = [
        ":clib",
        "//arolla/codegen:utils",
        "//arolla/codegen/io",
        "//arolla/proto:io_py_proto",
        "//py:python_path",
        "//py/arolla",
        requirement("jinja2"),
        "@com_google_absl_py//absl:app",
        "@com_google_absl_py//absl/flags",
        "@com_google_protobuf//:protobuf_python",
    ],
)

py_library(
    name = "codegen_function",
    srcs = [
        "codegen_function.py",
    ],
    deps = [
        "//arolla/codegen/io",
        "//py:python_path",
        "//py/arolla",
        "@com_google_protobuf//:protobuf_python",
    ],
)

py_test(
    name = "codegen_function_test",
    srcs = ["codegen_function_test.py"],
    deps = [
        ":codegen_function",
        "//py:python_path",
        "//py/arolla",
        "//py/arolla/codegen/testing:example_codegen_function_extensions_py_proto",
        "//py/arolla/codegen/testing:example_codegen_function_models",
        "//py/arolla/codegen/testing:example_codegen_function_py_proto",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

bzl_library(
    name = "expr_bzl",
    srcs = ["expr.bzl"],
    visibility = ["//visibility:public"],
    deps = [
        "//arolla/codegen:utils_bzl",
        "//arolla/codegen/io:io_bzl",
    ],
)
