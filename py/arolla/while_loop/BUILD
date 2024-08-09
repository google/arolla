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

# Tools to create while loops in expressions.

load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//py/arolla/dynamic_deps:build_defs.bzl", "arolla_pybind_extension")

package(default_visibility = ["//arolla:internal"])

licenses(["notice"])

py_library(
    name = "while_loop",
    srcs = [
        "while_loop.py",
    ],
    deps = [
        ":clib",
        "//py/arolla",
    ],
)

py_test(
    name = "while_loop_test",
    srcs = ["while_loop_test.py"],
    deps = [
        ":while_loop",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

arolla_pybind_extension(
    name = "clib",
    srcs = [
        "clib.cc",
    ],
    deps = [
        "//arolla/expr",
        "//arolla/expr/operators/while_loop",
        "//arolla/qtype",
        "//arolla/util",
        "//py/arolla/abc:pybind11_utils",
        "@com_google_absl//absl/strings:str_format",
        "@pybind11_abseil//pybind11_abseil:absl_casters",
    ],
)
