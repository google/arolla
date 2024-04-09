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

package(default_visibility = ["//py/arolla/expr:__subpackages__"])

licenses(["notice"])

py_library(
    name = "expr",
    srcs = [
        "expr.py",
    ],
    visibility = ["//arolla:internal"],
    deps = [
        ":lib",
    ],
)

py_library(
    name = "lib",
    srcs = [
        "builtin_ops.py",
        "containers.py",
        "default_view.py",
    ],
    deps = [
        "//py/arolla/abc",
        "//py/arolla/types",
    ],
)

py_test(
    name = "containers_test",
    srcs = [
        "containers_test.py",
    ],
    deps = [
        ":lib",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/bootstrap",
        "//py/arolla/abc",
        "//py/arolla/types",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "default_view_test",
    srcs = [
        "default_view_test.py",
    ],
    deps = [
        ":lib",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/bootstrap",
        "//py/arolla/testing",
        "//py/arolla/types",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)
