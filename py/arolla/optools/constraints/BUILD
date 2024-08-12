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

package(default_visibility = ["//visibility:private"])

licenses(["notice"])

py_library(
    name = "constraints",
    srcs = ["constraints.py"],
    visibility = ["//arolla:internal"],
    deps = [
        "//py/arolla/abc",
        "//py/arolla/expr",
        "//py/arolla/types",
    ],
)

py_test(
    name = "constraints_test",
    srcs = ["constraints_test.py"],
    deps = [
        "//py/arolla/abc",
        "//py/arolla/expr",
        "//py/arolla/operators:operators_clib",
        "//py/arolla/optools",
        "//py/arolla/testing",
        "//py/arolla/types",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)
