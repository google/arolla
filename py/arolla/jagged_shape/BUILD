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
    name = "jagged_shape",
    srcs = ["jagged_shape.py"],
    visibility = ["//visibility:public"],
    deps = [
        ":types",
    ],
)

py_library(
    name = "types",
    srcs = ["types.py"],
    deps = [
        "//py/arolla",
        "//py/arolla/jagged_shape/operators:operators_clib",
    ],
)

py_test(
    name = "types_test",
    srcs = ["types_test.py"],
    deps = [
        ":jagged_shape",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)
