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

# Testing utilities.

load("@rules_python//python:defs.bzl", "py_library", "py_test")

package(default_visibility = ["//py/arolla/testing:__subpackages__"])

licenses(["notice"])

py_library(
    name = "testing",
    srcs = ["testing.py"],
    visibility = ["//arolla:internal"],
    deps = [
        ":detect_qtype_signatures",
        ":test_utils",
    ],
)

py_library(
    name = "test_utils",
    srcs = ["test_utils.py"],
    deps = [
        "//py/arolla/abc",
        "//py/arolla/types",
    ],
)

py_library(
    name = "detect_qtype_signatures",
    srcs = ["detect_qtype_signatures.py"],
    deps = [
        "//py/arolla/abc",
        "//py/arolla/types",
    ],
)

py_test(
    name = "test_utils_test",
    srcs = ["test_utils_test.py"],
    deps = [
        ":test_utils",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//py/arolla/abc",
        "//py/arolla/expr",
        "//py/arolla/types",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "detect_qtype_signatures_test",
    srcs = ["detect_qtype_signatures_test.py"],
    deps = [
        ":detect_qtype_signatures",
        "//py/arolla/abc",
        "//py/arolla/expr",
        "//py/arolla/types",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)
