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

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

py_library(
    name = "arolla",
    deps = [
        ":arolla_without_predeclared_operators",
        "//py/arolla/operators:operators_clib",
    ],
)

py_test(
    name = "arolla_test",
    srcs = ["arolla_test.py"],
    deps = [
        ":arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

# This library declares no operator outside of the bootstrap set.
#
# NOTE: Some of the library internals depend on the presence of additional
# operators, like array.size, but the library expects that these operators
# declared externally.
py_library(
    name = "arolla_without_predeclared_operators",
    srcs = ["arolla.py"],
    tags = [
        "avoid_dep",  # go/build-cleaner-faq#dont-pick-for-dep
    ],
    deps = [
        "//py/arolla/abc",
        "//py/arolla/expr",
        "//py/arolla/optools",
        "//py/arolla/s11n",
        "//py/arolla/testing",
        "//py/arolla/types",
    ],
)
