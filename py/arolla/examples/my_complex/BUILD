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

# Example of custom QType definition.

load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//py/arolla/dynamic_deps:build_defs.bzl", "arolla_py_cc_deps")

package(default_visibility = ["//visibility:private"])

licenses(["notice"])

py_library(
    name = "my_complex",
    srcs = ["my_complex.py"],
    visibility = ["//visibility:public"],
    deps = [
        ":my_complex_clib",
        "//py/arolla",
    ],
)

py_test(
    name = "my_complex_test",
    srcs = ["my_complex_test.py"],
    deps = [
        ":my_complex",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_py_cc_deps(
    name = "my_complex_clib",
    deps = [
        ":my_complex_operators",
        ":my_complex_type",
    ],
)

cc_library(
    name = "my_complex_type",
    srcs = ["my_complex_type.cc"],
    hdrs = ["my_complex_type.h"],
    deps = [
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "my_complex_type_test",
    srcs = ["my_complex_type_test.cc"],
    deps = [
        ":my_complex_type",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "my_complex_operators",
    srcs = ["my_complex_operators.cc"],
    deps = [
        ":my_complex_type",
        "//arolla/optools",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/status",
    ],
    alwayslink = True,
)
