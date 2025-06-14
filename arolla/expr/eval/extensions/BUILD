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

# Custom operator support for arolla/expr/eval.

load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")

package(default_visibility = ["//arolla:internal"])

licenses(["notice"])

cc_library(
    name = "extensions",
    srcs = [
        "compile_core_map_operator.cc",
        "compile_std_function_operator.cc",
        "prepare_core_map_operator.cc",
        "prepare_core_map_operator.h",
        "seq_map_operator.cc",
        "seq_map_operator.h",
        "seq_reduce_operator.cc",
        "seq_reduce_operator.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/operators:bootstrap",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/sequence",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
    alwayslink = True,
)

cc_test(
    name = "core_map_operator_test",
    srcs = [
        "core_map_operator_test.cc",
        "prepare_core_map_operator.h",
    ],
    deps = [
        ":extensions",
        "//arolla/array/qtype",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/eval:test_utils",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util/testing",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "seq_map_operator_test",
    srcs = [
        "seq_map_operator.h",
        "seq_map_operator_test.cc",
    ],
    deps = [
        ":extensions",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/eval:test_utils",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/sequence",
        "//arolla/util/testing",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "seq_reduce_operator_test",
    srcs = [
        "seq_reduce_operator.h",
        "seq_reduce_operator_test.cc",
    ],
    deps = [
        ":extensions",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/eval:test_utils",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/sequence",
        "//arolla/util/testing",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "compile_std_function_operator_test",
    srcs = [
        "compile_std_function_operator_test.cc",
    ],
    deps = [
        ":extensions",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/eval:test_utils",
        "//arolla/expr/operators:bootstrap",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/util:status_backport",
        "//arolla/util/testing",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)
