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

# Tools for serving Expr models.

load("//arolla/util/testing:testing.bzl", "benchmark_smoke_test")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "serving",
    hdrs = [
        "embedded_model.h",
        "expr_compiler.h",
        "inplace_expr_compiler.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        ":expr_compiler_optimizer",
        ":serving_lite",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/optimization",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "expr_compiler_optimizer",
    srcs = ["expr_compiler_optimizer_initializer.cc"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = ["//visibility:private"],
    deps = [
        ":serving_lite",
        "//arolla/expr/optimization/default",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
    ],
    alwayslink = 1,
)

# The main difference from ":serving" is that it doesn't depend on ":expr_compiler_optimizer".
# Allows to reduce binary size by about 100KB by the cost of slower evaluation. Useful only in rare cases.
cc_library(
    name = "serving_lite",
    srcs = [
        "expr_compiler.cc",
    ],
    hdrs = [
        "embedded_model.h",
        "expr_compiler.h",
        "inplace_expr_compiler.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["avoid_dep"],
    deps = [
        ":serving_inplace",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/optimization",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
    ],
)

# The lightest serving target to be used for binaries that only need
# inplace computation of a codegened model (CompileInplaceExprOnStruct).
# NOTE: operator dependencies will be needed to support dynamic evalution on structs.
cc_library(
    name = "serving_inplace",
    srcs = [
        "inplace_expr_compiler.cc",
    ],
    hdrs = [
        "embedded_model.h",
        "inplace_expr_compiler.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["avoid_dep"],
    deps = [
        "//arolla/expr",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/naming",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
    ],
)

cc_test(
    name = "expr_compiler_test",
    srcs = ["expr_compiler_test.cc"],
    deps = [
        ":serving",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "inplace_expr_compiler_test",
    srcs = ["inplace_expr_compiler_test.cc"],
    deps = [
        ":serving_inplace",
        ":serving_lite",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "embedded_model_test",
    srcs = ["embedded_model_test.cc"],
    deps = [
        ":serving",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/io",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

benchmark_smoke_test(
    name = "embedded_model_test_benchmark_smoke_test",
    binary = ":embedded_model_test",
)

cc_test(
    name = "embedded_model_error_test",
    srcs = ["embedded_model_error_test.cc"],
    deps = [
        ":serving",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/io",
        "//arolla/qexpr/operators/all",
        "//arolla/util",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "embedded_model_set_error_test",
    srcs = ["embedded_model_set_error_test.cc"],
    deps = [
        ":serving",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/io",
        "//arolla/qexpr/operators/all",
        "//arolla/util",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)
