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

load("@rules_cc//cc:cc_binary.bzl", "cc_binary")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "eval",
    srcs = [
        "casting.cc",
        "compile_where_operator.cc",
        "compile_where_operator.h",
        "compile_while_operator.cc",
        "compile_while_operator.h",
        "dynamic_compiled_expr.cc",
        "dynamic_compiled_operator.cc",
        "eval.cc",
        "evaluator_operators.h",
        "executable_builder.cc",
        "expr_stack_trace.cc",
        "expr_stack_trace.h",
        "expr_utils.cc",
        "expr_utils.h",
        "extensions.cc",
        "invoke.cc",
        "model_executor.cc",
        "prepare_expression.cc",
        "side_output.cc",
        "slot_allocator.cc",
        "slot_allocator.h",
    ],
    hdrs = [
        "casting.h",
        "dynamic_compiled_expr.h",
        "dynamic_compiled_operator.h",
        "eval.h",
        "executable_builder.h",
        "extensions.h",
        "invoke.h",
        "model_executor.h",
        "prepare_expression.h",
        "side_output.h",
        "thread_safe_model_executor.h",
        "verbose_runtime_error.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/algorithm",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators:bootstrap",
        "//arolla/expr/operators/while_loop",
        "//arolla/expr/optimization",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/core:lib",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
    ],
)

# Headers listed as srcs in :eval, but needed in tests.
cc_library(
    name = "eval_headers_testonly",
    testonly = True,
    hdrs = [
        "compile_where_operator.h",
        "compile_while_operator.h",
        "dynamic_compiled_expr.h",
        "dynamic_compiled_operator.h",
        "executable_builder.h",
        "expr_stack_trace.h",
        "side_output.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        ":eval",
        "//arolla/expr",
        "//arolla/expr/operators/while_loop",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "test_utils",
    testonly = True,
    hdrs = [
        "test_utils.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        ":eval",
        "//arolla/qexpr",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "casting_test",
    srcs = ["casting_test.cc"],
    deps = [
        ":eval",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators:bootstrap",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/all",
        "//arolla/qexpr/testing",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "eval_test",
    srcs = ["eval_test.cc"],
    deps = [
        ":eval",
        ":test_utils",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/optimization/default",
        "//arolla/expr/testing",
        "//arolla/expr/testing:test_operators",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/all",
        "//arolla/qexpr/testing",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/util",
        "//arolla/util:status_backport",
        "//arolla/util/testing",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "prepare_expression_test",
    srcs = ["prepare_expression_test.cc"],
    deps = [
        ":eval",
        ":eval_headers_testonly",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/optimization",
        "//arolla/expr/testing",
        "//arolla/expr/testing:test_operators",
        "//arolla/qexpr/operators/all",
        "//arolla/qexpr/testing",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "invoke_test",
    srcs = ["invoke_test.cc"],
    deps = [
        ":eval",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//arolla/qexpr/testing",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "model_executor_test",
    srcs = ["model_executor_test.cc"],
    deps = [
        ":eval",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators:bootstrap",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/all",
        "//arolla/qexpr/testing",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/util",
        "//arolla/util:status_backport",
        "//arolla/util/testing",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "compile_where_operator_test",
    srcs = [
        "compile_where_operator_test.cc",
    ],
    deps = [
        ":eval",
        ":eval_headers_testonly",
        ":test_utils",
        "//arolla/array/qtype",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/optimization",
        "//arolla/expr/optimization/peephole_optimizations",
        "//arolla/expr/testing",
        "//arolla/expr/testing:test_operators",
        "//arolla/expr/visitors",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/all",
        "//arolla/qexpr/testing",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "compile_while_operator_test",
    srcs = ["compile_while_operator_test.cc"],
    deps = [
        ":eval",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/operators/while_loop",
        "//arolla/expr/visitors",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/qexpr/testing",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/util",
        "//arolla/util:status_backport",
        "//arolla/util/testing",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "slot_allocator_test",
    srcs = [
        "slot_allocator.h",
        "slot_allocator_test.cc",
    ],
    deps = [
        ":eval",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "thread_safe_model_executor_test",
    srcs = ["thread_safe_model_executor_test.cc"],
    deps = [
        ":eval",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/io",
        "//arolla/qexpr/operators/all",
        "//arolla/qexpr/testing",
        "//arolla/util/testing",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "side_output_test",
    srcs = ["side_output_test.cc"],
    deps = [
        ":eval",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/qexpr/operators/all",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "dynamic_compiled_expr_test",
    srcs = ["dynamic_compiled_expr_test.cc"],
    deps = [
        ":eval",
        ":test_utils",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "dynamic_compiled_operator_test",
    srcs = ["dynamic_compiled_operator_test.cc"],
    deps = [
        ":eval",
        ":test_utils",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "expr_utils_test",
    srcs = [
        "expr_utils.h",
        "expr_utils_test.cc",
    ],
    deps = [
        ":eval",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/qexpr/operators/all",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "executable_builder_test",
    srcs = ["executable_builder_test.cc"],
    deps = [
        ":eval",
        ":eval_headers_testonly",
        ":test_utils",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "extensions_test",
    srcs = ["extensions_test.cc"],
    deps = [
        ":eval",
        ":test_utils",
        "//arolla/expr",
        "//arolla/expr/operators:bootstrap",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "expr_stack_trace_test",
    srcs = ["expr_stack_trace_test.cc"],
    deps = [
        ":eval",
        ":eval_headers_testonly",
        "//arolla/expr",
        "//arolla/expr/testing",
        "//arolla/expr/testing:test_operators",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "math_benchmarks",
    testonly = 1,
    srcs = [
        "math_benchmarks.cc",
    ],
    deps = [
        ":eval",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/dense_array/testing",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/optimization/default",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_binary(
    name = "compile_benchmark",
    testonly = 1,
    srcs = [
        "compile_benchmark.cc",
    ],
    deps = [
        ":eval",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/io",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)
