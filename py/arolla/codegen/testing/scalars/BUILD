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

# Tests, benchmarks and usage examples for operator codegeneration.

load("@rules_cc//cc:cc_binary.bzl", "cc_binary")
load("@rules_cc//cc:cc_test.bzl", "cc_test")
load("@rules_python//python:defs.bzl", "py_library")
load("//arolla/codegen:utils.bzl", "call_python_function")
load("//arolla/util/testing:testing.bzl", "benchmark_smoke_test")
load("//py/arolla/codegen:expr.bzl", "compiled_exprs")

package(default_visibility = ["//arolla:internal"])

licenses(["notice"])

py_library(
    name = "scalars",
    testonly = 1,
    srcs = ["scalars.py"],
    deps = [
        "//py:python_path",
        "//py/arolla",
        "//py/arolla/derived_qtype",
    ],
)

compiled_exprs(
    name = "literal_one",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledLiteralOne": call_python_function(
            "arolla.codegen.testing.scalars.scalars.literal_one",
            args = [],
            deps = [":scalars"],
        ),
    },
)

compiled_exprs(
    name = "identity_x",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledIdentityX": call_python_function(
            "arolla.codegen.testing.scalars.scalars.identity_x",
            args = [],
            deps = [":scalars"],
        ),
    },
)

compiled_exprs(
    name = "identity_x_expensive_inputs",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledIdentityXExpensiveInputs": call_python_function(
            "arolla.codegen.testing.scalars.scalars.identity_x",
            args = [],
            deps = [":scalars"],
        ),
    },
    tool_flags = {"inputs_are_cheap_to_read": False},
)

compiled_exprs(
    name = "x_plus_y_times_x",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledXPlusYTimesX": call_python_function(
            "arolla.codegen.testing.scalars.scalars.x_plus_y_times_x",
            args = [],
            deps = [":scalars"],
        ),
    },
    visibility = ["//py/arolla/codegen/testing:__subpackages__"],
    deps = ["//arolla/qexpr/operators/math:lib"],
)

compiled_exprs(
    name = "status_or_test_zero_result",
    testonly = 1,
    export_named_outputs = True,
    models = {
        "::test_namespace::GetCompiledStatusOrTestZeroResult": call_python_function(
            "arolla.codegen.testing.scalars.scalars.status_or_test_zero_result",
            args = [],
            deps = [":scalars"],
        ),
    },
    deps = ["//arolla/qexpr/operators/math:lib"],
)

compiled_exprs(
    name = "conditional_operators_test_zero_result",
    testonly = 1,
    export_named_outputs = True,
    models = {
        "::test_namespace::GetCompiledConditionalOperatorsTestZeroResult": call_python_function(
            "arolla.codegen.testing.scalars.scalars.conditional_operators_test_zero_result",
            args = [],
            deps = [":scalars"],
        ),
    },
    deps = [
        "//arolla/qexpr",
        "//arolla/qexpr/operators/bool:lib",
        "//arolla/qexpr/operators/core:lib",
        "//arolla/qexpr/operators/math:lib",
    ],
)

compiled_exprs(
    name = "const_ref_returning_operator_x_plus_y_result",
    testonly = 1,
    copts = [
        # we explicitly testing loss of ownership during function return
        # forbidding inlining help us easier detect the problem.
        "-fno-inline",
    ],
    models = {
        "::test_namespace::GetCompiledConstRefReturnXPlusYResult": call_python_function(
            "arolla.codegen.testing.scalars.scalars.const_ref_returning_operator_x_plus_y_result",
            args = [],
            deps = [":scalars"],
        ),
    },
    deps = [
        "//arolla/qexpr",
        "//arolla/qexpr/operators/bool:lib",
        "//arolla/qexpr/operators/core:lib",
        "//arolla/qexpr/operators/math:lib",
    ],
)

compiled_exprs(
    name = "big_inline_chain_zero",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledInlineChainZero": call_python_function(
            "arolla.codegen.testing.scalars.scalars.big_inline_chain",
            args = [],
            deps = [":scalars"],
        ),
    },
    deps = ["//arolla/qexpr/operators/math:lib"],
)

compiled_exprs(
    name = "x_plus_y_times_5",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledXPlusYTimes5": call_python_function(
            "arolla.codegen.testing.scalars.scalars.x_plus_y_times_5",
            args = [],
            deps = [":scalars"],
        ),
    },
    deps = ["//arolla/qexpr/operators/math:lib"],
)

compiled_exprs(
    name = "x_plus_y_times_32_with_named_nodes",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledXPlusYTimes32WithNamedNodes": call_python_function(
            "arolla.codegen.testing.scalars.scalars.x_plus_y_times_32_with_named_nodes",
            args = [],
            deps = [":scalars"],
        ),
    },
    deps = ["//arolla/qexpr/operators/math:lib"],
)

compiled_exprs(
    name = "x_plus_y_times_5_with_export",
    testonly = 1,
    export_named_outputs = True,
    models = {
        "::test_namespace::GetCompiledXPlusYTimes5WithExport": call_python_function(
            "arolla.codegen.testing.scalars.scalars.x_plus_y_times_5_with_export",
            args = [],
            deps = [":scalars"],
        ),
    },
    deps = ["//arolla/qexpr/operators/math:lib"],
)

compiled_exprs(
    name = "x_plus_y_times_5_with_unused_export_x_minus_5",
    testonly = 1,
    export_named_outputs = True,
    models = {
        "::test_namespace::GetCompiledXPlusYTimes5WithUnusedExportXMinus5": call_python_function(
            "arolla.codegen.testing.scalars.scalars.x_plus_y_times_5_with_unused_export_x_minus_5",
            args = [],
            deps = [":scalars"],
        ),
    },
    deps = ["//arolla/qexpr/operators/math:lib"],
)

compiled_exprs(
    name = "x_plus_y_times_5_with_unused_two_nested_exports_xm5_andxm10",
    testonly = 1,
    export_named_outputs = True,
    models = {
        "::test_namespace::GetCompiledXPlusYTimes5WithUnusedTwoNestedExportsXM5AndXm10": call_python_function(
            "arolla.codegen.testing.scalars.scalars.x_plus_y_times_5_with_unused_two_nested_exports_xm5_andxm10",
            args = [],
            deps = [":scalars"],
        ),
    },
    deps = ["//arolla/qexpr/operators/math:lib"],
)

compiled_exprs(
    name = "x_plus_y_times_5_nested_export",
    testonly = 1,
    export_named_outputs = True,
    models = {
        "::test_namespace::GetCompiledXPlusYTimes5NestedExport": call_python_function(
            "arolla.codegen.testing.scalars.scalars.x_plus_y_times_5_nested_export",
            args = [],
            deps = [":scalars"],
        ),
    },
    deps = ["//arolla/qexpr/operators/math:lib"],
)

compiled_exprs(
    name = "x_plus_y_times_5_duplicated_export",
    testonly = 1,
    export_named_outputs = True,
    models = {
        "::test_namespace::GetCompiledXPlusYTimes5DuplicatedExport": call_python_function(
            "arolla.codegen.testing.scalars.scalars.x_plus_y_times_5_duplicated_export",
            args = [],
            deps = [":scalars"],
        ),
        "::test_namespace::GetCompiledXPlusYTimes5DuplicatedExportUnused": call_python_function(
            "arolla.codegen.testing.scalars.scalars.x_plus_y_times_5_duplicated_export_unused",
            args = [],
            deps = [":scalars"],
        ),
    },
    deps = ["//arolla/qexpr/operators/math:lib"],
)

compiled_exprs(
    name = "x_plus_y_times_5_with_export_nodes_but_disabled_export",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledXPlusYTimes5WithExportButDisabled": call_python_function(
            "arolla.codegen.testing.scalars.scalars.x_plus_y_times_5_with_export",
            args = [],
            deps = [":scalars"],
        ),
    },
    deps = ["//arolla/qexpr/operators/math:lib"],
)

compiled_exprs(
    name = "x_plus_y_optional",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledXPlusYOptional": call_python_function(
            "arolla.codegen.testing.scalars.scalars.x_plus_y_optional",
            args = [],
            deps = [":scalars"],
        ),
    },
    deps = [
        "//arolla/qexpr",
        "//arolla/qexpr/operators/math:lib",
    ],
)

compiled_exprs(
    name = "two_independent_fns",
    testonly = 1,
    models = {
        "::test_namespace::GetSumABXY": call_python_function(
            "arolla.codegen.testing.scalars.scalars.two_independent_fns",
            args = [],
            deps = [":scalars"],
        ),
    },
    deps = ["//arolla/qexpr/operators/math:lib"],
)

compiled_exprs(
    name = "two_long_fibonacci_chains",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledTwoFibonacciChains": call_python_function(
            "arolla.codegen.testing.scalars.scalars.two_long_fibonacci_chains",
            args = [],
            deps = [":scalars"],
        ),
    },
    deps = ["//arolla/qexpr/operators/math:lib"],
)

compiled_exprs(
    name = "two_long_fibonacci_chains_expensive_inputs",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledTwoFibonacciChainsExpensiveInputs": call_python_function(
            "arolla.codegen.testing.scalars.scalars.two_long_fibonacci_chains",
            args = [],
            deps = [":scalars"],
        ),
    },
    tool_flags = {"inputs_are_cheap_to_read": False},
    deps = ["//arolla/qexpr/operators/math:lib"],
)

compiled_exprs(
    name = "many_nested_long_fibonacci_chains",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledManyNestedFibonacciChains": call_python_function(
            "arolla.codegen.testing.scalars.scalars.many_nested_long_fibonacci_chains",
            args = [],
            deps = [":scalars"],
        ),
    },
    deps = ["//arolla/qexpr/operators/math:lib"],
)

compiled_exprs(
    name = "text_contains",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledTextContains": call_python_function(
            "arolla.codegen.testing.scalars.scalars.text_contains",
            args = [],
            deps = [":scalars"],
        ),
    },
    deps = ["//arolla/qexpr/operators/strings:lib"],
)

compiled_exprs(
    name = "bytes_contains_me",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledBytesContainsMe": call_python_function(
            "arolla.codegen.testing.scalars.scalars.bytes_contains_me",
            args = [],
            deps = [":scalars"],
        ),
    },
    deps = ["//arolla/qexpr/operators/strings:lib"],
)

compiled_exprs(
    name = "variadic_hello_str_join",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledHelloVariadicJoin": call_python_function(
            "arolla.codegen.testing.scalars.scalars.variadic_hello_str_join",
            args = [],
            deps = [":scalars"],
        ),
    },
    deps = ["//arolla/qexpr/operators/strings:lib"],
)

compiled_exprs(
    name = "variadic_hello_str_join_optional",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledHelloVariadicJoinOptional": call_python_function(
            "arolla.codegen.testing.scalars.scalars.variadic_hello_str_join_optional",
            args = [],
            deps = [":scalars"],
        ),
    },
    deps = ["//arolla/qexpr/operators/strings:lib"],
)

compiled_exprs(
    name = "variadic_equation_str_printf",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledEquationVariadicStrPrintf": call_python_function(
            "arolla.codegen.testing.scalars.scalars.variadic_equation_str_printf",
            args = [],
            deps = [":scalars"],
        ),
    },
    deps = [
        "//arolla/qexpr/operators/math:lib",
        "//arolla/qexpr/operators/strings:lib",
    ],
)

compiled_exprs(
    name = "variadic_equation_str_printf_optional",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledEquationVariadicStrPrintfOptional": call_python_function(
            "arolla.codegen.testing.scalars.scalars.variadic_equation_str_printf_optional",
            args = [],
            deps = [":scalars"],
        ),
    },
    deps = [
        "//arolla/qexpr",
        "//arolla/qexpr/operators/core:lib",
        "//arolla/qexpr/operators/math:lib",
        "//arolla/qexpr/operators/strings:lib",
    ],
)

compiled_exprs(
    name = "derived_qtype_casts",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledDerivedQTypeCasts": call_python_function(
            "arolla.codegen.testing.scalars.scalars.derived_qtype_casts",
            args = [],
            deps = [":scalars"],
        ),
    },
)

# Example for specifying a custom type representation for codegen.
# in this particular case we are overriding specification for builtin double.
compiled_exprs(
    name = "x_plus_y_times_5_overridden_type",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledXPlusYTimes5": call_python_function(
            "arolla.codegen.testing.scalars.scalars.x_plus_y_times_5",
            args = [],
            deps = [":scalars"],
        ),
    },
    tool_deps = [
        "//py/arolla/codegen/testing_tools:override_type_repr",  # adding registration to the dependency
    ],
    deps = ["//arolla/qexpr/operators/math:lib"],
)

cc_test(
    name = "scalars_test",
    srcs = ["scalars_test.cc"],
    deps = [
        ":big_inline_chain_zero",
        ":bytes_contains_me",
        ":conditional_operators_test_zero_result",
        ":const_ref_returning_operator_x_plus_y_result",
        ":derived_qtype_casts",
        ":identity_x",
        ":identity_x_expensive_inputs",
        ":literal_one",
        ":many_nested_long_fibonacci_chains",
        ":status_or_test_zero_result",
        ":text_contains",
        ":two_long_fibonacci_chains",
        ":two_long_fibonacci_chains_expensive_inputs",
        ":variadic_equation_str_printf",
        ":variadic_equation_str_printf_optional",
        ":variadic_hello_str_join",
        ":variadic_hello_str_join_optional",
        ":x_plus_y_optional",
        ":x_plus_y_times_32_with_named_nodes",
        ":x_plus_y_times_5",
        ":x_plus_y_times_5_duplicated_export",
        ":x_plus_y_times_5_nested_export",
        ":x_plus_y_times_5_with_export",
        ":x_plus_y_times_5_with_export_nodes_but_disabled_export",
        ":x_plus_y_times_5_with_unused_export_x_minus_5",
        ":x_plus_y_times_5_with_unused_two_nested_exports_xm5_andxm10",
        ":x_plus_y_times_x",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

# Benchmarks

compiled_exprs(
    name = "float_benchmark_expr_10k",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledFloat10KBenchmark": call_python_function(
            "arolla.codegen.testing.scalars.scalars.float_benchmark_expr",
            args = ["10_000"],
            deps = [":scalars"],
        ),
    },
    deps = ["//arolla/qexpr/operators/math:lib"],
)

compiled_exprs(
    name = "optional_float_benchmark_expr_1k",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledOptionalFloat1KBenchmark": call_python_function(
            "arolla.codegen.testing.scalars.scalars.optional_float_benchmark_expr",
            args = ["1_000"],
            deps = [":scalars"],
        ),
    },
    deps = [
        "//arolla/qexpr",
        "//arolla/qexpr/operators/math:lib",
    ],
)

compiled_exprs(
    name = "double_benchmark_expr_10k",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledDouble10KBenchmark": call_python_function(
            "arolla.codegen.testing.scalars.scalars.double_benchmark_expr",
            args = ["10_000"],
            deps = [":scalars"],
        ),
    },
    deps = ["//arolla/qexpr/operators/math:lib"],
)

compiled_exprs(
    name = "double_two_parallel_benchmark_expr_1k",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledDoubleTwoParallelComputes1KBenchmark": call_python_function(
            "arolla.codegen.testing.scalars.scalars.double_two_parallel_computes_benchmark_expr",
            args = ["1_000"],
            deps = [":scalars"],
        ),
    },
    deps = ["//arolla/qexpr/operators/math:lib"],
)

compiled_exprs(
    name = "many_inputs_and_side_outputs",
    testonly = 1,
    models = {
        "::test_namespace::GetManyInputsAndSideOutputs": call_python_function(
            "arolla.codegen.testing.scalars.scalars.many_inputs_and_side_outputs",
            args = ["1_000"],
            deps = [":scalars"],
        ),
    },
    deps = ["//arolla/qexpr/operators/math:lib"],
)

cc_binary(
    name = "scalar_benchmarks",
    testonly = 1,
    srcs = ["scalar_benchmarks.cc"],
    deps = [
        ":double_benchmark_expr_10k",
        ":double_two_parallel_benchmark_expr_1k",
        ":float_benchmark_expr_10k",
        ":optional_float_benchmark_expr_1k",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/serving",
        "@com_google_absl//absl/log:check",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

# test that initializations is not overflowing a stack
cc_test(
    name = "initialize_stack_test",
    srcs = ["initialize_stack_test.cc"],
    deps = [
        ":many_inputs_and_side_outputs",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
    ],
)

benchmark_smoke_test(
    name = "scalar_benchmarks_smoke_test",
    binary = ":scalar_benchmarks",
)
