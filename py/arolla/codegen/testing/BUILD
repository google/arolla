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
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")
load("@rules_python//python:defs.bzl", "py_library")
load("@rules_python//python:proto.bzl", "py_proto_library")
load("//arolla/codegen:utils.bzl", "call_python_function")
load("//arolla/codegen/io:io.bzl", "input_loader", "protopath_accessor", "slot_listener")
load("//arolla/util/testing:testing.bzl", "benchmark_smoke_test")
load("//py/arolla/codegen:expr.bzl", "arolla_codegen_functions", "compiled_exprs")

package(default_visibility = ["//arolla:internal"])

licenses(["notice"])

compiled_exprs(
    name = "x_plus_y_times_5_batch",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledXPlusYTimes5Batch": call_python_function(
            "arolla.codegen.testing.batch.x_plus_y_times_5",
            args = [],
            deps = [":batch"],
        ),
    },
    deps = [
        "//arolla/dense_array/qtype",
        "//arolla/qexpr/operators/dense_array:lib",
        "//arolla/qexpr/operators/math:lib",
    ],
)

compiled_exprs(
    name = "two_long_fibonacci_chains_batch",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledTwoFibonacciChainsBatch": call_python_function(
            "arolla.codegen.testing.batch.two_long_fibonacci_chains",
            args = [],
            deps = [":batch"],
        ),
    },
    deps = [
        "//arolla/dense_array/qtype",
        "//arolla/qexpr/operators/dense_array:lib",
        "//arolla/qexpr/operators/math:lib",
    ],
)

compiled_exprs(
    name = "reduce_tuple",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledWXYZDivisionViaReduceTuple": call_python_function(
            "arolla.codegen.testing.tuple.reduce_tuple",
            args = [],
            deps = [":tuple"],
        ),
    },
    deps = ["//arolla/qexpr/operators/math:lib"],
)

compiled_exprs(
    name = "make_tuple",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledEmptyTuple": call_python_function(
            "arolla.codegen.testing.tuple.make_empty_tuple",
            args = [],
            deps = [":tuple"],
        ),
        "::test_namespace::GetCompiledLiteralTuple": call_python_function(
            "arolla.codegen.testing.tuple.make_literal_tuple",
            args = [],
            deps = [":tuple"],
        ),
        "::test_namespace::GetCompiledMakeFlatTuple": call_python_function(
            "arolla.codegen.testing.tuple.make_flat_tuple",
            args = [],
            deps = [":tuple"],
        ),
        "::test_namespace::GetCompiledNestedTuple": call_python_function(
            "arolla.codegen.testing.tuple.make_nested_tuple",
            args = [],
            deps = [":tuple"],
        ),
    },
    deps = [
        "//arolla/qexpr/operators/bootstrap:lib",
        "//arolla/qexpr/operators/math:lib",
    ],
)

compiled_exprs(
    name = "aggregation_dot_product_times_5",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledAggregationDotProductTimes5": call_python_function(
            "arolla.codegen.testing.batch.aggregation_dot_product_times_5",
            args = [],
            deps = [":batch"],
        ),
    },
    deps = [
        "//arolla/dense_array/qtype",
        "//arolla/qexpr/operators/aggregation:lib",
        "//arolla/qexpr/operators/core:lib",
        "//arolla/qexpr/operators/dense_array:lib",
        "//arolla/qexpr/operators/math:lib",
    ],
)

# "Hybrid" models are used in batch_scalar_hybrid_test in order
# to demonstrate the way to mix such evaluations including input loader.
compiled_exprs(
    name = "fully_batch_expr_for_hybrid",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledFullyBatchForHybrid": call_python_function(
            "arolla.codegen.testing.batch_scalar_hybrid.fully_batch_expr",
            args = [],
            deps = [":batch_scalar_hybrid"],
        ),
    },
    deps = [
        "//arolla/dense_array/qtype",
        "//arolla/qexpr/operators/aggregation:lib",
        "//arolla/qexpr/operators/core:lib",
        "//arolla/qexpr/operators/dense_array:lib",
        "//arolla/qexpr/operators/math:lib",
    ],
)

compiled_exprs(
    name = "hybrid_batch_expr",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledHybridBatchPart": call_python_function(
            "arolla.codegen.testing.batch_scalar_hybrid.hybrid_batch_expr",
            args = [],
            deps = [":batch_scalar_hybrid"],
        ),
    },
    deps = [
        "//arolla/qexpr/operators/aggregation:lib",
        "//arolla/qexpr/operators/core:lib",
        "//arolla/qexpr/operators/dense_array:lib",
        "//arolla/qexpr/operators/math:lib",
    ],
)

compiled_exprs(
    name = "hybrid_pointwise_expr",
    testonly = 1,
    export_named_outputs = True,
    models = {
        "::test_namespace::GetCompiledHybridPointwisePart": call_python_function(
            "arolla.codegen.testing.batch_scalar_hybrid.hybrid_pointwise_expr",
            args = [],
            deps = [":batch_scalar_hybrid"],
        ),
    },
    deps = ["//arolla/qexpr/operators/math:lib"],
)

py_library(
    name = "tuple",
    testonly = 1,
    srcs = ["tuple.py"],
    deps = [
        "//py:python_path",
        "//py/arolla",
    ],
)

py_library(
    name = "batch",
    testonly = 1,
    srcs = ["batch.py"],
    deps = [
        "//py:python_path",
        "//py/arolla",
    ],
)

py_library(
    name = "batch_scalar_hybrid",
    testonly = 1,
    srcs = ["batch_scalar_hybrid.py"],
    deps = [
        "//py:python_path",
        "//py/arolla",
    ],
)

cc_test(
    name = "tuple_test",
    srcs = ["tuple_test.cc"],
    deps = [
        ":make_tuple",
        ":reduce_tuple",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "batch_scalar_hybrid_test",
    srcs = ["batch_scalar_hybrid_test.cc"],
    deps = [
        ":fully_batch_expr_for_hybrid",
        ":hybrid_batch_expr",
        ":hybrid_pointwise_expr",
        "//arolla/dense_array",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/serving",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "model_executor_test",
    srcs = ["model_executor_test.cc"],
    deps = [
        "//arolla/io",
        "//arolla/serving",
        "//py/arolla/codegen/testing/scalars:x_plus_y_times_x",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

# Benchmarks

compiled_exprs(
    name = "float_benchmark_expr_1k_batch",
    testonly = 1,
    models = {
        "::test_namespace::GetCompiledFloat1KBatchBenchmark": call_python_function(
            "arolla.codegen.testing.batch.float_benchmark_expr",
            args = ["1_000"],
            deps = [":batch"],
        ),
    },
    deps = [
        "//arolla/dense_array/qtype",
        "//arolla/qexpr/operators/dense_array:lib",
        "//arolla/qexpr/operators/math:lib",
    ],
)

cc_binary(
    name = "batch_benchmarks",
    testonly = 1,
    srcs = ["batch_benchmarks.cc"],
    deps = [
        ":float_benchmark_expr_1k_batch",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/serving",
        "@com_google_absl//absl/log:check",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "batch_test",
    srcs = ["batch_test.cc"],
    deps = [
        ":aggregation_dot_product_times_5",
        ":two_long_fibonacci_chains_batch",
        ":x_plus_y_times_5_batch",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "@com_google_googletest//:gtest_main",
    ],
)

benchmark_smoke_test(
    name = "batch_benchmarks_smoke_test",
    binary = ":batch_benchmarks",
)

benchmark_smoke_test(
    name = "batch_scalar_hybrid_benchmarks_smoke_test",
    benchmarks = "/8$",
    binary = ":batch_scalar_hybrid_test",
)

proto_library(
    name = "example_codegen_function_proto",
    testonly = 1,
    srcs = [
        "example_codegen_function.proto",
    ],
)

cc_library(
    name = "example_codegen_function_struct",
    testonly = 1,
    hdrs = [
        "example_codegen_function_struct.h",
    ],
)

py_proto_library(
    name = "example_codegen_function_py_proto",
    testonly = 1,
    deps = [":example_codegen_function_proto"],
)

cc_proto_library(
    name = "example_codegen_function_cc_proto",
    testonly = 1,
    deps = [":example_codegen_function_proto"],
)

proto_library(
    name = "example_codegen_function_extensions_proto",
    testonly = 1,
    srcs = [
        "example_codegen_function_extensions.proto",
    ],
    deps = [
        ":example_codegen_function_proto",
    ],
)

py_proto_library(
    name = "example_codegen_function_extensions_py_proto",
    testonly = 1,
    deps = [":example_codegen_function_extensions_proto"],
)

cc_proto_library(
    name = "example_codegen_function_extensions_cc_proto",
    testonly = 1,
    deps = [":example_codegen_function_extensions_proto"],
)

py_library(
    name = "example_codegen_function_models",
    testonly = 1,
    srcs = [
        "example_codegen_function_models.py",
    ],
    deps = [
        ":example_codegen_function_extensions_py_proto",
        ":example_codegen_function_py_proto",
        "//py:python_path",
        "//py/arolla",
        "//py/arolla/codegen:codegen_function",
    ],
)

input_loader(
    name = "example_codegen_function_struct_input_loader",
    testonly = 1,
    hdrs = ["py/arolla/codegen/testing/example_codegen_function_struct.h"],
    accessors = [
        protopath_accessor("&::a", "/a"),
        protopath_accessor("&::string_field", "/string_field"),
    ],
    input_cls = "::test_namespace::FooInputStruct",
    loader_name = "::test_namespace::FooStructInputLoader",
    deps = [":example_codegen_function_struct"],
)

slot_listener(
    name = "example_codegen_function_struct_vector_slot_listener",
    testonly = 1,
    hdrs = ["py/arolla/codegen/testing/example_codegen_function_struct.h"],
    accessors = [
        protopath_accessor("[:]/&::result", "/result"),
    ],
    output_cls = "std::vector<::test_namespace::ScoringOutputStruct>",
    slot_listener_name = "::test_namespace::ScoringOutputStructVectorSlotListener",
    deps = [":example_codegen_function_struct"],
)

arolla_codegen_functions(
    name = "example_codegen_function",
    testonly = 1,
    functions = {
        "first": call_python_function(
            "arolla.codegen.testing.example_codegen_function_models.first_function_spec",
            args = [],
            deps = [
                ":example_codegen_function_extensions_py_proto",
                ":example_codegen_function_models",
            ],
        ),
        "first_on_struct": call_python_function(
            "arolla.codegen.testing.example_codegen_function_models.first_function_spec_with_struct_input",
            args = [],
            deps = [
                ":example_codegen_function_extensions_py_proto",
                ":example_codegen_function_models",
            ],
        ),
        "second": call_python_function(
            "arolla.codegen.testing.example_codegen_function_models.second_function_spec",
            args = [],
            deps = [":example_codegen_function_models"],
        ),
    },
    deps = [
        ":example_codegen_function_cc_proto",
        ":example_codegen_function_extensions_cc_proto",
        ":example_codegen_function_struct",
        ":example_codegen_function_struct_input_loader",
        ":example_codegen_function_struct_vector_slot_listener",
        "//arolla/dense_array/qtype",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/aggregation:lib",
        "//arolla/qexpr/operators/core:lib",
        "//arolla/qexpr/operators/dense_array:lib",
        "//arolla/qexpr/operators/math:lib",
        "//arolla/qexpr/operators/strings:lib",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "example_codegen_function_test",
    srcs = ["example_codegen_function_test.cc"],
    deps = [
        ":_example_codegen_function_input_loaders",
        ":_example_codegen_function_slot_listeners",
        ":example_codegen_function",
        ":example_codegen_function_cc_proto",
        ":example_codegen_function_extensions_cc_proto",
        ":example_codegen_function_struct",
        "@com_google_googletest//:gtest_main",
    ],
)
