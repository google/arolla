load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@bazel_skylib//rules:diff_test.bzl", "diff_test")
load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")
load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load("//arolla/util/testing:testing.bzl", "benchmark_smoke_test")

package(
    default_applicable_licenses = [
    ],
    default_visibility = [
        "//arolla/decision_forest/qexpr_operator:__pkg__",
    ],
)

licenses(["notice"])

cc_library(
    name = "batched_evaluation",
    srcs = ["batched_forest_evaluator.cc"],
    hdrs = ["batched_forest_evaluator.h"],
    deps = [
        "//arolla/util:status_backport",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/decision_forest",
        "//arolla/decision_forest/pointwise_evaluation",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/util",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "batched_forest_evaluator_test",
    srcs = ["batched_forest_evaluator_test.cc"],
    deps = [
        ":batched_evaluation",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/decision_forest",
        "//arolla/decision_forest/split_conditions",
        "//arolla/decision_forest/testing",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:distributions",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "benchmarks",
    testonly = 1,
    srcs = ["benchmarks.cc"],
    deps = [
        ":batched_evaluation",
        "//arolla/util:status_backport",
        "//arolla/decision_forest",
        "//arolla/decision_forest/testing",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/status",
        "@com_google_benchmark//:benchmark_main",
    ],
)

benchmark_smoke_test(
    name = "benchmarks_smoke_test",
    benchmarks = "BM_Prod_IntervalSplits_100/7/1000",
    binary = ":benchmarks",
)
