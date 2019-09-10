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
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
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
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "batched_forest_evaluator_test",
    srcs = ["batched_forest_evaluator_test.cc"],
    deps = [
        ":batched_evaluation",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:distributions",
        "@com_google_absl//absl/status:statusor",
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
    ],
)

cc_binary(
    name = "benchmarks",
    testonly = 1,
    srcs = ["benchmarks.cc"],
    deps = [
        ":batched_evaluation",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/status",
        "//arolla/decision_forest",
        "//arolla/decision_forest/testing",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_benchmark//:benchmark_main",
    ],
)

benchmark_smoke_test(
    name = "benchmarks_smoke_test",
    benchmarks = "BM_Prod_IntervalSplits_100/7/1000",
    binary = ":benchmarks",
)
