load("//arolla/util/testing:testing.bzl", "benchmark_smoke_test")

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "ops",
    srcs = [
        "universal_dense_op.h",
    ],
    hdrs = [
        "dense_group_ops.h",
        "dense_ops.h",
        "multi_edge_util.h",
        "util.h",
    ],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "//arolla/dense_array",
        "//arolla/memory",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "util_test",
    srcs = ["util_test.cc"],
    deps = [
        ":ops",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "//arolla/dense_array",
        "//arolla/memory",
        "//arolla/util",
    ],
)

cc_test(
    name = "multi_edge_util_test",
    srcs = ["multi_edge_util_test.cc"],
    deps = [
        ":ops",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "//arolla/dense_array",
        "//arolla/memory",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "dense_ops_test",
    srcs = ["dense_ops_test.cc"],
    deps = [
        ":ops",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "//arolla/dense_array",
        "//arolla/memory",
        "//arolla/qexpr/operators/math:lib",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "dense_group_ops_test",
    srcs = ["dense_group_ops_test.cc"],
    deps = [
        ":ops",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:string_view",
        "//arolla/dense_array",
        "//arolla/dense_array/testing",
        "//arolla/memory",
        "//arolla/qexpr/operators/testing:lib",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_binary(
    name = "benchmarks",
    testonly = 1,
    srcs = [
        "benchmarks.cc",
    ],
    deps = [
        ":ops",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/random",
        "//arolla/dense_array",
        "//arolla/dense_array/testing",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/util",
        "@com_google_benchmark//:benchmark_main",
    ],
)

benchmark_smoke_test(
    name = "benchmarks_smoke_test",
    benchmarks = "BM_DenseOp_AddFull/32",
    binary = ":benchmarks",
)
