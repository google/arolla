load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@bazel_skylib//rules:diff_test.bzl", "diff_test")
load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")
load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
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
        "//arolla/util:status_backport",
        "//arolla/dense_array",
        "//arolla/memory",
        "//arolla/util",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "util_test",
    srcs = ["util_test.cc"],
    deps = [
        ":ops",
        "//arolla/dense_array",
        "//arolla/memory",
        "//arolla/util",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "multi_edge_util_test",
    srcs = ["multi_edge_util_test.cc"],
    deps = [
        ":ops",
        "//arolla/dense_array",
        "//arolla/memory",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "dense_ops_test",
    srcs = ["dense_ops_test.cc"],
    deps = [
        ":ops",
        "//arolla/dense_array",
        "//arolla/memory",
        "//arolla/qexpr/operators/math:lib",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "dense_group_ops_test",
    srcs = ["dense_group_ops_test.cc"],
    deps = [
        ":ops",
        "//arolla/dense_array",
        "//arolla/dense_array/testing",
        "//arolla/memory",
        "//arolla/qexpr/operators/testing:lib",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
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
        "//arolla/dense_array",
        "//arolla/dense_array/testing",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/util",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/random",
        "@com_google_benchmark//:benchmark_main",
    ],
)

benchmark_smoke_test(
    name = "benchmarks_smoke_test",
    benchmarks = "BM_DenseOp_AddFull/32",
    binary = ":benchmarks",
)
