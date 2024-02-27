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
    name = "dense_array",
    srcs = [
        "bitmap.cc",
        "dense_array.cc",
        "edge.cc",
    ],
    hdrs = [
        "bitmap.h",
        "dense_array.h",
        "edge.h",
    ],
    deps = [
        "//arolla/util:status_backport",
        "//arolla/memory",
        "//arolla/util",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

#
# Unittests
#
cc_test(
    name = "dense_array_test",
    srcs = ["dense_array_test.cc"],
    deps = [
        ":dense_array",
        "//arolla/memory",
        "//arolla/util",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "edge_test",
    srcs = ["edge_test.cc"],
    deps = [
        ":dense_array",
        "//arolla/memory",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "bitmap_test",
    srcs = ["bitmap_test.cc"],
    deps = [
        ":dense_array",
        "//arolla/memory",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:distributions",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "bitmap_benchmark",
    testonly = 1,
    srcs = ["bitmap_benchmark.cc"],
    deps = [
        ":dense_array",
        "@com_google_benchmark//:benchmark_main",
    ],
)

benchmark_smoke_test(
    name = "bitmap_benchmark_smoke_test",
    binary = ":bitmap_benchmark",
)

cc_binary(
    name = "factory_benchmarks",
    testonly = 1,
    srcs = ["factory_benchmarks.cc"],
    deps = [
        ":dense_array",
        "//arolla/util",
        "@com_google_benchmark//:benchmark_main",
    ],
)

benchmark_smoke_test(
    name = "factory_benchmarks_smoke_test",
    binary = ":factory_benchmarks",
)

cc_binary(
    name = "edge_benchmarks",
    testonly = 1,
    srcs = ["edge_benchmarks.cc"],
    deps = [
        ":dense_array",
        "//arolla/memory",
        "@com_google_absl//absl/types:span",
        "@com_google_benchmark//:benchmark_main",
    ],
)

benchmark_smoke_test(
    name = "edge_benchmarks_smoke_test",
    binary = ":edge_benchmarks",
)
