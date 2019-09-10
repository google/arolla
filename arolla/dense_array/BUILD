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
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "//arolla/memory",
        "//arolla/util",
        "//arolla/util:status_backport",
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
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/strings:string_view",
        "//arolla/memory",
        "//arolla/util",
    ],
)

cc_test(
    name = "edge_test",
    srcs = ["edge_test.cc"],
    deps = [
        ":dense_array",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/types:span",
        "//arolla/memory",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "bitmap_test",
    srcs = ["bitmap_test.cc"],
    deps = [
        ":dense_array",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:distributions",
        "@com_google_absl//absl/types:span",
        "//arolla/memory",
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
        "@com_google_absl//absl/types:span",
        "//arolla/memory",
        "@com_google_benchmark//:benchmark_main",
    ],
)

benchmark_smoke_test(
    name = "edge_benchmarks_smoke_test",
    binary = ":edge_benchmarks",
)
