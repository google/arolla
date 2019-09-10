# ::arolla::Array is an immutable array with support of very sparse data.

load("//arolla/util/testing:testing.bzl", "benchmark_smoke_test")

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "array",
    srcs = [
        "array.cc",
        "array_util.cc",
        "edge.cc",
        "id_filter.cc",
    ],
    hdrs = [
        "array.h",
        "array_util.h",
        "edge.h",
        "group_op.h",
        "id_filter.h",
        "multi_edge_util.h",
        "ops_util.h",
        "pointwise_op.h",
    ],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "//arolla/dense_array",
        "//arolla/dense_array/ops",
        "//arolla/memory",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "array_test",
    srcs = ["array_test.cc"],
    deps = [
        ":array",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/strings:string_view",
        "//arolla/dense_array",
        "//arolla/memory",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "array_util_test",
    srcs = ["array_util_test.cc"],
    deps = [
        ":array",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/types:span",
        "//arolla/dense_array",
        "//arolla/memory",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "edge_test",
    srcs = ["edge_test.cc"],
    deps = [
        ":array",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/types:span",
        "//arolla/dense_array",
        "//arolla/memory",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "multi_edge_util_test",
    srcs = ["multi_edge_util_test.cc"],
    deps = [
        ":array",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "//arolla/memory",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "id_filter_test",
    srcs = ["id_filter_test.cc"],
    deps = [
        ":array",
        "@com_google_googletest//:gtest_main",
        "//arolla/memory",
    ],
)

cc_test(
    name = "pointwise_op_test",
    srcs = ["pointwise_op_test.cc"],
    deps = [
        ":array",
        "@com_google_googletest//:gtest_main",
        "//arolla/dense_array",
        "//arolla/memory",
    ],
)

cc_test(
    name = "group_op_test",
    srcs = ["group_op_test.cc"],
    deps = [
        ":array",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:string_view",
        "//arolla/dense_array",
        "//arolla/memory",
        "//arolla/qexpr/operators/testing:lib",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "ops_util_test",
    srcs = ["ops_util_test.cc"],
    deps = [
        ":array",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "//arolla/dense_array",
        "//arolla/memory",
        "//arolla/util",
    ],
)

cc_binary(
    name = "benchmarks",
    testonly = 1,
    srcs = ["benchmarks.cc"],
    deps = [
        ":array",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:distributions",
        "@com_google_absl//absl/types:span",
        "//arolla/dense_array",
        "//arolla/dense_array/ops",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/aggregation:lib",
        "//arolla/util",
        "@com_google_benchmark//:benchmark_main",
    ],
)

benchmark_smoke_test(
    name = "benchmarks_smoke_test",
    benchmarks = "BM_WithIds_DenseToSparse",
    binary = ":benchmarks",
)
