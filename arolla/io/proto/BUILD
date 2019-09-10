# Proto related .

load("//arolla/util/testing:testing.bzl", "benchmark_smoke_test")

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "proto",
    srcs = [
        "proto_input_loader.cc",
    ],
    hdrs = [
        "proto_input_loader.h",
    ],
    deps = [
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:protoc",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/proto",
        "//arolla/proto/reflection",
        "//arolla/qtype",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "proto_input_loader_test",
    srcs = ["proto_input_loader_test.cc"],
    deps = [
        ":proto",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protoc",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/io",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/naming",
        "//arolla/proto",
        "//arolla/proto:test_cc_proto",
        "//arolla/qtype",
        "//arolla/util",
    ],
)

cc_test(
    name = "proto_input_loader_errors_test",
    srcs = ["proto_input_loader_errors_test.cc"],
    deps = [
        ":proto",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_protobuf//:protoc",
        "@com_google_protobuf//:protobuf_lite",
        "//arolla/dense_array/qtype",
        "//arolla/io",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/proto:test_cc_proto",
        "//arolla/proto:test_proto3_cc_proto",
        "//arolla/qtype",
        "//arolla/util/testing",
    ],
)

cc_binary(
    name = "proto_input_loader_benchmarks",
    testonly = 1,
    srcs = ["proto_input_loader_benchmarks.cc"],
    deps = [
        ":proto",
        "//arolla/io/proto/testing",
        "//arolla/proto:test_cc_proto",
        "@com_google_benchmark//:benchmark_main",
    ],
)

benchmark_smoke_test(
    name = "proto_input_loader_benchmarks_smoke_test",
    binary = ":proto_input_loader_benchmarks",
)
