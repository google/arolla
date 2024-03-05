load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@bazel_skylib//rules:diff_test.bzl", "diff_test")
load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")
load("@bazel_skylib//:bzl_library.bzl", "bzl_library")



package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "proto",
    srcs = ["types.cc"],
    hdrs = ["types.h"],
    deps = [
        "//arolla/memory",
        "//arolla/util",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:string_view",
    ],
)

proto_library(
    name = "io_proto",
    srcs = ["io.proto"],
)

cc_proto_library(
    name = "io_cc_proto",
    deps = [":io_proto"],
)

load("@com_google_protobuf//:protobuf.bzl", "py_proto_library")
py_proto_library(
    name = "io_py_pb2",
    srcs = ["io.proto"],
)

cc_test(
    name = "types_test",
    srcs = ["types_test.cc"],
    deps = [
        ":proto",
        "//arolla/memory",
        "//arolla/util",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

proto_library(
    name = "test_proto",
    testonly = 1,
    srcs = ["test.proto"],
    deps = [
        ":test_proto3",
    ],
)

cc_proto_library(
    name = "test_cc_proto",
    testonly = 1,
    deps = [":test_proto"],
)

py_proto_library(
    name = "test_py_proto",
    testonly = 1,
    srcs = ["test.proto"],
    deps = [":test_proto3_py_proto"],
)

proto_library(
    name = "test_proto3",
    testonly = 1,
    srcs = ["test_proto3.proto"],
)

cc_proto_library(
    name = "test_proto3_cc_proto",
    testonly = 1,
    deps = [":test_proto3"],
)

py_proto_library(
    name = "test_proto3_py_proto",
    testonly = 1,
    srcs = ["test_proto3.proto"],
)

proto_library(
    name = "test_proto_extension",
    testonly = 1,
    srcs = ["test_extension.proto"],
    deps = [
        ":test_proto",
    ],
)

cc_proto_library(
    name = "test_extension_cc_proto",
    testonly = 1,
    deps = [":test_proto_extension"],
)

py_proto_library(
    name = "test_extension_py_proto",
    testonly = 1,
    srcs = ["test_extension.proto"],
    deps = [":test_py_proto"],
)

proto_library(
    name = "test_proto_benchmark_extension",
    testonly = 1,
    srcs = ["test_benchmark_extension.proto"],
    deps = [
        ":test_proto",
    ],
)

cc_proto_library(
    name = "test_benchmark_extension_cc_proto",
    testonly = 1,
    deps = [":test_proto_benchmark_extension"],
)

py_proto_library(
    name = "test_benchmark_extension_py_proto",
    testonly = 1,
    srcs = ["test_benchmark_extension.proto"],
    deps = [":test_py_proto"],
)
