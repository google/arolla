


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
        "@com_google_absl//absl/strings:string_view",
        "//arolla/memory",
        "//arolla/util",
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
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/strings:string_view",
        "//arolla/memory",
        "//arolla/util",
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
