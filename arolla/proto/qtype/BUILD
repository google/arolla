# Utilities for proto descriptors and QTypes.

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "qtype",
    srcs = [
        "proto_qtype_map.cc",
    ],
    hdrs = [
        "proto_qtype_map.h",
    ],
    deps = [
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protoc",
        "//arolla/proto",
        "//arolla/qtype",
        "//arolla/util",
    ],
)

cc_test(
    name = "proto_qtype_map_test",
    srcs = ["proto_qtype_map_test.cc"],
    deps = [
        ":qtype",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_protobuf//:protoc",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)
