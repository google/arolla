package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "reflection",
    srcs = ["reader.cc"],
    hdrs = ["reader.h"],
    deps = [
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:protoc",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/proto",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "reader_test",
    srcs = ["reader_test.cc"],
    deps = [
        ":reflection",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_protobuf//:protoc",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/proto",
        "//arolla/proto:test_cc_proto",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "reader_errors_test",
    srcs = ["reader_errors_test.cc"],
    deps = [
        ":reflection",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_protobuf//:protoc",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/proto:test_cc_proto",
        "//arolla/qtype",
        "//arolla/util/testing",
    ],
)
