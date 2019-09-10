# Base serialization facilities for arolla expressions and values.
#
# Decoder and encoder defined here know virtually nothing about actual value
# serialization. The decoder explicitly depends on ExprOperator qtype, but
# provides no serialization format for ExprOperators.
#
# All concrete types and serialization formats need to be provided externally.



package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

proto_library(
    name = "base_proto",
    srcs = [
        "base.proto",
    ],
)

cc_proto_library(
    name = "base_cc_proto",
    deps = [
        ":base_proto",
    ],
)

cc_library(
    name = "serialization_base",
    srcs = [
        "decode.cc",
        "encode.cc",
    ],
    hdrs = [
        "decode.h",
        "encode.h",
    ],
    deps = [
        ":base_cc_proto",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "//arolla/expr",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "decode_test",
    srcs = [
        "decode_test.cc",
    ],
    deps = [
        ":base_cc_proto",
        ":serialization_base",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "//arolla/expr",
        "//arolla/expr/testing",
        "//arolla/expr/testing:test_operators",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "encode_test",
    srcs = [
        "encode_test.cc",
    ],
    deps = [
        ":base_cc_proto",
        ":serialization_base",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "//arolla/expr",
        "//arolla/expr/testing:test_operators",
        "//arolla/qtype",
        "//arolla/util/testing",
    ],
)
