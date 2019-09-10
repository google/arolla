


package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

proto_library(
    name = "operator_codec_proto",
    srcs = [
        "operator_codec.proto",
    ],
    deps = ["//arolla/serialization_base:base_proto"],
)

proto_library(
    name = "scalar_codec_proto",
    srcs = [
        "scalar_codec.proto",
    ],
    deps = ["//arolla/serialization_base:base_proto"],
)

proto_library(
    name = "optional_codec_proto",
    srcs = [
        "optional_codec.proto",
    ],
    deps = ["//arolla/serialization_base:base_proto"],
)

proto_library(
    name = "tuple_codec_proto",
    srcs = [
        "tuple_codec.proto",
    ],
    deps = ["//arolla/serialization_base:base_proto"],
)

cc_proto_library(
    name = "operator_codec_cc_proto",
    deps = [
        ":operator_codec_proto",
    ],
)

cc_proto_library(
    name = "scalar_codec_cc_proto",
    deps = [
        ":scalar_codec_proto",
    ],
)

cc_proto_library(
    name = "optional_codec_cc_proto",
    deps = [
        ":optional_codec_proto",
    ],
)

cc_proto_library(
    name = "tuple_codec_cc_proto",
    deps = [
        ":tuple_codec_proto",
    ],
)

cc_library(
    name = "codec_name",
    hdrs = ["codec_name.h"],
    visibility = [":__subpackages__"],
    deps = ["@com_google_absl//absl/strings:string_view"],
)
