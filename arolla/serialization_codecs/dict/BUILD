


package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

proto_library(
    name = "dict_codec_proto",
    srcs = [
        "dict_codec.proto",
    ],
    deps = ["//arolla/serialization_base:base_proto"],
)

cc_proto_library(
    name = "dict_codec_cc_proto",
    deps = [
        ":dict_codec_proto",
    ],
)

cc_library(
    name = "codec_name",
    hdrs = ["codec_name.h"],
    visibility = [":__subpackages__"],
    deps = ["@com_google_absl//absl/strings:string_view"],
)
