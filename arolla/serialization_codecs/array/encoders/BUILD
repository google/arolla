package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "encoders",
    srcs = [
        "array_encoder.cc",
    ],
    deps = [
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/dense_array/qtype",
        "//arolla/qtype",
        "//arolla/serialization:encode",
        "//arolla/serialization_base",
        "//arolla/serialization_codecs/array:array_codec_cc_proto",
        "//arolla/serialization_codecs/array:codec_name",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
    alwayslink = True,
)

# See tests in python/arolla/s11n/testing
cc_test(
    name = "array_encoder_test",
    srcs = ["array_encoder_test.cc"],
    deps = [
        ":encoders",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status:statusor",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/qtype",
        "//arolla/serialization:encode",
        "//arolla/serialization_codecs/array:array_codec_cc_proto",
        "//arolla/serialization_codecs/dense_array/encoders",
        "//arolla/serialization_codecs/generic/encoders",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)
