package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

# See tests in python/arolla/s11n/testing
cc_library(
    name = "decoders",
    srcs = [
        "dict_decoder.cc",
    ],
    deps = [
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "//arolla/expr",
        "//arolla/qtype",
        "//arolla/qtype/dict",
        "//arolla/serialization:decode",
        "//arolla/serialization_base",
        "//arolla/serialization_codecs/dict:codec_name",
        "//arolla/serialization_codecs/dict:dict_codec_cc_proto",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
    alwayslink = True,
)
