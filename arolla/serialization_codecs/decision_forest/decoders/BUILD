package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "decoders",
    srcs = [
        "decision_forest_decoder.cc",
    ],
    deps = [
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "//arolla/decision_forest",
        "//arolla/decision_forest/expr_operator",
        "//arolla/decision_forest/split_conditions",
        "//arolla/expr",
        "//arolla/qtype",
        "//arolla/serialization:decode",
        "//arolla/serialization_base",
        "//arolla/serialization_codecs/decision_forest:codec_name",
        "//arolla/serialization_codecs/decision_forest:decision_forest_codec_cc_proto",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
    alwayslink = True,
)
