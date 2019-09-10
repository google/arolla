# Allows registration of default codecs.

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "all",
    tags = ["keep_dep"],
    deps = [
        ":all_decoders",
        "//arolla/serialization_codecs/array/encoders",
        "//arolla/serialization_codecs/decision_forest/encoders",
        "//arolla/serialization_codecs/dense_array/encoders",
        "//arolla/serialization_codecs/dict/encoders",
        "//arolla/serialization_codecs/generic/encoders",
    ],
)

cc_library(
    name = "all_decoders",
    tags = ["keep_dep"],
    deps = [
        "//arolla/serialization_codecs/array/decoders",
        "//arolla/serialization_codecs/decision_forest/decoders",
        "//arolla/serialization_codecs/dense_array/decoders",
        "//arolla/serialization_codecs/dict/decoders",
        "//arolla/serialization_codecs/generic/decoders",
    ],
)
