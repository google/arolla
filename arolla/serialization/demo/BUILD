package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_binary(
    name = "demo",
    srcs = ["main.cc"],
    deps = [
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status:statusor",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/serialization:decode",
        "//arolla/serialization_base:base_cc_proto",
        "//arolla/serialization_codecs:all_decoders",
        "//arolla/util",
    ],
)

sh_test(
    name = "demo_smoke_test",
    srcs = [":demo"],
    args = [
        "$(execpaths {})".format(f)
        for f in glob(
            ["testdata/**"],
            exclude = ["testdata/old_codec_name_input.pb"],
        )
    ],
    data = glob(["testdata/**"]),
)
