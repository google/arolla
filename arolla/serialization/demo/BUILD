load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@bazel_skylib//rules:diff_test.bzl", "diff_test")
load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")
load("@bazel_skylib//:bzl_library.bzl", "bzl_library")

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
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/serialization:decode",
        "//arolla/serialization_base:base_cc_proto",
        "//arolla/serialization_codecs:all_decoders",
        "//arolla/util",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status:statusor",
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
