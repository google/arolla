# literal and type extensions for python/arolla/expr/codegen

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "dict",
    srcs = ["dict_codegen_literal.cc"],
    deps = [
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "//arolla/codegen/expr",
        "//arolla/qtype",
        "//arolla/qtype/dict",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
    alwayslink = 1,
)

cc_test(
    name = "dict_codegen_literal_test",
    srcs = ["dict_codegen_literal_test.cc"],
    deps = [
        ":dict",
        "@com_google_googletest//:gtest_main",
        "//arolla/codegen/expr",
        "//arolla/qtype",
        "//arolla/qtype/dict",
        "//arolla/util/testing",
    ],
)
