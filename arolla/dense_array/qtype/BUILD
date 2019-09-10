# QType definitions for Block

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "qtype",
    srcs = [
        "copier.h",
        "types.cc",
    ],
    hdrs = [
        "types.h",
    ],
    deps = [
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "//arolla/dense_array",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "types_test",
    size = "small",
    srcs = ["types_test.cc"],
    deps = [
        ":qtype",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "//arolla/dense_array",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "copier_test",
    size = "small",
    srcs = [
        "copier.h",
        "copier_test.cc",
    ],
    deps = [
        ":qtype",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/types:span",
        "//arolla/dense_array",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/util/testing",
        "//arolla/util:status_backport",
    ],
)
