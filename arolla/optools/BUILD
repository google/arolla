# Tools for defining operators in C++.

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "optools",
    srcs = ["optools.cc"],
    hdrs = ["optools.h"],
    deps = [
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "//arolla/expr",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "optools_test",
    srcs = ["optools_test.cc"],
    deps = [
        ":optools",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "//arolla/expr",
        "//arolla/expr/testing",
        "//arolla/qtype",
        "//arolla/util/testing",
    ],
)
