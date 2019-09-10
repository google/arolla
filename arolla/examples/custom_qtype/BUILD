# Example of custom QType definition.
#
# See python/arolla/examples/custom_qtype/ for the Python part.

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//arolla:internal"],
)

licenses(["notice"])

cc_library(
    name = "complex",
    srcs = ["complex.cc"],
    hdrs = ["complex.h"],
    deps = [
        "@com_google_absl//absl/strings:str_format",
        "//arolla/qtype",
        "//arolla/util",
    ],
)

cc_test(
    name = "complex_test",
    srcs = ["complex_test.cc"],
    deps = [
        ":complex",
        "@com_google_googletest//:gtest_main",
        "//arolla/qtype",
        "//arolla/util",
    ],
)

cc_library(
    name = "complex_operators",
    srcs = ["complex_operators.cc"],
    deps = [
        ":complex",
        "@com_google_absl//absl/status",
        "//arolla/optools",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
    alwayslink = 1,
)
