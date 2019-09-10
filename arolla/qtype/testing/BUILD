# QType-related tools for testing

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//arolla:internal"],
)

licenses(["notice"])

cc_library(
    name = "testing",
    testonly = 1,
    hdrs = [
        "qtype.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "qtype_test",
    srcs = ["qtype_test.cc"],
    deps = [
        ":testing",
        "@com_google_googletest//:gtest_main",
        "//arolla/qtype",
        "//arolla/util/testing",
    ],
)

cc_library(
    name = "dummy_types",
    testonly = 1,
    srcs = [
        "dummy_types.cc",
    ],
    hdrs = [
        "dummy_types.h",
    ],
    deps = [
        "//arolla/qtype",
        "//arolla/util",
    ],
)
