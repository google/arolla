
package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "qtype",
    srcs = ["qtype.cc"],
    hdrs = ["qtype.h"],
    deps = [
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/jagged_shape/array",
        "//arolla/jagged_shape/qtype",
        "//arolla/qtype",
        "//arolla/util",
    ],
)

cc_test(
    name = "qtype_test",
    srcs = ["qtype_test.cc"],
    deps = [
        ":qtype",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/jagged_shape/array",
        "//arolla/jagged_shape/dense_array",
        "//arolla/jagged_shape/dense_array/qtype",
        "//arolla/jagged_shape/qtype",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)
