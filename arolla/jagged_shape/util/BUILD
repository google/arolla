
package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//arolla/jagged_shape:__subpackages__"],
)

licenses(["notice"])

cc_library(
    name = "util",
    srcs = ["repr.cc"],
    hdrs = ["repr.h"],
    deps = [
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "//arolla/util",
    ],
)

cc_test(
    name = "repr_test",
    srcs = ["repr_test.cc"],
    deps = [
        ":util",
        "@com_google_googletest//:gtest_main",
    ],
)
