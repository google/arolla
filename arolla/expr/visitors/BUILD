# Visitors operating on Arolla expressions.

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "visitors",
    srcs = [
        "substitution.cc",
    ],
    hdrs = [
        "substitution.h",
    ],
    deps = [
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "//arolla/expr",
        "//arolla/util",
    ],
)

cc_test(
    name = "substitution_test",
    srcs = [
        "substitution_test.cc",
    ],
    deps = [
        ":visitors",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/container:flat_hash_map",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/qexpr/operators/all",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)
