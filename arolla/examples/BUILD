# Examples of C++ API.

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//arolla:internal"],
)

licenses(["notice"])

cc_test(
    name = "agg_execution_test",
    srcs = ["agg_execution_test.cc"],
    deps = [
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status:statusor",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/operators/all",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)
