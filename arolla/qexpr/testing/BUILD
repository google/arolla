# Tools for testing QExpr

load("//arolla/util/testing:testing.bzl", "benchmark_smoke_test")

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//arolla:internal"],
)

licenses(["notice"])

cc_library(
    name = "testing",
    testonly = 1,
    srcs = [
        "benchmarks.cc",
    ],
    hdrs = [
        "benchmarks.h",
        "operator_fixture.h",
    ],
    tags = ["keep_dep"],
    deps = [
        "@com_google_benchmark//:benchmark",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util:status_backport",
    ],
)

cc_binary(
    name = "test_benchmarks",
    testonly = 1,
    srcs = ["test_benchmarks.cc"],
    deps = [
        ":testing",
        "//arolla/qexpr",
        "//arolla/qtype",
        "@com_google_benchmark//:benchmark_main",
    ],
)

benchmark_smoke_test(
    name = "benchmarks_smoke_test",
    binary = ":test_benchmarks",
)

cc_test(
    name = "operator_fixture_test",
    srcs = ["operator_fixture_test.cc"],
    deps = [
        ":testing",
        "@com_google_googletest//:gtest_main",
        "//arolla/codegen/qexpr/testing:test_operators",
        "//arolla/codegen/qexpr/testing:test_operators_lib",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)
