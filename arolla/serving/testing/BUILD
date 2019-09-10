# Tests & benchmarks for Arolla serving.

load("//arolla/util/testing:testing.bzl", "benchmark_smoke_test")

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//arolla:internal"],
)

licenses(["notice"])

cc_binary(
    name = "thread_safety_benchmarks",
    testonly = 1,
    srcs = ["thread_safety_benchmarks.cc"],
    deps = [
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/synchronization",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/io",
        "//arolla/qexpr/operators/all",
        "//arolla/serving",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_benchmark//:benchmark_main",
    ],
)

benchmark_smoke_test(
    name = "thread_safety_benchmarks_smoke_test",
    benchmarks = "<10>",
    binary = ":thread_safety_benchmarks",
)
