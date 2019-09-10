package(
    default_applicable_licenses = [
    ],
    default_testonly = 1,
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "testing",
    testonly = 1,
    srcs = [
        "benchmark_util.cc",
    ],
    hdrs = [
        "benchmark_util.h",
    ],
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/proto:test_benchmark_extension_cc_proto",
        "//arolla/proto:test_cc_proto",
        "//arolla/qtype",
        "@com_google_benchmark//:benchmark_main",
    ],
)
