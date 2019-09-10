# This library introduces a "lazy" type representing a value with
# deferred/on-demand computation.

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//arolla:internal"],
)

licenses(["notice"])

cc_library(
    name = "lazy",
    srcs = [
        "lazy.cc",
        "lazy_qtype.cc",
    ],
    hdrs = [
        "lazy.h",
        "lazy_qtype.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/synchronization",
        "//arolla/qtype",
        "//arolla/util",
    ],
)

cc_test(
    name = "lazy_test",
    srcs = [
        "lazy_test.cc",
    ],
    deps = [
        ":lazy",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "lazy_qtype_test",
    srcs = [
        "lazy_qtype_test.cc",
    ],
    deps = [
        ":lazy",
        "@com_google_googletest//:gtest_main",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "integration_test",
    srcs = [
        "integration_test.cc",
    ],
    deps = [
        ":lazy",
        "@com_google_googletest//:gtest_main",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/operators/all",
        "//arolla/lazy/operators",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)
