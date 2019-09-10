# Mapping QType definitions

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "dict",
    srcs = [
        "dict_types.cc",
    ],
    hdrs = [
        "dict_types.h",
    ],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/synchronization",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "dict_types_test",
    srcs = ["dict_types_test.cc"],
    deps = [
        ":dict",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "//arolla/dense_array/qtype",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)
