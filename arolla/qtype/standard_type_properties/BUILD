# Properties of the standard Arolla QTypes.

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "standard_type_properties",
    srcs = [
        "common_qtype.cc",
        "properties.cc",
    ],
    hdrs = [
        "common_qtype.h",
        "properties.h",
    ],
    deps = [
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "properties_test",
    srcs = ["properties_test.cc"],
    deps = [
        ":standard_type_properties",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "common_qtype_test",
    srcs = ["common_qtype_test.cc"],
    deps = [
        ":standard_type_properties",
        "@com_google_googletest//:gtest_main",
        "//arolla/array/qtype",
        "//arolla/dense_array/qtype",
        "//arolla/qtype",
        "//arolla/util",
    ],
)
