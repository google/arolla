
package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "naming",
    srcs = [
        "policy.cc",
        "protopath_id.cc",
        "table.cc",
    ],
    hdrs = [
        "policy.h",
        "protopath_id.h",
        "table.h",
    ],
    deps = [
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "table_test",
    srcs = ["table_test.cc"],
    deps = [
        ":naming",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "policy_test",
    srcs = ["policy_test.cc"],
    deps = [
        ":naming",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "protopath_id_test",
    srcs = ["protopath_id_test.cc"],
    deps = [
        ":naming",
        "@com_google_googletest//:gtest_main",
        "//arolla/util:status_backport",
    ],
)
