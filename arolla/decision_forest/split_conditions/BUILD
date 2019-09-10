package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "split_conditions",
    hdrs = [
        "interval_split_condition.h",
        "set_of_values_split_condition.h",
    ],
    deps = [
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "//arolla/decision_forest",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
    ],
)

cc_test(
    name = "split_conditions_test",
    srcs = ["split_conditions_test.cc"],
    deps = [
        ":split_conditions",
        "@com_google_googletest//:gtest_main",
        "//arolla/decision_forest",
        "//arolla/qtype",
        "//arolla/util",
    ],
)
