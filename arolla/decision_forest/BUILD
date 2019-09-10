
package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "decision_forest",
    srcs = ["decision_forest.cc"],
    hdrs = [
        "decision_forest.h",
        "split_condition.h",
    ],
    deps = [
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "decision_forest_test",
    srcs = ["decision_forest_test.cc"],
    deps = [
        ":decision_forest",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "//arolla/decision_forest/split_conditions",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util/testing",
    ],
)
