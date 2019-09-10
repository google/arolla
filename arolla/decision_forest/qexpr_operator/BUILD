package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "qexpr_operator",
    srcs = [
        "compile_decision_forest_operator.cc",
    ],
    deps = [
        ":lib",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:str_format",
        "//arolla/decision_forest/expr_operator",
        "//arolla/expr/eval",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
    alwayslink = 1,
)

cc_library(
    name = "lib",
    srcs = [
        "batched_operator.cc",
        "pointwise_operator.cc",
    ],
    hdrs = [
        "batched_operator.h",
        "pointwise_operator.h",
    ],
    deps = [
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "//arolla/decision_forest",
        "//arolla/decision_forest/batched_evaluation",
        "//arolla/decision_forest/pointwise_evaluation",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "pointwise_test",
    srcs = ["pointwise_test.cc"],
    deps = [
        ":lib",
        "@com_google_googletest//:gtest_main",
        "//arolla/decision_forest",
        "//arolla/decision_forest/split_conditions",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
    ],
)

cc_test(
    name = "batched_test",
    srcs = ["batched_test.cc"],
    deps = [
        ":lib",
        "@com_google_googletest//:gtest_main",
        "//arolla/decision_forest",
        "//arolla/decision_forest/split_conditions",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
    ],
)
