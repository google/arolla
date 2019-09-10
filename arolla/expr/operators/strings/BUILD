# Expr-level operators on strings

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "strings",
    srcs = [
        "register_operators.cc",
        "string_operators.cc",
    ],
    hdrs = [
        "register_operators.h",
        "string_operators.h",
    ],
    deps = [
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "//arolla/expr",
        "//arolla/expr/operators",
        "//arolla/expr/operators:bootstrap",
        "//arolla/qtype",
        "//arolla/qtype/standard_type_properties",
        "//arolla/qtype/strings",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "string_operators_test",
    srcs = ["string_operators_test.cc"],
    deps = [
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators:bootstrap",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)
