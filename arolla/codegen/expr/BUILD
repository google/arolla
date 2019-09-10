# Library to help generating code for evaluating `ExprNode`.

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//arolla:internal"],
)

licenses(["notice"])

cc_library(
    name = "expr",
    srcs = [
        "codegen_operator.cc",
        "optimizations.cc",
        "types.cc",
    ],
    hdrs = [
        "codegen_operator.h",
        "optimizations.h",
        "types.h",
    ],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
        "@com_google_double_conversion//:double-conversion",
        "//arolla/algorithm",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/optimization",
        "//arolla/expr/optimization/default",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "codegen_operator_test",
    srcs = ["codegen_operator_test.cc"],
    deps = [
        ":expr",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status:statusor",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/qexpr/operators/all",
        "//arolla/qexpr/operators/math:operators_metadata",
        "//arolla/qtype",
        "//arolla/util",
    ],
)

# Test that CppName and CppLiteralRepr produce correct C++ code.

# Binary generating C++ code for a test.
cc_binary(
    name = "scalar_types_test_gen",
    testonly = 1,
    srcs = ["scalar_types_test_gen.cc"],
    deps = [
        ":expr",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "//arolla/codegen/dict",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/qtype",
        "//arolla/qtype/dict",
        "//arolla/util",
    ],
)

genrule(
    name = "scalar_types_test_gen_rule",
    testonly = 1,
    outs = ["scalar_types_gen_test.cc"],
    cmd = "$(execpath :scalar_types_test_gen) >  \"$@\"",
    tools = [":scalar_types_test_gen"],
)

cc_test(
    name = "scalar_types_gen_test",
    srcs = ["scalar_types_gen_test.cc"],
    deps = [
        "@com_google_googletest//:gtest_main",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/qtype/dict",
        "//arolla/util",
    ],
)

cc_test(
    name = "types_test",
    srcs = ["types_test.cc"],
    deps = [
        ":expr",
        "@com_google_googletest//:gtest_main",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "optimizations_test",
    srcs = ["optimizations_test.cc"],
    deps = [
        ":expr",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "//arolla/expr",
        "//arolla/util/testing",
    ],
)
