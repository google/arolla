# Custom operator support for arolla/expr/eval.

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//arolla:internal"],
)

licenses(["notice"])

cc_library(
    name = "eval_extensions",
    srcs = [
        "compile_core_map_operator.cc",
        "prepare_core_map_operator.cc",
        "prepare_core_map_operator.h",
        "seq_map_operator.cc",
        "seq_map_operator.h",
        "seq_reduce_operator.cc",
        "seq_reduce_operator.h",
    ],
    deps = [
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/operators:bootstrap",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/sequence",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
    alwayslink = True,
)

cc_test(
    name = "core_map_operator_test",
    srcs = [
        "core_map_operator_test.cc",
        "prepare_core_map_operator.h",
    ],
    deps = [
        ":eval_extensions",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "//arolla/array/qtype",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/eval:test_utils",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "seq_map_operator_test",
    srcs = [
        "seq_map_operator.h",
        "seq_map_operator_test.cc",
    ],
    deps = [
        ":eval_extensions",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/eval:test_utils",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/sequence",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "seq_reduce_operator_test",
    srcs = [
        "seq_reduce_operator.h",
        "seq_reduce_operator_test.cc",
    ],
    deps = [
        ":eval_extensions",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/eval:test_utils",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/sequence",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)
