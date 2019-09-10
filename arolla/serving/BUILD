# Tools for serving Expr models.

load("//arolla/util/testing:testing.bzl", "benchmark_smoke_test")

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "serving",
    hdrs = [
        "embedded_model.h",
        "expr_compiler.h",
        "inplace_expr_compiler.h",
    ],
    deps = [
        ":expr_compiler_optimizer",
        ":serving_lite",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/optimization",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)

cc_library(
    name = "expr_compiler_optimizer",
    srcs = ["expr_compiler_optimizer_initializer.cc"],
    visibility = ["//visibility:private"],
    deps = [
        ":serving_lite",
        "@com_google_absl//absl/status",
        "//arolla/expr/optimization/default",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
    alwayslink = 1,
)

# The main difference from ":serving" is that it doesn't depend on ":expr_compiler_optimizer".
# Allows to reduce binary size by about 100KB by the cost of slower evaluation. Useful only in rare cases.
cc_library(
    name = "serving_lite",
    srcs = [
        "expr_compiler.cc",
    ],
    hdrs = [
        "embedded_model.h",
        "expr_compiler.h",
        "inplace_expr_compiler.h",
    ],
    tags = ["avoid_dep"],
    deps = [
        ":serving_inplace",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/optimization",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)

# The lightest serving target to be used for binaries that only need
# inplace computation of a codegened model (CompileInplaceExprOnStructInput).
# Operator dependencies will be needed to support CompileDynamicExprOnStructInput.
cc_library(
    name = "serving_inplace",
    srcs = [
        "inplace_expr_compiler.cc",
    ],
    hdrs = [
        "embedded_model.h",
        "inplace_expr_compiler.h",
    ],
    tags = ["avoid_dep"],
    deps = [
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "//arolla/expr",
        "//arolla/expr/visitors",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/naming",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "expr_compiler_test",
    srcs = ["expr_compiler_test.cc"],
    deps = [
        ":serving",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "inplace_expr_compiler_test",
    srcs = ["inplace_expr_compiler_test.cc"],
    deps = [
        ":serving_inplace",
        ":serving_lite",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "embedded_model_test",
    srcs = ["embedded_model_test.cc"],
    deps = [
        ":serving",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/io",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "//arolla/util:status_backport",
        "@com_google_benchmark//:benchmark_main",
    ],
)

benchmark_smoke_test(
    name = "embedded_model_test_benchmark_smoke_test",
    binary = ":embedded_model_test",
)

cc_test(
    name = "embedded_model_error_test",
    srcs = ["embedded_model_error_test.cc"],
    deps = [
        ":serving",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/io",
        "//arolla/qexpr/operators/all",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "embedded_model_set_error_test",
    srcs = ["embedded_model_set_error_test.cc"],
    deps = [
        ":serving",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/io",
        "//arolla/qexpr/operators/all",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)
