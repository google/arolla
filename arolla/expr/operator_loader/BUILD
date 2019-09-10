# Library for defining and/or loading expr operators.
#




package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

proto_library(
    name = "operator_loader_proto",
    srcs = [
        "operator_loader.proto",
    ],
    deps = ["//arolla/serialization_base:base_proto"],
)

cc_proto_library(
    name = "operator_loader_cc_proto",
    deps = [
        ":operator_loader_proto",
    ],
)

cc_library(
    name = "operator_loader",
    srcs = [
        "backend_operator.cc",
        "dispatch_operator.cc",
        "dummy_operator.cc",
        "generic_operator.cc",
        "generic_operator_overload_condition.cc",
        "helper.cc",
        "load_operator_package.cc",
        "parameter_qtypes.cc",
        "qtype_constraint.cc",
        "qtype_inference.cc",
        "restricted_lambda_operator.cc",
    ],
    hdrs = [
        "backend_operator.h",
        "dispatch_operator.h",
        "dummy_operator.h",
        "generic_operator.h",
        "generic_operator_overload_condition.h",
        "helper.h",
        "load_operator_package.h",
        "parameter_qtypes.h",
        "qtype_constraint.h",
        "qtype_inference.h",
        "restricted_lambda_operator.h",
    ],
    deps = [
        ":operator_loader_cc_proto",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/serialization:decode",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "parameter_qtypes_test",
    srcs = [
        "parameter_qtypes_test.cc",
    ],
    deps = [
        ":operator_loader",
        "@com_google_googletest//:gtest_main",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
    ],
)

cc_test(
    name = "qtype_constraint_test",
    srcs = [
        "qtype_constraint_test.cc",
    ],
    deps = [
        ":operator_loader",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "qtype_inference_test",
    srcs = [
        "qtype_inference_test.cc",
    ],
    deps = [
        ":operator_loader",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "backend_operator_test",
    srcs = [
        "backend_operator_test.cc",
    ],
    deps = [
        ":operator_loader",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/operators/all",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "dispatch_operator_test",
    srcs = [
        "dispatch_operator_test.cc",
    ],
    deps = [
        ":operator_loader",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "restricted_lambda_operator_test",
    srcs = [
        "restricted_lambda_operator_test.cc",
    ],
    deps = [
        ":operator_loader",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "load_operator_package_test",
    srcs = [
        "load_operator_package_test.cc",
    ],
    deps = [
        ":operator_loader",
        ":operator_loader_cc_proto",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "//arolla/expr",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/serialization:encode",
        "//arolla/serialization_codecs:all",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "dummy_operator_test",
    srcs = ["dummy_operator_test.cc"],
    deps = [
        ":operator_loader",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "//arolla/array/qtype",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "generic_operator_test",
    srcs = ["generic_operator_test.cc"],
    deps = [
        ":operator_loader",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "//arolla/expr",
        "//arolla/expr/operators:bootstrap",
        "//arolla/expr/testing",
        "//arolla/expr/testing:test_operators",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "generic_operator_overload_condition_test",
    srcs = ["generic_operator_overload_condition_test.cc"],
    deps = [
        ":operator_loader",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "//arolla/expr",
        "//arolla/expr/operators:bootstrap",
        "//arolla/expr/testing:test_operators",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)
