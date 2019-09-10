# A collection of peephole optimizations

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//arolla:internal"],
)

licenses(["notice"])

cc_library(
    name = "peephole_optimizations",
    srcs = [
        "arithmetic.cc",
        "bool.cc",
        "const_with_shape.cc",
        "dict.cc",
        "presence.cc",
        "reduce.cc",
        "short_circuit_where.cc",
        "tuple.cc",
    ],
    hdrs = [
        "arithmetic.h",
        "bool.h",
        "const_with_shape.h",
        "dict.h",
        "presence.h",
        "reduce.h",
        "short_circuit_where.h",
        "tuple.h",
    ],
    deps = [
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "//arolla/expr",
        "//arolla/expr/operators:bootstrap",
        "//arolla/expr/optimization",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/qtype/dict",
        "//arolla/qtype/standard_type_properties",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "short_circuit_where_test",
    srcs = [
        "short_circuit_where_test.cc",
    ],
    deps = [
        ":peephole_optimizations",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status:statusor",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/optimization",
        "//arolla/expr/testing",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "dict_test",
    srcs = [
        "dict_test.cc",
    ],
    deps = [
        ":peephole_optimizations",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status:statusor",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/optimization",
        "//arolla/expr/testing",
        "//arolla/expr/visitors",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/qtype/dict",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "presence_test",
    srcs = ["presence_test.cc"],
    deps = [
        ":peephole_optimizations",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status:statusor",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/optimization",
        "//arolla/expr/testing",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "reduce_test",
    srcs = [
        "reduce_test.cc",
    ],
    deps = [
        ":peephole_optimizations",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/optimization",
        "//arolla/expr/testing",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "bool_test",
    srcs = ["bool_test.cc"],
    deps = [
        ":peephole_optimizations",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status:statusor",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/optimization",
        "//arolla/expr/testing",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "arithmetic_test",
    srcs = ["arithmetic_test.cc"],
    deps = [
        ":peephole_optimizations",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status:statusor",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/optimization",
        "//arolla/expr/testing",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "tuple_test",
    srcs = ["tuple_test.cc"],
    deps = [
        ":peephole_optimizations",
        "@com_google_googletest//:gtest_main",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/optimization",
        "//arolla/expr/testing",
        "//arolla/qexpr/operators/bootstrap",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "const_with_shape_test",
    srcs = ["const_with_shape_test.cc"],
    deps = [
        ":peephole_optimizations",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status:statusor",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/optimization",
        "//arolla/expr/testing",
        "//arolla/memory",
        "//arolla/qexpr/operators/bootstrap",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)
