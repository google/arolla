load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@bazel_skylib//rules:diff_test.bzl", "diff_test")
load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")
load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load("//arolla/util/testing:testing.bzl", "benchmark_smoke_test")


package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "qtype",
    srcs = [
        "any_qtype.cc",
        "base_types.cc",
        "derived_qtype.cc",
        "named_field_qtype.cc",
        "optional_qtype.cc",
        "qtype.cc",
        "shape_qtype.cc",
        "simple_qtype.cc",
        "slice_qtype.cc",
        "tuple_qtype.cc",
        "typed_ref.cc",
        "typed_slot.cc",
        "typed_value.cc",
        "unspecified_qtype.cc",
        "weak_qtype.cc",
    ],
    hdrs = [
        "any_qtype.h",
        "base_types.h",
        "derived_qtype.h",
        "named_field_qtype.h",
        "optional_qtype.h",
        "qtype.h",
        "qtype_traits.h",
        "shape_qtype.h",
        "simple_qtype.h",
        "slice_qtype.h",
        "tuple_qtype.h",
        "typed_ref.h",
        "typed_slot.h",
        "typed_value.h",
        "unspecified_qtype.h",
        "weak_qtype.h",
    ],
    deps = [
        "//arolla/util:status_backport",
        "//arolla/memory",
        "//arolla/util",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:distributions",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "qtype_test",
    srcs = [
        "optional_qtype_test.cc",
        "qtype_test.cc",
        "simple_qtype_test.cc",
        "slice_qtype_test.cc",
        "tuple_qtype_test.cc",
    ],
    deps = [
        ":qtype",
        ":qtype_test_utils",
        "//arolla/util:status_backport",
        "//arolla/memory",
        "//arolla/qtype/testing",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/hash:hash_testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "any_qtype_test",
    srcs = [
        "any_qtype_test.cc",
    ],
    deps = [
        ":qtype",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "derived_qtype_test",
    srcs = [
        "derived_qtype_test.cc",
    ],
    deps = [
        ":qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "unspecified_qtype_test",
    srcs = [
        "unspecified_qtype_test.cc",
    ],
    deps = [
        ":qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "typed_slot_test",
    srcs = ["typed_slot_test.cc"],
    deps = [
        ":qtype",
        "//arolla/memory",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "qtype_test_utils",
    testonly = 1,
    hdrs = ["qtype_test_utils.h"],
    deps = [
        ":qtype",
        "//arolla/memory",
        "@com_google_googletest//:gtest",
    ],
)

cc_binary(
    name = "qtype_benchmark",
    testonly = 1,
    srcs = ["qtype_benchmark.cc"],
    deps = [
        ":qtype",
        "//arolla/memory",
        "//arolla/util",
        "@com_google_benchmark//:benchmark_main",
    ],
)

# There is no tests in qtype_benchmark.cc. It only checks that the benchmark is compilable.
benchmark_smoke_test(
    name = "qtype_benchmark_smoke_test",
    binary = ":qtype_benchmark",
)

cc_test(
    name = "typed_value_test",
    srcs = ["typed_value_test.cc"],
    deps = [
        ":qtype",
        "//arolla/memory",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "shape_qtype_test",
    srcs = ["shape_qtype_test.cc"],
    deps = [
        ":qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "qtype_traits_test",
    srcs = ["qtype_traits_test.cc"],
    deps = [
        ":qtype",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "weak_qtype_test",
    srcs = ["weak_qtype_test.cc"],
    deps = [
        ":qtype",
        "//arolla/array/qtype",
        "//arolla/memory",
        "//arolla/qtype/standard_type_properties",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)
