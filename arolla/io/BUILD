
package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "io",
    srcs = [
        "accessor_helpers.h",
        "input_loader.cc",
        "slot_listener.cc",
        "string_slot_listener.cc",
        "typed_refs_input_loader.cc",
        "wildcard_input_loader.cc",
    ],
    hdrs = [
        "accessors_input_loader.h",
        "accessors_slot_listener.h",
        "chain_slot_listener.h",
        "delegating_input_loader.h",
        "delegating_slot_listener.h",
        "inplace_slot_builder.h",
        "input_loader.h",
        "slot_listener.h",
        "span_input_loader.h",
        "string_slot_listener.h",
        "tuple_input_loader.h",
        "typed_refs_input_loader.h",
        "wildcard_input_loader.h",
    ],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/log:die_if_null",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "input_loader_test",
    srcs = [
        "input_loader_test.cc",
    ],
    deps = [
        ":io",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "slot_listener_test",
    srcs = [
        "slot_listener_test.cc",
    ],
    deps = [
        ":io",
        "@com_google_googletest//:gtest_main",
        "//arolla/memory",
        "//arolla/qtype",
    ],
)

cc_test(
    name = "chain_slot_listener_test",
    srcs = [
        "chain_slot_listener_test.cc",
    ],
    deps = [
        ":io",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "accessor_helpers_test",
    srcs = [
        "accessor_helpers.h",
        "accessor_helpers_test.cc",
    ],
    deps = [
        ":io",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "accessors_input_loader_test",
    srcs = [
        "accessors_input_loader_test.cc",
    ],
    deps = [
        ":io",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "accessors_slot_listener_test",
    srcs = [
        "accessors_slot_listener_test.cc",
    ],
    deps = [
        ":io",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "inplace_slot_builder_test",
    srcs = [
        "inplace_slot_builder_test.cc",
    ],
    deps = [
        ":io",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "delegating_input_loader_test",
    srcs = ["delegating_input_loader_test.cc"],
    deps = [
        ":io",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/qtype",
    ],
)

cc_test(
    name = "delegating_slot_listener_test",
    srcs = ["delegating_slot_listener_test.cc"],
    deps = [
        ":io",
        "@com_google_googletest//:gtest_main",
        "//arolla/memory",
        "//arolla/qtype",
    ],
)

cc_test(
    name = "wildcard_input_loader_test",
    srcs = [
        "wildcard_input_loader_test.cc",
    ],
    deps = [
        ":io",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:optional",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "string_slot_listener_test",
    srcs = [
        "string_slot_listener_test.cc",
    ],
    deps = [
        ":io",
        "@com_google_googletest//:gtest_main",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
    ],
)

cc_test(
    name = "span_input_loader_test",
    srcs = [
        "span_input_loader_test.cc",
    ],
    deps = [
        ":io",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/types:span",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "tuple_input_loader_test",
    srcs = [
        "tuple_input_loader_test.cc",
    ],
    deps = [
        ":io",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "typed_refs_input_loader_test",
    srcs = [
        "typed_refs_input_loader_test.cc",
    ],
    deps = [
        ":io",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/types:span",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util/testing",
    ],
)
