# Tools to create while loop operators.

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "while_loop",
    srcs = [
        "while_loop.cc",
        "while_loop_impl.cc",
        "while_loop_impl.h",
    ],
    hdrs = [
        "while_loop.h",
    ],
    deps = [
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "//arolla/expr",
        "//arolla/expr/operators:bootstrap",
        "//arolla/expr/visitors",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "while_loop_test",
    srcs = ["while_loop_test.cc"],
    deps = [
        ":while_loop",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "while_loop_impl_test",
    srcs = [
        "while_loop_impl.h",
        "while_loop_impl_test.cc",
    ],
    deps = [
        ":while_loop",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/qexpr/operators/all",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)
