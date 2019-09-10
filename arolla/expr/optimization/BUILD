# Infrastructure to perform optimizations on expressions.

package(
    default_applicable_licenses = [
    ],
    default_visibility = [
        "//arolla:internal",
    ],
)

licenses(["notice"])

cc_library(
    name = "optimization",
    srcs = [
        "optimizer.cc",
        "peephole_optimizer.cc",
    ],
    hdrs = [
        "optimizer.h",
        "peephole_optimizer.h",
    ],
    visibility = [
        "//arolla:internal",
        "//video/content_analysis/match/siberia/match_analysis:__subpackages__",
    ],
    deps = [
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "//arolla/expr",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "peephole_optimizer_test",
    srcs = ["peephole_optimizer_test.cc"],
    deps = [
        ":optimization",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/hash:hash_testing",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/expr/visitors",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/util",
        "//arolla/util/testing",
        "//arolla/util:status_backport",
    ],
)

cc_test(
    name = "optimizer_test",
    srcs = ["optimizer_test.cc"],
    deps = [
        ":optimization",
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
