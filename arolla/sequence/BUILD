package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "sequence",
    srcs = [
        "mutable_sequence.cc",
        "sequence.cc",
        "sequence_qtype.cc",
    ],
    hdrs = [
        "mutable_sequence.h",
        "sequence.h",
        "sequence_qtype.h",
    ],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
    ],
)

cc_test(
    name = "mutable_sequence_test",
    srcs = [
        "mutable_sequence_test.cc",
    ],
    deps = [
        ":sequence",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "//arolla/qtype",
        "//arolla/util",
    ],
)

cc_test(
    name = "sequence_test",
    srcs = [
        "sequence_test.cc",
    ],
    deps = [
        ":sequence",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/types:span",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "sequence_qtype_test",
    srcs = [
        "sequence_qtype_test.cc",
    ],
    deps = [
        ":sequence",
        "@com_google_googletest//:gtest_main",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)
