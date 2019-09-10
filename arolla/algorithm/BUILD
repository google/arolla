# Algorithms library

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//arolla:internal"],
)

licenses(["notice"])

cc_library(
    name = "algorithm",
    srcs = [
        "control_flow_graph.cc",
    ],
    hdrs = [
        "control_flow_graph.h",
    ],
    deps = [
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "control_flow_graph_test",
    srcs = ["control_flow_graph_test.cc"],
    deps = [
        ":algorithm",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "//arolla/util/testing",
    ],
)
