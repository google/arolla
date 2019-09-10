# Array and DenseArray ops with shared (templated) implementations. Specific implementations should
# be kept in the respective packages.

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//arolla:internal"],
)

licenses(["notice"])

cc_library(
    name = "array_like",
    srcs = [],
    hdrs = ["edge_ops.h"],
    deps = [
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util:status_backport",
    ],
)
