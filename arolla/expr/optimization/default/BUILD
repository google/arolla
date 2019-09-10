# TODO: describe this package.

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "default",
    srcs = ["default_optimizer.cc"],
    hdrs = ["default_optimizer.h"],
    deps = [
        "@com_google_absl//absl/status:statusor",
        "//arolla/expr/optimization",
        "//arolla/expr/optimization/peephole_optimizations",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)
