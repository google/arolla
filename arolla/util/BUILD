# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

load("//arolla/util/testing:testing.bzl", "benchmark_smoke_test")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

# By design, these utilities are small, and have minimal dependencies, so
# they are bundled into a single library. If you want to add something
# heavyweight, this isn't the place for it.
cc_library(
    name = "util",
    srcs = [
        "algorithms.cc",
        "binary_search.cc",
        "bits.cc",
        "bytes.cc",
        "demangle.cc",
        "fingerprint.cc",
        "init_arolla.cc",
        "init_arolla_internal.cc",
        "preallocated_buffers.cc",
        "repr.cc",
        "status.cc",
        "string.cc",
        "text.cc",
        "threading.cc",
        "unit.cc",
    ],
    hdrs = [
        "algorithms.h",
        "api.h",
        "binary_search.h",
        "bits.h",
        "bytes.h",
        "demangle.h",
        "fast_dynamic_downcast_final.h",
        "fingerprint.h",
        "init_arolla.h",
        "init_arolla_internal.h",
        "is_bzero_constructible.h",
        "iterator.h",
        "lru_cache.h",
        "map.h",
        "memory.h",
        "meta.h",
        "operator_name.h",
        "preallocated_buffers.h",
        "refcount.h",
        "refcount_ptr.h",
        "repr.h",
        "status.h",
        "string.h",
        "struct_field.h",
        "switch_index.h",
        "text.h",
        "thread_safe_shared_ptr.h",
        "threading.h",
        "threadlocal.h",
        "types.h",
        "unit.h",
        "view_types.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/util:status_backport",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/numeric:int128",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
        "@com_google_cityhash//:cityhash",
        "@com_google_double_conversion//:double-conversion",
    ],
)

cc_library(
    name = "status_backport",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    textual_hdrs = [
        "status_macros_backport.h",
    ],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
    ],
)

#
# Unittests
#
cc_test(
    name = "algorithms_test",
    size = "small",
    srcs = ["algorithms_test.cc"],
    deps = [
        ":util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "benchmarks",
    testonly = 1,
    srcs = ["benchmarks.cc"],
    deps = [
        ":util",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

benchmark_smoke_test(
    name = "benchmarks_smoke_test",
    binary = ":benchmarks",
)

cc_test(
    name = "binary_search_test",
    size = "small",
    srcs = ["binary_search_test.cc"],
    deps = [
        ":util",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "bits_test",
    size = "small",
    srcs = ["bits_test.cc"],
    deps = [
        ":util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "bytes_test",
    srcs = ["bytes_test.cc"],
    deps = [
        ":util",
        "//arolla/util/testing",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "demangle_test",
    srcs = ["demangle_test.cc"],
    deps = [
        ":util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "fast_dynamic_downcast_final_test",
    size = "small",
    srcs = ["fast_dynamic_downcast_final_test.cc"],
    deps = [
        ":util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "fast_dynamic_downcast_final_benchmark",
    testonly = 1,
    srcs = ["fast_dynamic_downcast_final_benchmark.cc"],
    deps = [
        ":util",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "fingerprint_test",
    size = "small",
    srcs = ["fingerprint_test.cc"],
    deps = [
        ":util",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "fingerprint_benchmark",
    testonly = 1,
    srcs = ["fingerprint_benchmark.cc"],
    deps = [
        ":util",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "iterator_test",
    size = "small",
    srcs = ["iterator_test.cc"],
    deps = [
        ":util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "map_test",
    srcs = ["map_test.cc"],
    deps = [
        ":util",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "memory_test",
    srcs = [
        "memory_test.cc",
    ],
    deps = [
        ":util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "meta_test",
    srcs = ["meta_test.cc"],
    deps = [
        ":util",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "operator_name_test",
    size = "small",
    srcs = ["operator_name_test.cc"],
    deps = [
        ":util",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "status_test",
    srcs = ["status_test.cc"],
    deps = [
        ":util",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "struct_field_test",
    srcs = ["struct_field_test.cc"],
    deps = [
        ":util",
        "//arolla/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "unit_test",
    srcs = ["unit_test.cc"],
    deps = [
        ":util",
        "//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "repr_test",
    srcs = ["repr_test.cc"],
    deps = [
        ":util",
        "//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "text_test",
    srcs = ["text_test.cc"],
    deps = [
        ":util",
        "//arolla/util/testing",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "string_test",
    srcs = ["string_test.cc"],
    deps = [
        ":util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "switch_index_test",
    srcs = ["switch_index_test.cc"],
    deps = [
        ":util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "thread_safe_shared_ptr_test",
    srcs = ["thread_safe_shared_ptr_test.cc"],
    deps = [
        ":util",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "status_macros_backport_test",
    srcs = [
        "status_macros_backport_test.cc",
    ],
    deps = [
        ":status_backport",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "preallocated_buffers_test",
    srcs = [
        "preallocated_buffers_test.cc",
    ],
    deps = [
        ":util",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "refcount_test",
    srcs = [
        "refcount_test.cc",
    ],
    deps = [
        ":util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "refcount_benchmark",
    testonly = 1,
    srcs = ["refcount_benchmark.cc"],
    deps = [
        ":util",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "refcount_ptr_test",
    srcs = [
        "refcount_ptr_test.cc",
    ],
    deps = [
        ":util",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/log:check",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "refcount_ptr_benchmark",
    testonly = 1,
    srcs = ["refcount_ptr_benchmark.cc"],
    deps = [
        ":util",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "lru_cache_test",
    srcs = [
        "lru_cache_test.cc",
    ],
    deps = [
        ":util",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)
