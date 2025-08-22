# Copyright 2025 Google LLC
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

# Examples for input loaders and slot listeners.

load("@rules_cc//cc:cc_test.bzl", "cc_test")
load(
    "//arolla/codegen/io:io.bzl",
    "input_loader",
    "proto_fields_accessor",
)

licenses(["notice"])

input_loader(
    name = "main_proto_input_loader",
    testonly = 1,
    hdrs = [
        "arolla/proto/testing/test.pb.h",
        "arolla/proto/testing/test_proto3.pb.h",
    ],
    accessors = [
        proto_fields_accessor(
            "arolla.proto.testing.test_pb2.Root",
            py_deps = ["//arolla/proto/testing:test_py_proto"],
        ),
    ],
    input_cls = "testing_namespace::Root",
    loader_name = "::example_namespace::GetMainProtoInputLoader",
    deps = [
        "//arolla/proto/testing:test_cc_proto",
        "//arolla/proto/testing:test_proto3_cc_proto",
    ],
)

input_loader(
    name = "inner_proto_input_loader",
    testonly = 1,
    hdrs = [
        "arolla/proto/testing/test.pb.h",
        "arolla/proto/testing/test_proto3.pb.h",
    ],
    accessors = [
        proto_fields_accessor(
            "arolla.proto.testing.test_pb2.Inner",
            py_deps = ["//arolla/proto/testing:test_py_proto"],
        ),
    ],
    input_cls = "testing_namespace::Inner",
    loader_name = "::example_namespace::GetInnerProtoInputLoader",
    deps = [
        "//arolla/proto/testing:test_cc_proto",
        "//arolla/proto/testing:test_proto3_cc_proto",
    ],
)

cc_test(
    name = "inner_map_access_test",
    srcs = ["inner_map_access_test.cc"],
    deps = [
        ":inner_proto_input_loader",
        ":main_proto_input_loader",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/io",
        "//arolla/proto/testing:test_cc_proto",
        "//arolla/qexpr/operators/all",
        "//arolla/serving",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)
