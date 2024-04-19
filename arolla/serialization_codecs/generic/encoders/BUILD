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

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

# See tests in py/arolla/s11n/testing
cc_library(
    name = "encoders",
    srcs = [
        "operator_encoder.cc",
        "optional_encoder.cc",
        "scalar_encoder.cc",
        "tuple_encoder.cc",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/expr",
        "//arolla/expr/operator_loader",
        "//arolla/expr/operators/while_loop",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/serialization:encode",
        "//arolla/serialization_base",
        "//arolla/serialization_codecs/generic:codec_name",
        "//arolla/serialization_codecs/generic:operator_codec_cc_proto",
        "//arolla/serialization_codecs/generic:optional_codec_cc_proto",
        "//arolla/serialization_codecs/generic:scalar_codec_cc_proto",
        "//arolla/serialization_codecs/generic:tuple_codec_cc_proto",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
    ],
    alwayslink = True,
)
