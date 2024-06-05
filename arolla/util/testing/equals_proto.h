// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#ifndef AROLLA_UTIL_TESTING_EQUALS_PROTO_H_
#define AROLLA_UTIL_TESTING_EQUALS_PROTO_H_

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/text_format.h"
#include "google/protobuf/util/message_differencer.h"

namespace arolla::testing {

// A simple version of the EqualsProto predicate for Arolla.
template <typename TypeProto>
::testing::AssertionResult EqualsProto(const TypeProto& actual_proto,
                                       absl::string_view expected_proto_text) {
  using ::google::protobuf::TextFormat;
  using ::google::protobuf::util::MessageDifferencer;
  TypeProto expected_proto;
  // Converted to std::string because OSS version of ParseFromString doesn't
  // work with string_view.
  if (!TextFormat::ParseFromString(std::string(expected_proto_text),  // NOLINT
                                   &expected_proto)) {
    return ::testing::AssertionFailure()
           << "could not parse proto: " << expected_proto_text;
  }
  MessageDifferencer differencer;
  std::string differences;
  differencer.ReportDifferencesToString(&differences);
  if (!differencer.Compare(expected_proto, actual_proto)) {
    return ::testing::AssertionFailure() << "the protos are different:\n"
                                         << differences;
  }
  return ::testing::AssertionSuccess();
}

// A simple version of the EqualsProto matcher for Arolla.
inline auto EqualsProto(absl::string_view expected_proto_text) {
  return ::testing::Truly([expected_proto_text = std::string(
                               expected_proto_text)](const auto& actual_proto) {
    return EqualsProto(actual_proto, expected_proto_text);
  });
}

}  // namespace arolla::testing

#endif  // AROLLA_UTIL_TESTING_EQUALS_PROTO_H_
