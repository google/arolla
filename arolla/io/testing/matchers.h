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
#ifndef AROLLA_IO_TESTING_MATCHERS_H_
#define AROLLA_IO_TESTING_MATCHERS_H_

#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "arolla/io/input_loader.h"
#include "arolla/qtype/qtype.h"

namespace arolla::testing {
namespace matchers_impl {

struct NameTypeFormatter {
  void operator()(std::string* out,
                  const std::pair<std::string, QTypePtr>& nametype) const {
    absl::StrAppendFormat(out, "%s: %s", nametype.first,
                          nametype.second->name());
  }
};

class InputLoaderSupportsMatcher {
 public:
  using is_gtest_matcher = void;

  explicit InputLoaderSupportsMatcher(
      std::vector<std::pair<std::string, QTypePtr>> expected_types)
      : expected_types_(std::move(expected_types)) {}

  template <typename T>
  bool MatchAndExplain(const InputLoaderPtr<T>& input_loader,
                       std::ostream* os) const {
    return MatchAndExplain(*input_loader, os);
  }

  template <typename T>
  bool MatchAndExplain(const InputLoader<T>& input_loader,
                       std::ostream* os) const {
    for (const auto& [name, expected_type] : expected_types_) {
      const QType* type = input_loader.GetQTypeOf(name);
      if (type == nullptr) {
        if (os != nullptr) {
          *os << "does not support input \"" << name << "\"";
          auto suggestions = input_loader.SuggestAvailableNames();
          if (!suggestions.empty()) {
            *os << " (supported: "
                << absl::StrJoin(input_loader.SuggestAvailableNames(), ", ")
                << ")";
          }
        }
        return false;
      }
      if (type != expected_type) {
        if (os != nullptr) {
          *os << "unexpected type for \"" << name << "\": expected "
              << expected_type->name() << ", got " << type->name();
        }
        return false;
      }
    }
    if (os != nullptr) {
      *os << "supports all the requested inputs";
    }
    return true;
  }

  void DescribeTo(std::ostream* os) const {
    *os << "can load all of "
        << absl::StrJoin(expected_types_, ", ", NameTypeFormatter());
  }

  void DescribeNegationTo(std::ostream* os) const {
    *os << "cannot load any of "
        << absl::StrJoin(expected_types_, ", ", NameTypeFormatter());
  }

 private:
  std::vector<std::pair<std::string, QTypePtr>> expected_types_;
};

}  // namespace matchers_impl

// GMock matcher that validates that the input loader can load the expected
// inputs of the expected types.
inline matchers_impl::InputLoaderSupportsMatcher InputLoaderSupports(
    std::vector<std::pair<std::string, QTypePtr>> expected_types) {
  return matchers_impl::InputLoaderSupportsMatcher(std::move(expected_types));
}

}  // namespace arolla::testing

#endif  // AROLLA_IO_TESTING_MATCHERS_H_
