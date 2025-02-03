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
#ifndef AROLLA_UTIL_TEXT_H_
#define AROLLA_UTIL_TEXT_H_

#include <ostream>
#include <string>
#include <utility>

#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace arolla {

// Text class represents an immutable UTF-8 text string.
class Text {
 public:
  Text() = default;
  // Specialize constructors from different types for better performance.
  explicit Text(const char* s) : data_(s) {}
  explicit Text(absl::string_view view) : data_(view) {}
  explicit Text(const std::string& s) : data_(s) {}
  explicit Text(std::string&& s) : data_(std::move(s)) {}
  explicit Text(const absl::Cord& cord) : data_(std::move(cord)) {}

  Text& operator=(const char* s) {
    data_ = s;
    return *this;
  }
  Text& operator=(absl::string_view s) {
    // data_ = s;  doesn't work for some compilers
    data_.assign(s.data(), s.size());
    return *this;
  }
  Text& operator=(const std::string& s) {
    data_ = s;
    return *this;
  }
  Text& operator=(std::string&& s) {
    data_ = std::move(s);
    return *this;
  }
  Text& operator=(const absl::Cord& cord) {
    data_ = std::string(cord);
    return *this;
  }

  absl::string_view view() const { return data_; }

  /* implicit */
  operator absl::string_view() const {  // NOLINT(google-explicit-constructor)
    return data_;
  }

  friend bool operator==(const Text& lhs, const Text& rhs) {
    return lhs.data_ == rhs.data_;
  }

  friend bool operator==(const char* lhs, const Text& rhs) {
    return lhs == rhs.data_;
  }

  friend bool operator==(const Text& lhs, const char* rhs) {
    return lhs.data_ == rhs;
  }

  friend bool operator!=(const Text& lhs, const Text& rhs) {
    return !(lhs == rhs);
  }

  friend bool operator<(const Text& lhs, const Text& rhs) {
    return lhs.data_ < rhs.data_;
  }

  friend bool operator<=(const Text& lhs, const Text& rhs) {
    return lhs.data_ <= rhs.data_;
  }

  friend bool operator>(const Text& lhs, const Text& rhs) {
    return lhs.data_ > rhs.data_;
  }

  friend bool operator>=(const Text& lhs, const Text& rhs) {
    return lhs.data_ >= rhs.data_;
  }

 private:
  std::string data_;
};

inline std::ostream& operator<<(std::ostream& stream, const Text& value) {
  return stream << "Text{" << value.view() << "}";
}

AROLLA_DECLARE_REPR(Text);
AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(Text);

}  // namespace arolla

#endif  // AROLLA_UTIL_TEXT_H_
