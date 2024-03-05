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
#ifndef AROLLA_UTIL_BYTES_H_
#define AROLLA_UTIL_BYTES_H_

#include <ostream>
#include <string>
#include <utility>

#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace arolla {

// Bytes class represents an immutable collection of bytes.
class Bytes {
 public:
  Bytes() = default;
  // Specialize constructors from different types for better performance.
  explicit Bytes(const char* s) : data_(s) {}
  explicit Bytes(absl::string_view view) : data_(view) {}
  explicit Bytes(const std::string& s) : data_(s) {}
  explicit Bytes(std::string&& s) : data_(std::move(s)) {}
  explicit Bytes(const absl::Cord& cord) : data_(cord) {}

  Bytes& operator=(const char* s) {
    data_ = s;
    return *this;
  }
  Bytes& operator=(absl::string_view s) {
    // data_ = s;  doesn't work for some compilers
    data_.assign(s.data(), s.size());
    return *this;
  }
  Bytes& operator=(const std::string& s) {
    data_ = s;
    return *this;
  }
  Bytes& operator=(std::string&& s) {
    data_ = std::move(s);
    return *this;
  }
  Bytes& operator=(const absl::Cord& cord) {
    data_ = std::string(cord);
    return *this;
  }

  Bytes(const Bytes&) = default;
  Bytes& operator=(const Bytes&) = default;
  Bytes(Bytes&& rhs) = default;
  Bytes& operator=(Bytes&& rhs) = default;

  absl::string_view view() const { return data_; }

  /* implicit */
  operator absl::string_view() const {  // NOLINT(google-explicit-constructor)
    return data_;
  }

 private:
  std::string data_;
};

inline std::ostream& operator<<(std::ostream& stream, const Bytes& value) {
  return stream << "Bytes{" << value.view() << "}";
}

inline bool operator==(const Bytes& lhs, const Bytes& rhs) {
  return lhs.view() == rhs.view();
}

inline bool operator!=(const Bytes& lhs, const Bytes& rhs) {
  return !(lhs == rhs);
}

inline bool operator<(const Bytes& lhs, const Bytes& rhs) {
  return lhs.view() < rhs.view();
}

inline bool operator<=(const Bytes& lhs, const Bytes& rhs) {
  return lhs.view() <= rhs.view();
}

inline bool operator>(const Bytes& lhs, const Bytes& rhs) {
  return lhs.view() > rhs.view();
}

inline bool operator>=(const Bytes& lhs, const Bytes& rhs) {
  return lhs.view() >= rhs.view();
}

AROLLA_DECLARE_REPR(Bytes);
AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(Bytes);

}  // namespace arolla

#endif  // AROLLA_UTIL_BYTES_H_
