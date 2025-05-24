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
#ifndef AROLLA_QTYPE_ANY_QTYPE_H_
#define AROLLA_QTYPE_ANY_QTYPE_H_

#include <any>
#include <cstdint>
#include <functional>
#include <type_traits>
#include <typeinfo>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"

namespace arolla {

// `Any` is a wrapper for std::any that has a QType and can be used inside
// an expression. Can hold any C++ type. The main use case is reducing
// boilerplate in experimental code.
//
//   An example. Experimental operators can use `Data` without defining a
//   separate QType for it:
// struct Data { ... };
// struct ExperimentalOp1 {
//   Any operator()(...) { return Any(Data{...}); }
// };
// struct ExperimentalOp2 {
//   absl::StatusOr<float> operator()(const Any& data, ...) {
//     ASSIGN_OR_RETURN(const Data& d, data.As<Data>());
//     ...
//   }
// };
//
class Any {
 public:
  // Creates a not initialized Any object.
  Any() = default;

  Any(Any&&) = default;
  Any(const Any&) = default;

  Any& operator=(Any&&) = default;
  Any& operator=(const Any&) = default;

  template <typename T,
            typename = std::enable_if_t<!std::is_same_v<Any, std::decay_t<T>>>>
  explicit Any(T&& v) : value_(std::forward<T>(v)) {}

  // Returns value as T or error if `Any` is not initialized or contain a
  // different type.
  template <typename T>
  absl::StatusOr<std::reference_wrapper<const T>> As() const {
    const T* v = std::any_cast<T>(&value_);
    if (v) {
      return *v;
    } else {
      return InvalidCast(typeid(T));
    }
  }

  // Computes random Fingerprint without taking the content into account.
  // Copies of `Any` have the same fingerprint.
  void ArollaFingerprint(FingerprintHasher* hasher) const {
    hasher->Combine(uuid_);
  }

 private:
  std::any value_;
  Fingerprint uuid_ = RandomFingerprint();

  absl::Status InvalidCast(const std::type_info& t) const;
};

AROLLA_DECLARE_SIMPLE_QTYPE(ANY, Any);

}  // namespace arolla

#endif  // AROLLA_QTYPE_ANY_QTYPE_H_
