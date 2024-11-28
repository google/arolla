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
#ifndef AROLLA_LAZY_LAZY_H_
#define AROLLA_LAZY_LAZY_H_

#include <memory>

#include "absl//functional/any_invocable.h"
#include "absl//status/statusor.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace arolla {

// A "lazy" type representing a value with deferred/on-demand computation.
//
// NOTE: There is no promise that the value will be cached after the first
// evaluation.
class Lazy {
 public:
  // Non-copyable.
  Lazy(const Lazy&) = delete;
  Lazy& operator=(const Lazy&) = delete;

  // Returns the value qtype.
  QTypePtr value_qtype() const { return value_qtype_; }

  // Returns the fingerprint of the "lazy" object.
  const Fingerprint& fingerprint() const { return fingerprint_; }

  // Returns the value.
  virtual absl::StatusOr<TypedValue> Get() const = 0;

  virtual ~Lazy() = default;

 protected:
  Lazy(QTypePtr value_qtype, const Fingerprint& fingerprint)
      : value_qtype_(value_qtype), fingerprint_(fingerprint) {}

 private:
  QTypePtr value_qtype_;
  Fingerprint fingerprint_;
};

using LazyPtr = std::shared_ptr<const Lazy>;

// Returns a "lazy" object that acts as a proxy for a value.
LazyPtr MakeLazyFromQValue(TypedValue value);

// Returns a "lazy" object that acts as a proxy for a callable object.
LazyPtr MakeLazyFromCallable(
    QTypePtr value_qtype,
    absl::AnyInvocable<absl::StatusOr<TypedValue>() const> callable);

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(LazyPtr);
AROLLA_DECLARE_REPR(LazyPtr);

}  // namespace arolla

#endif  // AROLLA_LAZY_LAZY_H_
