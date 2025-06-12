// Copyright 2025 Google LLC
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
#include "arolla/lazy/lazy.h"

#include <memory>
#include <utility>

#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace arolla {
namespace {

// The implementation of LazyValue.
class LazyValue final : public Lazy {
 public:
  explicit LazyValue(TypedValue&& value)
      : Lazy(value.GetType(), FingerprintHasher("::arolla::LazyValue")
                                  .Combine(value.GetFingerprint())
                                  .Finish()),
        value_(std::move(value)) {}

  absl::StatusOr<TypedValue> Get() const final { return value_; }

 private:
  TypedValue value_;
};

// The implementation of LazyFromCallable.
class LazyCallable final : public Lazy {
 public:
  using Callable = absl::AnyInvocable<absl::StatusOr<TypedValue>() const>;

  explicit LazyCallable(QTypePtr value_qtype, Callable&& callable)
      : Lazy(value_qtype, RandomFingerprint()),
        callable_(std::move(callable)) {}

  absl::StatusOr<TypedValue> Get() const final {
    auto result = callable_();
    if (result.ok() && result->GetType() != value_qtype()) {
      return absl::FailedPreconditionError(
          absl::StrFormat("expected a lazy callable to return %s, got %s",
                          value_qtype()->name(), result->GetType()->name()));
    }
    return result;
  }

 private:
  Callable callable_;
};

}  // namespace

LazyPtr MakeLazyFromQValue(TypedValue value) {
  return std::make_shared<LazyValue>(std::move(value));
}

LazyPtr MakeLazyFromCallable(QTypePtr value_qtype,
                             LazyCallable::Callable callable) {
  return std::make_shared<LazyCallable>(value_qtype, std::move(callable));
}

void FingerprintHasherTraits<LazyPtr>::operator()(FingerprintHasher* hasher,
                                                  const LazyPtr& value) const {
  if (value != nullptr) {
    hasher->Combine(value->fingerprint());
  }
}

ReprToken ReprTraits<LazyPtr>::operator()(const LazyPtr& value) const {
  if (value == nullptr) {
    return ReprToken{"lazy[?]{nullptr}"};
  }
  return ReprToken{absl::StrCat("lazy[", value->value_qtype()->name(), "]")};
}

}  // namespace arolla
