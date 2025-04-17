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
#ifndef AROLLA_EXPR_EXPR_ATTRIBUTES_H_
#define AROLLA_EXPR_EXPR_ATTRIBUTES_H_

#include <iosfwd>
#include <optional>
#include <ostream>
#include <utility>

#include "absl/log/check.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"

namespace arolla::expr {

// A helper class that stores statically computed attributes of an expression
// node.
//
// NOTE: The official abbreviation for "ExprAttributes" is "Attr" and "sequence
// of ExprAttributes" is "Attrs".
//
// We often need to handle sequences of attributes corresponding to the operator
// inputs, and it's convenient to have a way to distinguish a set of attributes
// from a sequence of sets.
class ExprAttributes {
 public:
  ExprAttributes() noexcept = default;

  // Movable.
  ExprAttributes(ExprAttributes&&) noexcept = default;
  ExprAttributes& operator=(ExprAttributes&&) noexcept = default;

  // Copyable.
  ExprAttributes(const ExprAttributes&) noexcept = default;
  ExprAttributes& operator=(const ExprAttributes&) noexcept = default;

  explicit ExprAttributes(const QType* /*nullable*/ qtype) : qtype_(qtype) {}

  explicit ExprAttributes(TypedRef qvalue)
      : qtype_(qvalue.GetType()), qvalue_(qvalue) {}

  explicit ExprAttributes(TypedValue&& qvalue)
      : qtype_(qvalue.GetType()), qvalue_(std::move(qvalue)) {}

  explicit ExprAttributes(const TypedValue& qvalue)
      : qtype_(qvalue.GetType()), qvalue_(qvalue) {}

  ExprAttributes(QTypePtr qtype, TypedValue&& qvalue)
      : qtype_(qtype), qvalue_(std::move(qvalue)) {
    DCHECK_EQ(qtype_, qvalue_->GetType());
  }

  ExprAttributes(QTypePtr qtype, const TypedValue& qvalue)
      : qtype_(qtype), qvalue_(qvalue) {
    DCHECK_EQ(qtype_, qvalue_->GetType());
  }

  ExprAttributes(const QType* /*nullable*/ qtype,
                 std::optional<TypedValue>&& qvalue)
      : qtype_(qtype), qvalue_(std::move(qvalue)) {
    if (qvalue_.has_value()) {
      DCHECK_EQ(qtype_, qvalue_->GetType());
    }
  }

  ExprAttributes(const QType* /*nullable*/ qtype,
                 const std::optional<TypedValue>& qvalue)
      : qtype_(qtype), qvalue_(qvalue) {
    if (qvalue_.has_value()) {
      DCHECK_EQ(qtype_, qvalue_->GetType());
    }
  }

  const QType* /*nullable*/ qtype() const { return qtype_; }
  const std::optional<TypedValue>& qvalue() const { return qvalue_; }

  bool IsEmpty() const { return qtype_ == nullptr; }

  bool IsIdenticalTo(const ExprAttributes& other) const {
    if (qtype_ != other.qtype_) {
      return false;
    }
    if (qvalue_.has_value() != other.qvalue_.has_value()) {
      return false;
    }
    if (!qvalue_.has_value() || !other.qvalue_.has_value()) {
      return true;
    }
    return qvalue_->GetFingerprint() == other.qvalue_->GetFingerprint();
  }

  bool IsSubsetOf(const ExprAttributes& other) const {
    if (qtype_ != nullptr && qtype_ != other.qtype_) {
      return false;
    }
    if (!qvalue_.has_value()) {
      return true;
    }
    return (other.qvalue_.has_value() &&
            qvalue_->GetFingerprint() == other.qvalue_->GetFingerprint());
  }

 private:
  const QType* /*nullable*/ qtype_ = nullptr;
  std::optional<TypedValue> qvalue_;
};

template <typename Sink>
void AbslStringify(Sink& sink, const ExprAttributes& attr) {
  if (attr.qvalue()) {
    absl::Format(&sink, "Attr(qvalue=%s)", attr.qvalue()->Repr());
  } else if (attr.qtype()) {
    absl::Format(&sink, "Attr(qtype=%s)", attr.qtype()->name());
  } else {
    absl::Format(&sink, "Attr{}");
  }
}

std::ostream& operator<<(std::ostream& ostream, const ExprAttributes& attr);

}  // namespace arolla::expr

namespace arolla {
AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(expr::ExprAttributes);
}  // namespace arolla

#endif  // AROLLA_EXPR_EXPR_ATTRIBUTES_H_
