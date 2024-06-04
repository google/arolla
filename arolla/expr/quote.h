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
#ifndef AROLLA_EXPR_QUOTE_H_
#define AROLLA_EXPR_QUOTE_H_

#include <utility>

#include "absl/base/nullability.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/refcount_ptr.h"
#include "arolla/util/repr.h"

namespace arolla::expr {

// A QValue that stores an Expr inside. It can be used inside other exprs
// as a literal or leaf value without evaluating it right away. This is a bit
// similar to Lisp's "quote".
class ExprQuote {
 public:
  ExprQuote() = default;
  explicit ExprQuote(ExprNodePtr expr) : expr_(std::move(expr)) {}

  bool has_expr() const { return expr_ != nullptr; }

  // Returns the quoted Expr, or an error if !has_expr().
  absl::StatusOr<ExprNodePtr> expr() const;

  const absl::Nullable<RefcountPtr<const ExprNode>>& operator*() const {
    return expr_;
  }
  const absl::Nullable<RefcountPtr<const ExprNode>>* operator->() const {
    return &expr_;
  }

  // Returns fingerprint of the underlying expr, or a dummy value if
  // non-initialized.
  Fingerprint expr_fingerprint() const;

  friend bool operator==(const ExprQuote& lhs, const ExprQuote& rhs) {
    return lhs.expr_fingerprint() == rhs.expr_fingerprint();
  }

  friend bool operator!=(const ExprQuote& lhs, const ExprQuote& rhs) {
    return lhs.expr_fingerprint() != rhs.expr_fingerprint();
  }

  template <typename H>
  friend H AbslHashValue(H h, const ExprQuote& expr_quote) {
    return H::combine(std::move(h), expr_quote.expr_fingerprint());
  }

 private:
  absl::Nullable<RefcountPtr<const ExprNode>> expr_ = nullptr;
};

}  // namespace arolla::expr

namespace arolla {

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(expr::ExprQuote);
AROLLA_DECLARE_REPR(expr::ExprQuote);
AROLLA_DECLARE_SIMPLE_QTYPE(EXPR_QUOTE, expr::ExprQuote);
AROLLA_DECLARE_OPTIONAL_QTYPE(EXPR_QUOTE, expr::ExprQuote);
AROLLA_DECLARE_DENSE_ARRAY_QTYPE(EXPR_QUOTE, expr::ExprQuote);

}  // namespace arolla

#endif  // AROLLA_EXPR_QUOTE_H_
