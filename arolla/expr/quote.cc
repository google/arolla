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
#include "arolla/expr/quote.h"

#include "absl/log/check.h"
#include "absl/numeric/int128.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace arolla::expr {

// arolla.bytes(uuid.uuid4().bytes).fingerprint
constexpr Fingerprint kEmptyQuoteHash{
    absl::MakeUint128(0x5466dba2e1989659, 0x6f2834ee88b8b08b)};

absl::StatusOr<ExprNodePtr> ExprQuote::expr() const {
  if (expr_ == nullptr) {
    return absl::InvalidArgumentError("uninitialized ExprQuote");
  }
  return expr_;
}

Fingerprint ExprQuote::expr_fingerprint() const {
  return expr_ != nullptr ? expr_->fingerprint() : kEmptyQuoteHash;
}

}  // namespace arolla::expr

namespace arolla {

void FingerprintHasherTraits<expr::ExprQuote>::operator()(
    FingerprintHasher* hasher, const expr::ExprQuote& value) const {
  hasher->Combine(absl::string_view("::arolla::expr::ExprQuote"),
                  value.expr_fingerprint());
}

ReprToken ReprTraits<expr::ExprQuote>::operator()(
    const expr::ExprQuote& value) const {
  if (!value.has_expr()) {
    return ReprToken{"ExprQuote(nullptr)"};
  }
  return ReprToken{absl::StrFormat(
      "ExprQuote('%s')", absl::Utf8SafeCHexEscape(ToDebugString(*value)))};
}

AROLLA_DEFINE_SIMPLE_QTYPE(EXPR_QUOTE, expr::ExprQuote);
AROLLA_DEFINE_OPTIONAL_QTYPE(EXPR_QUOTE, expr::ExprQuote);
AROLLA_DEFINE_DENSE_ARRAY_QTYPE(EXPR_QUOTE, expr::ExprQuote);

}  // namespace arolla
