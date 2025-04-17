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
#include "arolla/expr/expr_attributes.h"

#include <ostream>

#include "arolla/util/fingerprint.h"

namespace arolla::expr {

std::ostream& operator<<(std::ostream& ostream, const ExprAttributes& attr) {
  AbslStringify(ostream, attr);
  return ostream;
}

}  // namespace arolla::expr

namespace arolla {

void FingerprintHasherTraits<expr::ExprAttributes>::operator()(
    FingerprintHasher* hasher, const expr::ExprAttributes& attr) const {
  hasher->Combine(attr.qtype());
  hasher->Combine(attr.qvalue().has_value() ? attr.qvalue()->GetFingerprint()
                                            : Fingerprint{});
}

}  // namespace arolla
