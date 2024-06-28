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
#ifndef AROLLA_QEXPR_QEXPR_OPERATOR_SIGNATURE_H_
#define AROLLA_QEXPR_QEXPR_OPERATOR_SIGNATURE_H_

#include <ostream>
#include <vector>

#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/qtype/qtype.h"

namespace arolla {

class QExprOperatorSignature {
 public:
  static const QExprOperatorSignature* Get(
      absl::Span<const QTypePtr> input_types, QTypePtr output_type);

  // Gets the types of this function's inputs.
  absl::Span<const QTypePtr> input_types() const { return input_types_; }

  // Gets the type of this function's output.
  QTypePtr output_type() const { return output_type_; }

 private:
  QExprOperatorSignature(absl::Span<const QTypePtr> input_types,
                         QTypePtr output_type);

  // Non-copyable, non-movable.
  QExprOperatorSignature(const QExprOperatorSignature&) = delete;
  QExprOperatorSignature& operator=(const QExprOperatorSignature&) = delete;

  std::vector<QTypePtr> input_types_;
  QTypePtr output_type_;
};

template <typename Sink>
void AbslStringify(Sink& sink, const QExprOperatorSignature* signature) {
  absl::Format(&sink, "%s->%s", FormatTypeVector(signature->input_types()),
               JoinTypeNames({signature->output_type()}));
}

inline std::ostream& operator<<(std::ostream& ostream,
                                const QExprOperatorSignature* signature) {
  return ostream << FormatTypeVector(signature->input_types()) << "->"
                 << JoinTypeNames({signature->output_type()});
}

}  // namespace arolla

#endif  // AROLLA_QEXPR_QEXPR_OPERATOR_SIGNATURE_H_
