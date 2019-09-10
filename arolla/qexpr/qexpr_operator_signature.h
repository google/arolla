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

#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/qtype/qtype.h"

namespace arolla {

class QExprOperatorSignature {
  struct PrivateConstructoTag {};

 public:
  static const QExprOperatorSignature* Get(
      absl::Span<const QTypePtr> input_qtypes, QTypePtr output_qtype);

  QExprOperatorSignature(PrivateConstructoTag,
                         absl::Span<const QTypePtr> input_qtypes,
                         QTypePtr output_qtype);

  absl::string_view name() const { return name_; }

  // Gets the types of this function's inputs.
  absl::Span<const QTypePtr> GetInputTypes() const { return input_qtypes_; }

  // Gets the type of this function's output.
  QTypePtr GetOutputType() const { return output_qtype_; }

 private:
  std::string name_;
  std::vector<QTypePtr> input_qtypes_;
  QTypePtr output_qtype_;
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_QEXPR_OPERATOR_SIGNATURE_H_
