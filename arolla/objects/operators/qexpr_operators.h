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
#ifndef AROLLA_OBJECTS_OPERATORS_QEXPR_OPERATORS_H_
#define AROLLA_OBJECTS_OPERATORS_QEXPR_OPERATORS_H_

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/objects/object_qtype.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"

namespace arolla {

// objects.make_object operator.
class MakeObjectOperatorFamily final : public OperatorFamily {
  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_types,
      QTypePtr output_type) const override;
};

// objects.get_object_attr operator.
class GetObjectAttrOperatorFamily final : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const override;
};

}  // namespace arolla

#endif  // AROLLA_OBJECTS_OPERATORS_QEXPR_OPERATORS_H_
