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
#ifndef AROLLA_JAGGED_SHAPE_QTYPE_QTYPE_H_
#define AROLLA_JAGGED_SHAPE_QTYPE_QTYPE_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/simple_qtype.h"

namespace arolla {

// Base class for all array JaggedShapeQTypes. Not movable or copyable.
class JaggedShapeQType : public SimpleQType {
 public:
  virtual QTypePtr edge_qtype() const = 0;

 protected:
  using SimpleQType::SimpleQType;
};

// Returns the jagged shape qtype corresponding to the provided edge qtype.
absl::StatusOr<QTypePtr> GetJaggedShapeQTypeFromEdgeQType(QTypePtr edge_qtype);

// Sets the jagged shape qtype fn corresponding to `edge_qtype`.
absl::Status SetEdgeQTypeToJaggedShapeQType(QTypePtr edge_qtype,
                                            QTypePtr jagged_shape_qtype);

// Returns True iff `qtype` is a jagged shape qtype.
bool IsJaggedShapeQType(QTypePtr qtype);

}  // namespace arolla

#endif  // AROLLA_JAGGED_SHAPE_QTYPE_H_
