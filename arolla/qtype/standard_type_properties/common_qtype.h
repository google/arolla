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
#ifndef AROLLA_QTYPE_STANDARD_TYPE_PROPERTIES_COMMON_QTYPE_H_
#define AROLLA_QTYPE_STANDARD_TYPE_PROPERTIES_COMMON_QTYPE_H_


#include "absl/types/span.h"
#include "arolla/qtype/qtype.h"

namespace arolla {

// Determines the common type that all input types can be implicitly cast to.
//
// If enable_broadcasting is set, the function will also allow broadcasting from
// scalar/optional types to array types.
//
// TODO: Consider removing enable_broadcasting parameter.
//
const QType* CommonQType(const QType* lhs_qtype, const QType* rhs_qtype,
                         bool enable_broadcasting);

const QType* CommonQType(absl::Span<const QType* const> qtypes,
                         bool enable_broadcasting);

// Returns true if `from_qtype` can be casted to to_qtype implicitly.
bool CanCastImplicitly(const QType* from_qtype, const QType* to_qtype,
                       bool enable_broadcasting);

// Broadcasts the given `qtype` to a common shape with `target_qtypes`. Returns
// nullptr if broadcasting wasn't successful.
//
// NOTE: This function *does* broadcasting from scalar to optional. For example:
//
//  BroadcastQType({OPTIONAL_INT64}, INT32) -> OPTIONAL_INT32
//
const QType* BroadcastQType(absl::Span<QType const* const> target_qtypes,
                            const QType* qtype);

}  // namespace arolla

#endif  // AROLLA_QTYPE_STANDARD_TYPE_PROPERTIES_COMMON_QTYPE_H_
