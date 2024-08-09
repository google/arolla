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
#ifndef AROLLA_QTYPE_STANDARD_TYPE_PROPERTIES_PROPERTIES_H_
#define AROLLA_QTYPE_STANDARD_TYPE_PROPERTIES_PROPERTIES_H_

#include "absl/status/statusor.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/shape_qtype.h"

namespace arolla {

// Returns a non-optional scalar QType of values stored in the type (value_type
// for containers, self for scalars). Returns nullptr if there is no distinct
// scalar type (e.g. for tuples).
const QType* /*nullable*/ GetScalarQTypeOrNull(const QType* /*nullable*/ qtype);

// Returns a non-optional scalar QType of values stored in the type (value_type
// for containers, self for scalars). Returns an error if there is no distinct
// scalar type (e.g. for tuples).
//
// Expects argument to be not null.
absl::StatusOr<QTypePtr> GetScalarQType(QTypePtr qtype);

// Returns a shape qtype corresponding to the given qtype, or nullptr if there
// is no corresponding shape qtype.
const ShapeQType* /*nullable*/ GetShapeQTypeOrNull(
    const QType* /*nullable*/ qtype);

// Returns a shape qtype corresponding to the given qtype.
absl::StatusOr<const ShapeQType*> GetShapeQType(QTypePtr qtype);

// Decays optional / array type from the qtype if any.
// The difference from GetScalarQType is that this function supports
// non-container non-scalar types.
QTypePtr DecayContainerQType(QTypePtr qtype);

// Constructs a QType with the same container type (scalar / optional / array)
// and the provided base scalar type. Returns an error if such a type does not
// exist (e.g. for tuples).
//
// Expects arguments to be not null.
absl::StatusOr<QTypePtr> WithScalarQType(QTypePtr qtype,
                                         QTypePtr new_scalar_qtype);

// Returns a QType that reperesents element presence for the provided QType
// (bool for scalars and optionals, compatible array type for arrays). Returns
// an error if such a type does not exist (e.g. for tuples).
//
// Expects argument to be not null.
absl::StatusOr<QTypePtr> GetPresenceQType(QTypePtr qtype);

// Returns True, if the type holds a value or values that could be missing.
// E.g.
//   IsOptionalLikeQType(VECTOR_INT) is False
//   IsOptionalLikeQType(VECTOR_OPTIONAL_INT) is True
//
bool IsOptionalLikeQType(const QType* /*nullable*/ qtype);

// Returns a QType that is a version of the argument QType with values wrapped
// into optional. For scalar QTypes simply returns ToOptionalQType(qtype).
// Returns an error if such a type does not exist.
//
// Expects the argument to be not null.
absl::StatusOr<QTypePtr> ToOptionalLikeQType(QTypePtr qtype);

}  // namespace arolla

#endif  // AROLLA_QTYPE_STANDARD_TYPE_PROPERTIES_PROPERTIES_H_
