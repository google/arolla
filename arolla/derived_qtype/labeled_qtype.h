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
#ifndef AROLLA_DERIVED_QTYPE_LABELED_QTYPE_H_
#define AROLLA_DERIVED_QTYPE_LABELED_QTYPE_H_

#include <optional>
#include <string>

#include "absl/base/nullability.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/repr.h"

namespace arolla {

// Repr fn: (value)->repr | nullopt
// Args:
//   value: A TypedRef with a labeled QType.
// Returns:
//   A representation of `value`, or nullopt if it couldn't be represented (for
//   any reason).
using LabeledQTypeReprFn =
    absl::AnyInvocable<std::optional<ReprToken>(TypedRef value) const>;

// Registers a custom repr fn for values with the a labeled QType matching the
// provided `label`. `override` allows repr functions to be overridden.
absl::Status RegisterLabeledQTypeReprFn(std::string label,
                                        LabeledQTypeReprFn repr_fn,
                                        bool override = false);

// Labeled QType is a lightweight implementation of the Derived QType
// interface that can be dynamically instantiated, allowing creation of new
// types without re-compilation.
//
// The empty label corresponds to the base type.
//
// Note for the future: Consider exposing value_qtype / repr /
// qvalue_specialization_key backed with expressions.

// Returns true if `qtype` is a labeled type.
bool IsLabeledQType(QTypePtr absl_nullable qtype);

// Returns a lightweight derived qtype. Returns the base type if the label is
// empty. The label should preferably be unique, at least between projects. Use
// e.g. "project::module::type" to achieve this.
QTypePtr absl_nonnull GetLabeledQType(QTypePtr absl_nonnull qtype,
                                       absl::string_view label);

// Returns the label associated with the type. Returns an empty string if
// the argument is not labeled type.
absl::string_view GetQTypeLabel(QTypePtr absl_nullable qtype);

// Returns the QType specialization key for labeled types.
absl::string_view GetLabeledQTypeSpecializationKey();

}  // namespace arolla

#endif  // AROLLA_DERIVED_QTYPE_LABELED_QTYPE_H_
