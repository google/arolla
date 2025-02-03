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
#ifndef AROLLA_QTYPE_SLICE_QTYPE_H_
#define AROLLA_QTYPE_SLICE_QTYPE_H_

#include "absl/strings/string_view.h"
#include "arolla/qtype/qtype.h"

namespace arolla {

// Returns true if `qtype` is a slice qtype.
bool IsSliceQType(const QType* /*nullable*/ qtype);

// Returns the slice qtype corresponding to the provided qtypes.
QTypePtr MakeSliceQType(QTypePtr start, QTypePtr stop, QTypePtr step);

// Returns the QType specialization key for a slice.
absl::string_view GetSliceQTypeSpecializationKey();

}  // namespace arolla

#endif  // AROLLA_QTYPE_SLICE_QTYPE_H_
