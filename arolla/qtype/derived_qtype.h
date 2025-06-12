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
#ifndef AROLLA_QTYPE_DERIVED_QTYPE_H_
#define AROLLA_QTYPE_DERIVED_QTYPE_H_

#include <string>

#include "absl/status/status.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace arolla {

// Derived QType interface.
//
// Derived QType is an analogue of strong typedef in qtype system.
// The derived type shares the memory representation with the base type, but
// provides a different semantic.
//
// Type info and type layout of a derived qtype must be the same as in the base
// qtype.
//
// NOTE: We recommend verifying your derived qtypes using VerifyDerivedQType().
class DerivedQTypeInterface {
 public:
  virtual ~DerivedQTypeInterface() = default;

  // Returns base qtype.
  virtual QTypePtr GetBaseQType() const = 0;
};

// This class provides a basic implementation of DeriveQTypeInterface based on
// the methods of the base_qtype.
//
// Example:
//   class Point : public BasicDerivedQType {
//    public:
//     Point()
//         : DerivedQType(ConstructorArgs{
//               .name = "POINT",
//               .base_qtype =
//                   MakeTupleQType({GetQType<double>(), GetQType<double>()}),
//           }) {}
//   };
//
class BasicDerivedQType : public QType, public DerivedQTypeInterface {
 public:
  // Note: Supports overriding in sub-classes.
  ReprToken UnsafeReprToken(const void* source) const override;

  void UnsafeCombineToFingerprintHasher(const void* source,
                                        FingerprintHasher* hasher) const final;
  void UnsafeCopy(const void* source, void* destination) const final;

  QTypePtr GetBaseQType() const final { return base_qtype_; }

 protected:
  struct ConstructorArgs {
    std::string name;                      // required
    QTypePtr base_qtype;                   // required
    const QType* value_qtype = nullptr;    // nullptr for non-containers
    std::string qtype_specialization_key;  // optional
  };

  explicit BasicDerivedQType(ConstructorArgs args);

 private:
  QTypePtr base_qtype_;
};

// Verifies correctness of a derived qtype state.
//
// // NOTE: We recommend applying this verification for all qtypes implementing
// DerivedQTypeInterface. For types based on BasicDerivedQType the verification
// happens automatically.
absl::Status VerifyDerivedQType(QTypePtr qtype);

// Returns the base_qtype if the argument is a derived qtype, or the argument
// unchanged.
const QType* /*nullable*/ DecayDerivedQType(const QType* /*nullable*/ qtype);

// Returns the base qvalue if the argument is a derived qvalue, or the argument
// unchanged.
TypedRef DecayDerivedQValue(TypedRef qvalue);

// Returns the base qvalue if the argument is a derived qvalue, or the argument
// unchanged.
TypedValue DecayDerivedQValue(const TypedValue& qvalue);

// Returns `qvalue` downcasted to `derived_qtype`.
//
// NOTE: `qvalue.GetType()` must be equal to `DecayDerivedQType(derived_qtype)`.
TypedRef UnsafeDowncastDerivedQValue(QTypePtr derived_qtype, TypedRef qvalue);

// Returns `qvalue` downcasted to `derived_qtype`.
//
// NOTE: `qvalue.GetType()` must be equal to `DecayDerivedQType(derived_qtype)`.
TypedValue UnsafeDowncastDerivedQValue(QTypePtr derived_qtype,
                                       const TypedValue& qvalue);

}  // namespace arolla

#endif  // AROLLA_QTYPE_DERIVED_QTYPE_H_
