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
#ifndef AROLLA_OBJECTS_OBJECT_QTYPE_H_
#define AROLLA_OBJECTS_OBJECT_QTYPE_H_

// IWYU pragma: always_keep, the file defines QTypeTraits<T> specializations.

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/refcount_ptr.h"
#include "arolla/util/repr.h"

namespace arolla {

// Object type storing a mapping from attributes to values (as TypedValues).
// Structured like a JavaScript-like object, optionally containing a prototype
// chain representing a hierarchy.
// * Attribute shadowing is allowed.
// * attribute() contains the attr->value mapping for the current object and
//   does not include fields from the prototype (unless shadowed).
// * Attribute retrieval starts at the current Object and traverses up the
//   prototype chain until either the attribute is found or the chain ends.
//
// Objects are moveable and copyable (cheaply).
//
// Does not offer a mutable API.
class Object {
 public:
  using Attributes = absl::flat_hash_map<std::string, TypedValue>;

  Object() = default;
  explicit Object(Attributes attributes,
                  std::optional<Object> prototype = std::nullopt)
      : impl_(RefcountPtr<Impl>::Make(std::move(attributes),
                                      std::move(prototype))) {}

  // Moveable and copyable.
  Object(Object&& other) = default;
  Object& operator=(Object&& other) = default;
  Object(const Object& other) = default;
  Object& operator=(const Object& other) = default;

  // Returns the value for `attr` if it exists in `attributes()` or in the
  // `prototype` (recursively.)
  const TypedValue* absl_nullable GetAttrOrNull(absl::string_view attr) const;

  // Returns the `attributes` of this Object. Does _not_ look into the
  // `prototype`.
  const Attributes& attributes() const;

  // Returns the <attr_name, value> pairs sorted by the attr_name.
  std::vector<Attributes::const_pointer> GetSortedAttributes() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Returns the `prototype` of this Object.
  const std::optional<Object>& prototype() const;

  // Computes the fingerprint of the Object.
  void ArollaFingerprint(FingerprintHasher* hasher) const;

  // Creates a repr of the Object.
  ReprToken ArollaReprToken() const;

 private:
  struct Impl;
  RefcountPtr<Impl> impl_;
};

struct Object::Impl : RefcountedBase {
  Impl() = default;
  Impl(Attributes attributes, std::optional<Object> prototype)
      : attributes(std::move(attributes)), prototype(std::move(prototype)) {}

  static const Impl& Empty();

  Attributes attributes;
  std::optional<Object> prototype;
};

AROLLA_DECLARE_QTYPE(Object);

}  // namespace arolla

#endif  // AROLLA_OBJECTS_OBJECT_QTYPE_H_
