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
#include "arolla/util/class_info.h"

#include <sstream>
#include <type_traits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/hash/hash.h"
#include "absl/strings/str_cat.h"

namespace arolla {
namespace {

using ::testing::MatchesRegex;

struct Base {};
struct Derived : public Base {};
struct SubDerived : public Derived {};
struct Unrelated {};

static_assert(std::is_trivially_destructible_v<ClassInfo>);
static_assert(std::is_trivially_copy_constructible_v<ClassInfo>);
static_assert(std::is_trivially_copy_assignable_v<ClassInfo>);

TEST(ClassInfoTest, SubtypeVerification) {
  const auto base_info = RegisterRootClass(typeid(Base));
  const auto derived_info = RegisterSubclass(typeid(Derived), base_info);
  const auto sub_derived_info =
      RegisterSubclass(typeid(SubDerived), derived_info);
  const auto unrelated_info = RegisterRootClass(typeid(Unrelated));

  // Verifying operator== and operator!=
  EXPECT_TRUE(base_info == base_info);
  EXPECT_FALSE(base_info != base_info);
  EXPECT_TRUE(base_info != derived_info);
  EXPECT_FALSE(base_info == derived_info);

  // Verifying class names (demangled, suppressing anonymous namespace)
  EXPECT_THAT(base_info.name(), MatchesRegex("arolla::.*::Base"));
  EXPECT_THAT(derived_info.name(), MatchesRegex("arolla::.*::Derived"));
  EXPECT_THAT(unrelated_info.name(), MatchesRegex("arolla::.*::Unrelated"));
  EXPECT_THAT(base_info.stringify(),
              MatchesRegex("ClassInfo\\{'arolla::.*::Base'\\}"));
  EXPECT_THAT(derived_info.stringify(),
              MatchesRegex("ClassInfo\\{'arolla::.*::Derived'\\}"));
  EXPECT_THAT(unrelated_info.stringify(),
              MatchesRegex("ClassInfo\\{'arolla::.*::Unrelated'\\}"));

  // Verifying std::ostream and absl::Stringify integrations
  std::stringstream ss;
  ss << base_info;
  EXPECT_THAT(ss.str(), base_info.stringify());
  EXPECT_THAT(absl::StrCat(base_info), base_info.stringify());

  // Self subtype checking
  EXPECT_TRUE(base_info.is_subclass_of(base_info));
  EXPECT_TRUE(derived_info.is_subclass_of(derived_info));
  EXPECT_TRUE(sub_derived_info.is_subclass_of(sub_derived_info));
  EXPECT_TRUE(unrelated_info.is_subclass_of(unrelated_info));

  // Down-tree checks (Positive cases)
  EXPECT_TRUE(derived_info.is_subclass_of(base_info));
  EXPECT_TRUE(sub_derived_info.is_subclass_of(base_info));
  EXPECT_TRUE(sub_derived_info.is_subclass_of(derived_info));

  // Up-tree checks (Negative cases)
  EXPECT_FALSE(base_info.is_subclass_of(derived_info));
  EXPECT_FALSE(base_info.is_subclass_of(sub_derived_info));
  EXPECT_FALSE(derived_info.is_subclass_of(sub_derived_info));

  // Cross-tree checks
  EXPECT_FALSE(unrelated_info.is_subclass_of(base_info));
  EXPECT_FALSE(base_info.is_subclass_of(unrelated_info));
  EXPECT_FALSE(derived_info.is_subclass_of(unrelated_info));
}

TEST(ClassInfoTest, ReRegistrationVerification) {
  struct TestA {};
  struct TestB {};
  struct TestChild {};

  // 1. Register initially
  const auto root_info = RegisterRootClass(typeid(TestA));
  const auto another_root = RegisterRootClass(typeid(TestB));
  const auto child_info = RegisterSubclass(typeid(TestChild), root_info);

  // 2. Register identical parameters (Must return exactly the static same
  // ClassInfo values)
  const auto root_again = RegisterRootClass(typeid(TestA));
  EXPECT_EQ(root_again, root_info);

  const auto child_again = RegisterSubclass(typeid(TestChild), root_info);
  EXPECT_EQ(child_again, child_info);

  // 3. Re-registration with mismatched parameters (Must LOG(FATAL) / die)
  // Re-register root as subclass
  EXPECT_DEATH(RegisterSubclass(typeid(TestA), another_root),
               "already registered as a root class");

  // Re-register subclass as root
  EXPECT_DEATH(RegisterRootClass(typeid(TestChild)),
               "already registered as a subclass");

  // Re-register subclass with different parent
  EXPECT_DEATH(RegisterSubclass(typeid(TestChild), another_root),
               "cannot be re-registered as a subclass");
}

TEST(ClassInfoTest, CopySemantics) {
  struct CopyBase {};
  struct CopyDerived {};

  const auto base_info = RegisterRootClass(typeid(CopyBase));
  const auto derived_info = RegisterSubclass(typeid(CopyDerived), base_info);

  // Copy construction preserves identity.
  const ClassInfo base_copy(base_info);
  EXPECT_EQ(base_copy, base_info);
  EXPECT_TRUE(base_copy.is_subclass_of(base_info));
  EXPECT_TRUE(derived_info.is_subclass_of(base_copy));

  // Copy assignment preserves identity.
  ClassInfo assigned = base_info;
  assigned = derived_info;
  EXPECT_EQ(assigned, derived_info);
  EXPECT_TRUE(assigned.is_subclass_of(base_info));
}

TEST(ClassInfoTest, DeepHierarchy) {
  // Exercise a hierarchy at kMaxDepth=8 (root + 7 subclasses).
  struct D0 {};
  struct D1 {};
  struct D2 {};
  struct D3 {};
  struct D4 {};
  struct D5 {};
  struct D6 {};
  struct D7 {};

  const auto d0 = RegisterRootClass(typeid(D0));
  const auto d1 = RegisterSubclass(typeid(D1), d0);
  const auto d2 = RegisterSubclass(typeid(D2), d1);
  const auto d3 = RegisterSubclass(typeid(D3), d2);
  const auto d4 = RegisterSubclass(typeid(D4), d3);
  const auto d5 = RegisterSubclass(typeid(D5), d4);
  const auto d6 = RegisterSubclass(typeid(D6), d5);
  const auto d7 = RegisterSubclass(typeid(D7), d6);

  // The deepest leaf is a subclass of every ancestor.
  EXPECT_TRUE(d7.is_subclass_of(d0));
  EXPECT_TRUE(d7.is_subclass_of(d3));
  EXPECT_TRUE(d7.is_subclass_of(d6));
  EXPECT_TRUE(d7.is_subclass_of(d7));

  // Mid-level is not a subclass of its children.
  EXPECT_FALSE(d3.is_subclass_of(d4));
  EXPECT_FALSE(d0.is_subclass_of(d7));

  // Verify names resolve correctly at depth.
  EXPECT_THAT(d7.name(), MatchesRegex("arolla::.*::D7"));
}

class AdlBase {
 public:
  explicit AdlBase(ClassInfo class_info) : class_info_(class_info) {}

 private:
  ClassInfo class_info_;

  AROLLA_DECLARE_ROOT_CLASS_INFO(AdlBase, class_info_);
};

class AdlDerived : public AdlBase {
 public:
  AdlDerived() : AdlBase(GetClassInfo<AdlDerived>()) {}

  AROLLA_DECLARE_SUBCLASS_INFO(AdlDerived, AdlBase);
};

class AdlFinalDerived : public AdlBase {
 public:
  AdlFinalDerived() : AdlBase(GetClassInfo<AdlFinalDerived>()) {}

  AROLLA_DECLARE_SUBCLASS_INFO(AdlFinalDerived, AdlBase);
};

TEST(ClassInfoTest, HiddenFriendADL) {
  {
    AdlDerived derived_obj;
    AdlBase* base_ptr = &derived_obj;
    EXPECT_TRUE(IsInstanceOf<AdlBase>(derived_obj));
    EXPECT_TRUE(IsInstanceOf<AdlDerived>(base_ptr));
    EXPECT_FALSE(IsInstanceOf<AdlFinalDerived>(base_ptr));
    EXPECT_EQ(FastDowncast<AdlDerived>(base_ptr), &derived_obj);
    EXPECT_EQ(FastDowncast<AdlFinalDerived>(base_ptr), nullptr);
  }
  {
    AdlFinalDerived final_derived_obj;
    AdlBase* base_ptr = &final_derived_obj;
    EXPECT_TRUE(IsInstanceOf<AdlBase>(base_ptr));
    EXPECT_FALSE(IsInstanceOf<AdlDerived>(base_ptr));
    EXPECT_TRUE(IsInstanceOf<AdlFinalDerived>(base_ptr));
    EXPECT_EQ(FastDowncast<AdlDerived>(base_ptr), nullptr);
    EXPECT_EQ(FastDowncast<AdlFinalDerived>(base_ptr), &final_derived_obj);
  }
}

TEST(ClassInfoTest, Hashing) {
  const auto base_info = RegisterRootClass(typeid(Base));
  const auto derived_info = RegisterSubclass(typeid(Derived), base_info);
  const auto unrelated_info = RegisterRootClass(typeid(Unrelated));
  const ClassInfo base_copy = base_info;

  EXPECT_EQ(absl::HashOf(base_info), absl::HashOf(base_copy));
  EXPECT_NE(absl::HashOf(base_info), absl::HashOf(derived_info));
  EXPECT_NE(absl::HashOf(base_info), absl::HashOf(unrelated_info));
}

}  // namespace
}  // namespace arolla
