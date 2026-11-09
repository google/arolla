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
#include "arolla/util/fast_dynamic_downcast_final.h"

#include <new>

#include "gtest/gtest.h"

namespace arolla {
namespace {

class A {
 public:
  virtual ~A() = default;
};

class B final : public A {};
class C : public A {};
class D final : public C {};

TEST(FastDynamicDownCastFinal, Basics) {
  B b;
  D d;
  EXPECT_EQ(fast_dynamic_downcast_final<B*>(std::launder<A>(nullptr)), nullptr);
  EXPECT_EQ(fast_dynamic_downcast_final<D*>(std::launder<A>(nullptr)), nullptr);
  EXPECT_EQ(fast_dynamic_downcast_final<B*>(std::launder<A>(&b)), &b);
  EXPECT_EQ(fast_dynamic_downcast_final<B*>(std::launder<A>(&d)), nullptr);
  EXPECT_EQ(fast_dynamic_downcast_final<D*>(std::launder<A>(&b)), nullptr);
  EXPECT_EQ(fast_dynamic_downcast_final<D*>(std::launder<A>(&d)), &d);
  EXPECT_EQ(fast_dynamic_downcast_final<D*>(std::launder<C>(&d)), &d);
  EXPECT_EQ(
      fast_dynamic_downcast_final<const B*>(std::launder<const A>(nullptr)),
      nullptr);
  EXPECT_EQ(
      fast_dynamic_downcast_final<const D*>(std::launder<const A>(nullptr)),
      nullptr);
  EXPECT_EQ(fast_dynamic_downcast_final<const B*>(std::launder<const A>(&b)),
            &b);
  EXPECT_EQ(fast_dynamic_downcast_final<const B*>(std::launder<const A>(&d)),
            nullptr);
  EXPECT_EQ(fast_dynamic_downcast_final<const D*>(std::launder<const A>(&b)),
            nullptr);
  EXPECT_EQ(fast_dynamic_downcast_final<const D*>(std::launder<const A>(&d)),
            &d);
  EXPECT_EQ(fast_dynamic_downcast_final<const D*>(std::launder<const C>(&d)),
            &d);
}

class E {
 public:
  virtual ~E() = default;
};

class F final : public A, public E {};

TEST(FastDynamicDownCastFinal, MultiInheritance) {
  F f;
  EXPECT_EQ(fast_dynamic_downcast_final<F*>(std::launder<A>(nullptr)), nullptr);
  EXPECT_EQ(fast_dynamic_downcast_final<F*>(std::launder<E>(nullptr)), nullptr);
  EXPECT_EQ(fast_dynamic_downcast_final<F*>(std::launder<A>(&f)), &f);
  EXPECT_EQ(fast_dynamic_downcast_final<F*>(std::launder<E>(&f)), &f);

  EXPECT_EQ(fast_dynamic_downcast_final<const F*>(std::launder<A>(nullptr)),
            nullptr);
  EXPECT_EQ(fast_dynamic_downcast_final<const F*>(std::launder<E>(nullptr)),
            nullptr);
  EXPECT_EQ(fast_dynamic_downcast_final<const F*>(std::launder<A>(&f)), &f);
  EXPECT_EQ(fast_dynamic_downcast_final<const F*>(std::launder<E>(&f)), &f);
}

class PrivateDerived final : private A {};
class VirtualDerived final : public virtual A {};
struct DiamondBranch1 : A {};
struct DiamondBranch2 : A {};
struct DiamondDerived final : DiamondBranch1, DiamondBranch2 {};
struct NonPolymorphicBase {};
struct NonPolymorphicDerived final : NonPolymorphicBase {};

template <typename T, typename SPtr>
concept CanFastDowncast =
    requires(SPtr s) { fast_dynamic_downcast_final<T>(s); };

TEST(FastDynamicDownCastFinal, Constraints) {
  static_assert(CanFastDowncast<B*, A*>);
  static_assert(CanFastDowncast<const B*, A*>);
  static_assert(CanFastDowncast<const B*, const A*>);
  static_assert(CanFastDowncast<F*, E*>);

  static_assert(!CanFastDowncast<B, A*>);                 // non-pointer T
  static_assert(!CanFastDowncast<B*, const A*>);          // drops const
  static_assert(!CanFastDowncast<B*, volatile A*>);       // drops volatile
  static_assert(!CanFastDowncast<B*, B*>);                // same type
  static_assert(!CanFastDowncast<A*, B*>);                // upcast
  static_assert(!CanFastDowncast<C*, A*>);                // non-final target
  static_assert(!CanFastDowncast<PrivateDerived*, A*>);   // private base
  static_assert(!CanFastDowncast<DiamondDerived*, A*>);   // ambiguous base
  static_assert(!CanFastDowncast<VirtualDerived*, A*>);   // virtual base
  static_assert(!CanFastDowncast<NonPolymorphicDerived*,  // non-polymorphic
                                 NonPolymorphicBase*>);
}

}  // namespace
}  // namespace arolla
