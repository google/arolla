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
#include "arolla/util/refcount_ptr.h"

#include <memory>
#include <sstream>
#include <type_traits>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/cleanup/cleanup.h"
#include "absl/log/check.h"

namespace arolla {
namespace {

using ::testing::IsNull;
using ::testing::Pointer;

struct RefcountedObject : RefcountedBase {
  static int instance_counter;
  int value = 0;

  RefcountedObject() { ++instance_counter; }
  ~RefcountedObject() { --instance_counter; }
};

struct RefcountedObjectWithArgs : RefcountedBase {
  static int instance_counter;
  int x;
  std::unique_ptr<int> non_copyable_y;

  RefcountedObjectWithArgs(int x, std::unique_ptr<int> non_copyable_y)
      : x(x), non_copyable_y(std::move(non_copyable_y)) {
    ++instance_counter;
  }
  ~RefcountedObjectWithArgs() { --instance_counter; }
};

int RefcountedObject::instance_counter;
int RefcountedObjectWithArgs::instance_counter;

using RefcountedObjectPtr = RefcountPtr<RefcountedObject>;
using RefcountedObjectWithArgsPtr = RefcountPtr<RefcountedObjectWithArgs>;

class RefcountPtrTest : public ::testing::Test {
 protected:
  void SetUp() override { ASSERT_EQ(RefcountedObject::instance_counter, 0); }
  void TearDown() override { ASSERT_EQ(RefcountedObject::instance_counter, 0); }
};

static_assert(!std::is_copy_constructible_v<RefcountedBase> &&
              !std::is_copy_assignable_v<RefcountedBase>);
static_assert(!std::is_move_constructible_v<RefcountedBase> &&
              !std::is_move_assignable_v<RefcountedBase>);

TEST(RefcountPtr, Make) {
  auto ptr = RefcountedObjectWithArgsPtr::Make(2, std::make_unique<int>(3));
  EXPECT_EQ(RefcountedObjectWithArgs::instance_counter, 1);
  EXPECT_EQ(ptr->x, 2);
  const std::unique_ptr<int>& non_copyable_y = ptr->non_copyable_y;
  EXPECT_EQ(*non_copyable_y, 3);
}

TEST(RefcountPtr, Own) {
  auto unique_ptr = std::make_unique<RefcountedObject>();
  EXPECT_EQ(RefcountedObject::instance_counter, 1);
  auto* raw_ptr = unique_ptr.get();
  auto ptr = RefcountedObjectPtr::Own(std::move(unique_ptr));
  EXPECT_THAT(ptr, Pointer(raw_ptr));
}

TEST(RefcountPtr, OwnNullptr) {
  auto ptr = RefcountedObjectPtr::Own(std::unique_ptr<RefcountedObject>());
  EXPECT_THAT(ptr, IsNull());
}

TEST(RefcountPtr, NewRef) {
  auto unique_ptr = std::make_unique<RefcountedObject>();
  EXPECT_EQ(RefcountedObject::instance_counter, 1);
  auto* raw_ptr = unique_ptr.get();
  auto ptr1 = RefcountedObjectPtr::Own(std::move(unique_ptr));
  EXPECT_EQ(RefcountedObject::instance_counter, 1);
  auto ptr2 = RefcountedObjectPtr::NewRef(raw_ptr);
  EXPECT_THAT(ptr2.get(), Pointer(raw_ptr));
  EXPECT_EQ(RefcountedObject::instance_counter, 1);
  ptr1.reset();
  EXPECT_EQ(RefcountedObject::instance_counter, 1);
  ptr2.reset();
  EXPECT_EQ(RefcountedObject::instance_counter, 0);
}

TEST(RefcountPtr, NewRefNullptr) {
  auto ptr = RefcountedObjectPtr::NewRef(nullptr);
  EXPECT_THAT(ptr, IsNull());
}

TEST(RefcountPtr, DefaultConstructor) {
  EXPECT_THAT(RefcountedObjectPtr(), IsNull());
}

TEST(RefcountPtr, NullptrConstructor) {
  EXPECT_THAT(RefcountedObjectPtr(nullptr), IsNull());
}

TEST(RefcountPtr, CopyConstructor) {
  auto unique_ptr = std::make_unique<RefcountedObject>();
  auto* raw_ptr = unique_ptr.get();
  auto ptr1 = RefcountedObjectPtr::Own(std::move(unique_ptr));
  auto ptr2 = ptr1;
  EXPECT_THAT(ptr2, Pointer(raw_ptr));
}

TEST(RefcountPtr, CopyConstructorNullptr) {
  auto ptr1 = RefcountedObjectPtr{};
  auto ptr2 = ptr1;
  EXPECT_THAT(ptr2, IsNull());
}

TEST(RefcountPtr, MoveConstructor) {
  auto unique_ptr = std::make_unique<RefcountedObject>();
  auto* raw_ptr = unique_ptr.get();
  auto ptr1 = RefcountedObjectPtr::Own(std::move(unique_ptr));
  auto ptr2 = std::move(ptr1);
  EXPECT_THAT(ptr2, Pointer(raw_ptr));
}

TEST(RefcountPtr, MoveConstructorNullptr) {
  auto ptr1 = RefcountedObjectPtr();
  auto ptr2 = std::move(ptr1);
  EXPECT_THAT(ptr2, IsNull());
}

TEST(RefcountPtr, CopyOperator) {
  auto unique_ptr1 = std::make_unique<RefcountedObject>();
  auto* raw_ptr1 = unique_ptr1.get();
  auto ptr1 = RefcountedObjectPtr::Own(std::move(unique_ptr1));
  ptr1 = ptr1;
  ASSERT_THAT(ptr1, Pointer(raw_ptr1));
  auto ptr2 = RefcountedObjectPtr::Own(std::make_unique<RefcountedObject>());
  ptr2 = ptr1;
  ASSERT_THAT(ptr1, Pointer(raw_ptr1));
  ASSERT_THAT(ptr2, Pointer(raw_ptr1));
  ptr2 = ptr1;
  ASSERT_THAT(ptr1, Pointer(raw_ptr1));
  ASSERT_THAT(ptr2, Pointer(raw_ptr1));
}

TEST(RefcountPtr, CopyOperatorNullptr) {
  auto ptr1 = RefcountedObjectPtr();
  auto ptr2 = RefcountedObjectPtr::Own(std::make_unique<RefcountedObject>());
  ptr2 = ptr1;
  ASSERT_THAT(ptr1, IsNull());
  ASSERT_THAT(ptr2, IsNull());
  ptr2 = ptr1;
  ASSERT_THAT(ptr1, IsNull());
  ASSERT_THAT(ptr2, IsNull());
}

TEST(RefcountPtr, MoveOperator) {
  auto unique_ptr1 = std::make_unique<RefcountedObject>();
  auto* raw_ptr1 = unique_ptr1.get();
  auto ptr1 = RefcountedObjectPtr::Own(std::move(unique_ptr1));
  auto ptr2 = RefcountedObjectPtr::Own(std::make_unique<RefcountedObject>());
  ptr2 = std::move(ptr1);
  ASSERT_THAT(ptr2, Pointer(raw_ptr1));
  ptr1 = std::move(ptr2);
  ASSERT_THAT(ptr1, Pointer(raw_ptr1));
}

TEST(RefcountPtr, MoveOperatorNullptr) {
  auto ptr1 = RefcountedObjectPtr();
  auto ptr2 = RefcountedObjectPtr::Own(std::make_unique<RefcountedObject>());
  ptr2 = std::move(ptr1);
  ASSERT_THAT(ptr2, IsNull());
  ptr1 = std::move(ptr2);
  ASSERT_THAT(ptr1, IsNull());
}

TEST(RefcountPtr, Reset) {
  auto ptr = RefcountedObjectPtr::Own(std::make_unique<RefcountedObject>());
  ptr.reset();
  ASSERT_THAT(ptr, IsNull());
  ptr.reset();
  ASSERT_THAT(ptr, IsNull());
}

TEST(RefcountPtr, Compare) {
  auto ptr1 = RefcountedObjectPtr::Own(std::make_unique<RefcountedObject>());
  {
    // nullptr comparison.
    auto ptr2 = RefcountedObjectPtr();
    ASSERT_FALSE((ptr1 == nullptr));
    ASSERT_TRUE((ptr1 != nullptr));
    ASSERT_TRUE((ptr2 == nullptr));
    ASSERT_FALSE((ptr2 != nullptr));
  }
  {
    // RefcountPtr comparison.
    auto ptr1_copy = ptr1;
    auto ptr2 = RefcountedObjectPtr::Own(std::make_unique<RefcountedObject>());
    // Equal ptrs.
    EXPECT_TRUE(ptr1 == ptr1);
    EXPECT_TRUE(ptr2 == ptr2);
    EXPECT_TRUE(ptr1 == ptr1_copy);
    EXPECT_FALSE(ptr1 != ptr1);
    EXPECT_FALSE(ptr2 != ptr2);
    EXPECT_FALSE(ptr1 != ptr1_copy);
    // Not equal ptrs.
    EXPECT_FALSE(ptr1 == ptr2);
    EXPECT_TRUE(ptr1 != ptr2);
  }
}

TEST(RefcountPtr, Swap) {
  using std::swap;
  auto unique_ptr1 = std::make_unique<RefcountedObject>();
  auto unique_ptr2 = std::make_unique<RefcountedObject>();
  auto* raw_ptr1 = unique_ptr1.get();
  auto* raw_ptr2 = unique_ptr2.get();
  auto ptr1 = RefcountedObjectPtr::Own(std::move(unique_ptr1));
  auto ptr2 = RefcountedObjectPtr::Own(std::move(unique_ptr2));
  swap(ptr1, ptr2);
  ASSERT_THAT(ptr1, Pointer(raw_ptr2));
  ASSERT_THAT(ptr2, Pointer(raw_ptr1));
  swap(ptr1, ptr1);
  ASSERT_THAT(ptr1, Pointer(raw_ptr2));
  ASSERT_THAT(ptr2, Pointer(raw_ptr1));
}

TEST(RefcountPtr, Get) {
  auto unique_ptr = std::make_unique<RefcountedObject>();
  auto* raw_ptr = unique_ptr.get();
  auto ptr = RefcountedObjectPtr::Own(std::move(unique_ptr));
  ASSERT_EQ(ptr.get(), raw_ptr);
}

TEST(RefcountPtr, Dereference) {
  auto ptr = RefcountedObjectPtr::Own(std::make_unique<RefcountedObject>());
  (*ptr).value = 1;
  ASSERT_EQ(ptr->value, 1);
  ptr->value = 2;
  ASSERT_EQ((*ptr).value, 2);
}

TEST(RefcountPtr, OStream) {
  auto ptr = RefcountedObjectPtr::Own(std::make_unique<RefcountedObject>());
  std::ostringstream actual;
  std::ostringstream expected;
  actual << ptr;
  expected << ptr.get();
  ASSERT_EQ(actual.str(), expected.str());
}

TEST(RefcountPtr, Checks) {
  auto ptr1 = RefcountedObjectPtr::Own(std::make_unique<RefcountedObject>());
  auto ptr2 = RefcountedObjectPtr();
  CHECK_NE(ptr1, nullptr);
  CHECK_EQ(ptr2, nullptr);
  DCHECK_NE(ptr1, nullptr);
  DCHECK_EQ(ptr2, nullptr);
}

// NOTE: Regression test.
TEST(RefcountPtr, SelfReferencingCopyAssignment) {
  struct SelfReferencingObject;
  using SelfReferencingObjectPtr = RefcountPtr<SelfReferencingObject>;
  struct SelfReferencingObject : public RefcountedBase {
    SelfReferencingObjectPtr parent = nullptr;
    ~SelfReferencingObject() noexcept = default;
  };
  auto ptr = SelfReferencingObjectPtr::Make();
  ptr->parent = SelfReferencingObjectPtr::Make();
  ptr = ptr->parent;
}

// NOTE: Regression test.
TEST(RefcountPtr, SelfReferencingChain) {
  struct SelfReferencingObject;
  using SelfReferencingObjectPtr = RefcountPtr<SelfReferencingObject>;
  struct SelfReferencingObject : public RefcountedBase {
    SelfReferencingObjectPtr parent = nullptr;
    ~SelfReferencingObject() noexcept {
      thread_local bool is_cleanup_ongoing = false;
      thread_local SelfReferencingObjectPtr to_destruct;

      // When `this` is `ptr1->parent`, this causes its destructor to be called
      // in the move assignment, which previously decremented the counter and
      // called into this destructor again during deletion without setting the
      // internal ptr to `nullptr`. This caused decrements to be applied again,
      // which in turn caused an underflow.
      to_destruct = std::move(parent);
      if (is_cleanup_ongoing) {
        return;
      }

      is_cleanup_ongoing = true;
      absl::Cleanup reset_flag = [&] { is_cleanup_ongoing = false; };
      while (to_destruct != nullptr) {
        to_destruct.reset();
      }
    }
  };
  auto ptr1 = SelfReferencingObjectPtr::Make();
  ptr1->parent = SelfReferencingObjectPtr::Make();
  ptr1->parent->parent = SelfReferencingObjectPtr::Make();
}

}  // namespace
}  // namespace arolla
