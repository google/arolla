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
#include "py/arolla/py_utils/py_object_ptr_impl.h"

#include <Python.h>

#include <utility>

#include "gtest/gtest.h"

namespace arolla::python::py_object_ptr_impl_internal::testing {
namespace {

struct DummyGILGuard {
  static int active;  // number of instances at the moment
  static int total;   // total number of constructed instances

  static void reset() {
    active = 0;
    total = 0;
  }

  DummyGILGuard() {
    ++active;
    ++total;
  }
  ~DummyGILGuard() { --active; }

  DummyGILGuard(const DummyGILGuard&) = delete;
  DummyGILGuard& operator=(const DummyGILGuard&) = delete;
};

int DummyGILGuard::active;
int DummyGILGuard::total;

struct DummyPyObject {
  int ref_counter = {1};
};

struct DummyTraits {
  using GILGuardType = DummyGILGuard;
  using PyObjectType = DummyPyObject;
  void inc_ref(PyObjectType* ptr) { ++ptr->ref_counter; }
  void dec_ref(PyObjectType* ptr) { --ptr->ref_counter; }
};

class DummyPyObjectPtr final
    : public BasePyObjectPtr<DummyPyObjectPtr, DummyTraits> {
 public:
  // Default-constructible.
  DummyPyObjectPtr() = default;

  // Copyable.
  DummyPyObjectPtr(const DummyPyObjectPtr&) = default;
  DummyPyObjectPtr& operator=(const DummyPyObjectPtr&) = default;

  // Movable.
  DummyPyObjectPtr(DummyPyObjectPtr&&) = default;
  DummyPyObjectPtr& operator=(DummyPyObjectPtr&&) = default;
};

class BasePyObjectPtrTest : public ::testing::Test {
 protected:
  void SetUp() override { DummyGILGuard::reset(); }
};

TEST_F(BasePyObjectPtrTest, OwnFactoryNull) {
  {
    auto ptr = DummyPyObjectPtr::Own(nullptr);
    ASSERT_EQ(ptr.get(), nullptr);
    ASSERT_EQ(DummyGILGuard::active, 0);
    ASSERT_EQ(DummyGILGuard::total, 0);
  }
  ASSERT_EQ(DummyGILGuard::active, 0);
  ASSERT_EQ(DummyGILGuard::total, 0);
}

TEST_F(BasePyObjectPtrTest, OwnNullFactory) {
  DummyPyObject obj;
  {
    auto ptr = DummyPyObjectPtr::Own(&obj);
    ASSERT_EQ(ptr.get(), &obj);
    ASSERT_EQ(obj.ref_counter, 1);
    ASSERT_EQ(DummyGILGuard::active, 0);
    ASSERT_EQ(DummyGILGuard::total, 0);
  }
  ASSERT_EQ(obj.ref_counter, 0);
  ASSERT_EQ(DummyGILGuard::active, 0);
  ASSERT_EQ(DummyGILGuard::total, 1);
}

TEST_F(BasePyObjectPtrTest, NewRefNullFactory) {
  {
    auto ptr = DummyPyObjectPtr::NewRef(nullptr);
    ASSERT_EQ(ptr.get(), nullptr);
    ASSERT_EQ(DummyGILGuard::active, 0);
    ASSERT_EQ(DummyGILGuard::total, 0);
  }
  ASSERT_EQ(DummyGILGuard::active, 0);
  ASSERT_EQ(DummyGILGuard::total, 0);
}

TEST_F(BasePyObjectPtrTest, NewRefFactory) {
  DummyPyObject obj;
  {
    auto ptr = DummyPyObjectPtr::NewRef(&obj);
    ASSERT_EQ(ptr.get(), &obj);
    ASSERT_EQ(obj.ref_counter, 2);
    ASSERT_EQ(DummyGILGuard::active, 0);
    ASSERT_EQ(DummyGILGuard::total, 1);
  }
  ASSERT_EQ(obj.ref_counter, 1);
  ASSERT_EQ(DummyGILGuard::active, 0);
  ASSERT_EQ(DummyGILGuard::total, 2);
}

TEST_F(BasePyObjectPtrTest, DefaultCtor) {
  {
    DummyPyObjectPtr ptr;
    ASSERT_EQ(ptr.get(), nullptr);
    ASSERT_EQ(DummyGILGuard::active, 0);
    ASSERT_EQ(DummyGILGuard::total, 0);
  }
  ASSERT_EQ(DummyGILGuard::active, 0);
  ASSERT_EQ(DummyGILGuard::total, 0);
}

TEST_F(BasePyObjectPtrTest, CopyNullCtor) {
  {
    DummyPyObjectPtr ptr1;
    DummyPyObjectPtr ptr2 = ptr1;
    ASSERT_EQ(ptr1.get(), nullptr);
    ASSERT_EQ(ptr2.get(), nullptr);
    ASSERT_EQ(DummyGILGuard::active, 0);
    ASSERT_EQ(DummyGILGuard::total, 0);
  }
  ASSERT_EQ(DummyGILGuard::active, 0);
  ASSERT_EQ(DummyGILGuard::total, 0);
}

TEST_F(BasePyObjectPtrTest, CopyCtor) {
  DummyPyObject obj;
  {
    DummyPyObjectPtr ptr1 = DummyPyObjectPtr::Own(&obj);
    DummyPyObjectPtr ptr2 = ptr1;
    ASSERT_EQ(ptr1.get(), &obj);
    ASSERT_EQ(ptr2.get(), &obj);
    ASSERT_EQ(obj.ref_counter, 2);
    ASSERT_EQ(DummyGILGuard::active, 0);
    ASSERT_EQ(DummyGILGuard::total, 1);
  }
  ASSERT_EQ(obj.ref_counter, 0);
  ASSERT_EQ(DummyGILGuard::active, 0);
  ASSERT_EQ(DummyGILGuard::total, 3);
}

TEST_F(BasePyObjectPtrTest, MoveNullCtor) {
  {
    DummyPyObjectPtr ptr1;
    DummyPyObjectPtr ptr2 = std::move(ptr1);
    ASSERT_EQ(ptr1.get(), nullptr);  // NOLINT
    ASSERT_EQ(ptr2.get(), nullptr);
    ASSERT_EQ(DummyGILGuard::active, 0);
    ASSERT_EQ(DummyGILGuard::total, 0);
  }
  ASSERT_EQ(DummyGILGuard::active, 0);
  ASSERT_EQ(DummyGILGuard::total, 0);
}

TEST_F(BasePyObjectPtrTest, MoveCtor) {
  DummyPyObject obj;
  {
    DummyPyObjectPtr ptr1 = DummyPyObjectPtr::Own(&obj);
    DummyPyObjectPtr ptr2 = std::move(ptr1);
    ASSERT_EQ(ptr1.get(), nullptr);  // NOLINT
    ASSERT_EQ(ptr2.get(), &obj);
    ASSERT_EQ(obj.ref_counter, 1);
    ASSERT_EQ(DummyGILGuard::active, 0);
    ASSERT_EQ(DummyGILGuard::total, 0);
  }
  ASSERT_EQ(obj.ref_counter, 0);
  ASSERT_EQ(DummyGILGuard::active, 0);
  ASSERT_EQ(DummyGILGuard::total, 1);
}

TEST_F(BasePyObjectPtrTest, CopyOp_Null_Null) {
  {
    DummyPyObjectPtr ptr1;
    DummyPyObjectPtr ptr2;
    ptr1 = ptr2;
    ASSERT_EQ(ptr1.get(), nullptr);
    ASSERT_EQ(ptr2.get(), nullptr);
    ASSERT_EQ(DummyGILGuard::active, 0);
    ASSERT_EQ(DummyGILGuard::total, 0);
  }
  ASSERT_EQ(DummyGILGuard::active, 0);
  ASSERT_EQ(DummyGILGuard::total, 0);
}

TEST_F(BasePyObjectPtrTest, CopyOp_Null_Obj) {
  DummyPyObject obj;
  {
    DummyPyObjectPtr ptr1;
    DummyPyObjectPtr ptr2 = DummyPyObjectPtr::Own(&obj);
    ptr1 = ptr2;
    ASSERT_EQ(ptr1.get(), &obj);
    ASSERT_EQ(ptr2.get(), &obj);
    ASSERT_EQ(obj.ref_counter, 2);
    ASSERT_EQ(DummyGILGuard::active, 0);
    ASSERT_EQ(DummyGILGuard::total, 1);
  }
  ASSERT_EQ(obj.ref_counter, 0);
  ASSERT_EQ(DummyGILGuard::active, 0);
  ASSERT_EQ(DummyGILGuard::total, 3);
}

TEST_F(BasePyObjectPtrTest, CopyOp_Obj_Null) {
  DummyPyObject obj;
  {
    DummyPyObjectPtr ptr1 = DummyPyObjectPtr::Own(&obj);
    DummyPyObjectPtr ptr2;
    ptr1 = ptr2;
    ASSERT_EQ(ptr1.get(), nullptr);
    ASSERT_EQ(ptr2.get(), nullptr);
    ASSERT_EQ(obj.ref_counter, 0);
    ASSERT_EQ(DummyGILGuard::active, 0);
    ASSERT_EQ(DummyGILGuard::total, 1);
  }
  ASSERT_EQ(obj.ref_counter, 0);
  ASSERT_EQ(DummyGILGuard::active, 0);
  ASSERT_EQ(DummyGILGuard::total, 1);
}

TEST_F(BasePyObjectPtrTest, CopyOp_Obj1_Obj1) {
  DummyPyObject obj1;
  {
    DummyPyObjectPtr ptr1 = DummyPyObjectPtr::Own(&obj1);
    DummyPyObjectPtr ptr2 = DummyPyObjectPtr::NewRef(
        &obj1);  // calling "Own" twice would look suspicious
    ptr1 = ptr2;
    ASSERT_EQ(ptr1.get(), &obj1);
    ASSERT_EQ(ptr2.get(), &obj1);
    ASSERT_EQ(obj1.ref_counter, 2);
    ASSERT_EQ(DummyGILGuard::active, 0);
    ASSERT_EQ(DummyGILGuard::total, 1);
  }
  ASSERT_EQ(obj1.ref_counter, 0);
  ASSERT_EQ(DummyGILGuard::active, 0);
  ASSERT_EQ(DummyGILGuard::total, 3);
}

TEST_F(BasePyObjectPtrTest, CopyOp_Obj1_Obj2) {
  DummyPyObject obj1;
  DummyPyObject obj2;
  {
    DummyPyObjectPtr ptr1 = DummyPyObjectPtr::Own(&obj1);
    DummyPyObjectPtr ptr2 = DummyPyObjectPtr::Own(&obj2);
    ptr1 = ptr2;
    ASSERT_EQ(ptr1.get(), &obj2);
    ASSERT_EQ(ptr2.get(), &obj2);
    ASSERT_EQ(obj1.ref_counter, 0);
    ASSERT_EQ(obj2.ref_counter, 2);
    ASSERT_EQ(DummyGILGuard::active, 0);
    ASSERT_EQ(DummyGILGuard::total, 1);
  }
  ASSERT_EQ(obj1.ref_counter, 0);
  ASSERT_EQ(obj2.ref_counter, 0);
  ASSERT_EQ(DummyGILGuard::active, 0);
  ASSERT_EQ(DummyGILGuard::total, 3);
}

TEST_F(BasePyObjectPtrTest, MoveOp_Null_Null) {
  {
    DummyPyObjectPtr ptr1;
    DummyPyObjectPtr ptr2;
    ptr1 = std::move(ptr2);
    ASSERT_EQ(ptr1.get(), nullptr);
    ASSERT_EQ(ptr2.get(), nullptr);  // NOLINT
    ASSERT_EQ(DummyGILGuard::active, 0);
    ASSERT_EQ(DummyGILGuard::total, 0);
  }
  ASSERT_EQ(DummyGILGuard::active, 0);
  ASSERT_EQ(DummyGILGuard::total, 0);
}

TEST_F(BasePyObjectPtrTest, MoveOp_Null_Obj) {
  DummyPyObject obj;
  {
    DummyPyObjectPtr ptr1;
    DummyPyObjectPtr ptr2 = DummyPyObjectPtr::Own(&obj);
    ptr1 = std::move(ptr2);
    ASSERT_EQ(ptr1.get(), &obj);
    ASSERT_EQ(ptr2.get(), nullptr);  // NOLINT
    ASSERT_EQ(obj.ref_counter, 1);
    ASSERT_EQ(DummyGILGuard::active, 0);
    ASSERT_EQ(DummyGILGuard::total, 0);
  }
  ASSERT_EQ(obj.ref_counter, 0);
  ASSERT_EQ(DummyGILGuard::active, 0);
  ASSERT_EQ(DummyGILGuard::total, 1);
}

TEST_F(BasePyObjectPtrTest, MoveOp_Obj_Null) {
  DummyPyObject obj;
  {
    DummyPyObjectPtr ptr1 = DummyPyObjectPtr::Own(&obj);
    DummyPyObjectPtr ptr2;
    ptr1 = std::move(ptr2);
    ASSERT_EQ(ptr1.get(), nullptr);
    ASSERT_EQ(ptr2.get(), nullptr);  // NOLINT
    ASSERT_EQ(obj.ref_counter, 0);
    ASSERT_EQ(DummyGILGuard::active, 0);
    ASSERT_EQ(DummyGILGuard::total, 1);
  }
  ASSERT_EQ(obj.ref_counter, 0);
  ASSERT_EQ(DummyGILGuard::active, 0);
  ASSERT_EQ(DummyGILGuard::total, 1);
}

TEST_F(BasePyObjectPtrTest, MoveOp_Obj1_Obj1) {
  DummyPyObject obj1;
  {
    DummyPyObjectPtr ptr1 = DummyPyObjectPtr::Own(&obj1);
    DummyPyObjectPtr ptr2 = DummyPyObjectPtr::NewRef(
        &obj1);  // calling "Own" twice would look suspicious
    ptr1 = std::move(ptr2);
    ASSERT_EQ(ptr1.get(), &obj1);
    ASSERT_EQ(ptr2.get(), nullptr);  // NOLINT
    ASSERT_EQ(obj1.ref_counter, 1);
    ASSERT_EQ(DummyGILGuard::active, 0);
    ASSERT_EQ(DummyGILGuard::total, 2);
  }
  ASSERT_EQ(obj1.ref_counter, 0);
  ASSERT_EQ(DummyGILGuard::active, 0);
  ASSERT_EQ(DummyGILGuard::total, 3);
}

TEST_F(BasePyObjectPtrTest, MoveOp_Obj1_Obj2) {
  DummyPyObject obj1;
  DummyPyObject obj2;
  {
    DummyPyObjectPtr ptr1 = DummyPyObjectPtr::Own(&obj1);
    DummyPyObjectPtr ptr2 = DummyPyObjectPtr::Own(&obj2);
    ptr1 = std::move(ptr2);
    ASSERT_EQ(ptr1.get(), &obj2);
    ASSERT_EQ(ptr2.get(), nullptr);  // NOLINT
    ASSERT_EQ(obj1.ref_counter, 0);
    ASSERT_EQ(obj2.ref_counter, 1);
    ASSERT_EQ(DummyGILGuard::active, 0);
    ASSERT_EQ(DummyGILGuard::total, 1);
  }
  ASSERT_EQ(obj1.ref_counter, 0);
  ASSERT_EQ(obj2.ref_counter, 0);
  ASSERT_EQ(DummyGILGuard::active, 0);
  ASSERT_EQ(DummyGILGuard::total, 2);
}

TEST_F(BasePyObjectPtrTest, Equality) {
  DummyPyObject obj;
  const DummyPyObjectPtr null_ptr;
  const DummyPyObjectPtr not_null_ptr = DummyPyObjectPtr::Own(&obj);
  ASSERT_TRUE(null_ptr == nullptr);
  ASSERT_FALSE(null_ptr != nullptr);
  ASSERT_FALSE(not_null_ptr == nullptr);
  ASSERT_TRUE(not_null_ptr != nullptr);
  ASSERT_EQ(obj.ref_counter, 1);
  ASSERT_EQ(DummyGILGuard::active, 0);
  ASSERT_EQ(DummyGILGuard::total, 0);
}

TEST_F(BasePyObjectPtrTest, Release) {
  DummyPyObject obj;
  {
    DummyPyObjectPtr ptr = DummyPyObjectPtr::Own(&obj);
    ASSERT_EQ(ptr.release(), &obj);
    ASSERT_EQ(ptr.get(), nullptr);
    ASSERT_EQ(obj.ref_counter, 1);
    ASSERT_EQ(DummyGILGuard::active, 0);
    ASSERT_EQ(DummyGILGuard::total, 0);
  }
  ASSERT_EQ(obj.ref_counter, 1);
  ASSERT_EQ(DummyGILGuard::active, 0);
  ASSERT_EQ(DummyGILGuard::total, 0);
}

TEST_F(BasePyObjectPtrTest, ResetNull) {
  {
    DummyPyObjectPtr ptr;
    ptr.reset();
    ASSERT_EQ(ptr.get(), nullptr);
    ASSERT_EQ(DummyGILGuard::active, 0);
    ASSERT_EQ(DummyGILGuard::total, 0);
  }
  ASSERT_EQ(DummyGILGuard::active, 0);
  ASSERT_EQ(DummyGILGuard::total, 0);
}

TEST_F(BasePyObjectPtrTest, Reset) {
  DummyPyObject obj;
  {
    DummyPyObjectPtr ptr = DummyPyObjectPtr::Own(&obj);
    ptr.reset();
    ASSERT_EQ(ptr.get(), nullptr);
    ASSERT_EQ(obj.ref_counter, 0);
    ASSERT_EQ(DummyGILGuard::active, 0);
    ASSERT_EQ(DummyGILGuard::total, 1);
  }
  ASSERT_EQ(obj.ref_counter, 0);
  ASSERT_EQ(DummyGILGuard::active, 0);
  ASSERT_EQ(DummyGILGuard::total, 1);
}

}  // namespace
}  // namespace arolla::python::py_object_ptr_impl_internal::testing
