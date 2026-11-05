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
#include "arolla/util/once_callbacks.h"

#include <memory>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace arolla {
namespace {

using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::UnorderedElementsAre;

TEST(OnceCallbackTest, InvokeAndDestroy) {
  OnceCallback empty;
  EXPECT_FALSE(empty);

  auto resource = std::make_shared<int>(34);
  std::weak_ptr<int> weak_resource = resource;
  std::vector<int> calls;
  OnceCallback callback = [resource = std::move(resource), &calls] {
    calls.push_back(*resource);
  };
  EXPECT_TRUE(callback);
  EXPECT_FALSE(weak_resource.expired());

  std::move(callback).Notify();
  EXPECT_THAT(calls, ElementsAre(34));
  EXPECT_TRUE(weak_resource.expired());
  EXPECT_FALSE(callback);
}

TEST(OnceCallbackTest, ConstructFromLvalueCallable) {
  std::vector<int> calls;
  auto fn = [&calls] { calls.push_back(17); };
  OnceCallback callback(fn);
  EXPECT_TRUE(callback);
  std::move(callback).Notify();
  EXPECT_THAT(calls, ElementsAre(17));
}

TEST(OnceCallbacksTest, AddAndNotifySingle) {
  OnceCallbacks callbacks;
  EXPECT_TRUE(callbacks.empty());

  auto resource = std::make_shared<int>(20);
  std::weak_ptr<int> weak_resource = resource;
  std::vector<int> calls;
  callbacks.Add(
      [resource = std::move(resource), &calls] { calls.push_back(*resource); });
  EXPECT_FALSE(callbacks.empty());

  OnceCallbacks extracted;
  swap(callbacks, extracted);
  EXPECT_TRUE(callbacks.empty());
  EXPECT_THAT(calls, IsEmpty());
  EXPECT_FALSE(weak_resource.expired());
  std::move(extracted).Notify();
  EXPECT_THAT(calls, ElementsAre(20));
  // Captured resources are released immediately upon Notify().
  EXPECT_TRUE(weak_resource.expired());
}

TEST(OnceCallbacksTest, AddAndNotifyMultiple) {
  OnceCallbacks callbacks;
  std::vector<int> calls;
  callbacks.Add([&calls] { calls.push_back(1); });
  callbacks.Add([&calls] { calls.push_back(2); });
  callbacks.Add([&calls] { calls.push_back(3); });
  callbacks.Add([&calls] { calls.push_back(4); });
  EXPECT_FALSE(callbacks.empty());

  OnceCallbacks extracted = std::move(callbacks);
  EXPECT_TRUE(callbacks.empty());
  EXPECT_THAT(calls, IsEmpty());
  std::move(extracted).Notify();
  EXPECT_THAT(calls, UnorderedElementsAre(1, 2, 3, 4));
}

TEST(OnceCallbacksTest, AddWithExistingTokenCombinesCallbacks) {
  OnceCallbacks callbacks;
  std::vector<int> calls;
  OnceCallbacks::Token token;
  EXPECT_FALSE(token);
  token = callbacks.Add([&calls] { calls.push_back(1); }, std::move(token));
  EXPECT_TRUE(token);
  token = callbacks.Add([&calls] { calls.push_back(2); }, std::move(token));
  EXPECT_TRUE(token);
  token = callbacks.Add([&calls] { calls.push_back(3); }, std::move(token));
  EXPECT_TRUE(token);

  std::move(callbacks).Notify();
  EXPECT_THAT(calls, UnorderedElementsAre(1, 2, 3));
}

TEST(OnceCallbacksTest, AddAndRemoveInterleavedClusters) {
  OnceCallbacks callbacks;
  std::vector<int> calls;

  auto t1 = callbacks.Add([&calls] { calls.push_back(10); });
  auto t2 = callbacks.Add([&calls] { calls.push_back(20); });
  t1 = callbacks.Add([&calls] { calls.push_back(11); }, std::move(t1));
  auto t3 = callbacks.Add([&calls] { calls.push_back(30); });
  t2 = callbacks.Add([&calls] { calls.push_back(21); }, std::move(t2));
  t1 = callbacks.Add([&calls] { calls.push_back(12); }, std::move(t1));
  t3 = callbacks.Add([&calls] { calls.push_back(31); }, std::move(t3));

  // Remove the middle cluster `t2` and invoke it separately.
  {
    auto removed_t2 = callbacks.Remove(std::move(t2));
    EXPECT_FALSE(t2);
    EXPECT_FALSE(removed_t2.empty());
    std::move(removed_t2).Notify();
    EXPECT_THAT(calls, UnorderedElementsAre(20, 21));
    calls.clear();
  }

  // Add to `t1` and `t3`, plus a new cluster.
  t1 = callbacks.Add([&calls] { calls.push_back(13); }, std::move(t1));
  t3 = callbacks.Add([&calls] { calls.push_back(32); }, std::move(t3));
  auto t4 = callbacks.Add([&calls] { calls.push_back(40); });
  t4 = callbacks.Add([&calls] { calls.push_back(41); }, std::move(t4));

  // Remove the cluster `t1` and the cluster `t4`.
  callbacks.Remove(std::move(t1));
  callbacks.Remove(std::move(t4));
  EXPECT_FALSE(t1);
  EXPECT_FALSE(t4);

  std::move(callbacks).Notify();
  EXPECT_THAT(calls, UnorderedElementsAre(32, 31, 30));
}

TEST(OnceCallbacksTest, RemoveCombinedToken) {
  OnceCallbacks callbacks;
  std::vector<int> calls;
  auto r1 = std::make_shared<int>(1);
  auto r2 = std::make_shared<int>(2);
  std::weak_ptr<int> weak_r1 = r1;
  std::weak_ptr<int> weak_r2 = r2;

  auto token =
      callbacks.Add([r1 = std::move(r1), &calls] { calls.push_back(*r1); });
  token = callbacks.Add([r2 = std::move(r2), &calls] { calls.push_back(*r2); },
                        std::move(token));
  {
    auto removed = callbacks.Remove(std::move(token));
    EXPECT_FALSE(token);
    EXPECT_FALSE(removed.empty());
    EXPECT_FALSE(weak_r1.expired());
    EXPECT_FALSE(weak_r2.expired());
  }
  EXPECT_TRUE(weak_r1.expired());
  EXPECT_TRUE(weak_r2.expired());
  EXPECT_TRUE(callbacks.empty());
  EXPECT_THAT(calls, IsEmpty());
}

TEST(OnceCallbacksTest, RemoveNodes) {
  OnceCallbacks callbacks;
  EXPECT_TRUE(callbacks.Remove({}).empty());

  auto r1 = std::make_shared<int>(1);
  auto r2 = std::make_shared<int>(2);
  auto r3 = std::make_shared<int>(3);
  std::weak_ptr<int> weak_r1 = r1;
  std::weak_ptr<int> weak_r2 = r2;
  std::weak_ptr<int> weak_r3 = r3;
  std::vector<int> calls;
  auto n1 =
      callbacks.Add([r1 = std::move(r1), &calls] { calls.push_back(*r1); });
  callbacks.Add([r2 = std::move(r2), &calls] { calls.push_back(*r2); });
  auto n3 =
      callbacks.Add([r3 = std::move(r3), &calls] { calls.push_back(*r3); });

  {
    auto removed = callbacks.Remove(std::move(n1));
    EXPECT_FALSE(n1);
    EXPECT_FALSE(weak_r1.expired());
  }
  EXPECT_TRUE(weak_r1.expired());

  {
    auto removed = callbacks.Remove(std::move(n3));
    EXPECT_FALSE(n3);
    EXPECT_FALSE(weak_r3.expired());
  }
  EXPECT_TRUE(weak_r3.expired());
  EXPECT_FALSE(weak_r2.expired());

  std::move(callbacks).Notify();
  EXPECT_THAT(calls, ElementsAre(2));
  EXPECT_TRUE(weak_r2.expired());
}

TEST(OnceCallbacksTest, DestroyListWithoutNotify) {
  std::vector<int> calls;
  auto r1 = std::make_shared<int>(1);
  auto r2 = std::make_shared<int>(2);
  std::weak_ptr<int> weak_r1 = r1;
  std::weak_ptr<int> weak_r2 = r2;
  {
    OnceCallbacks callbacks;
    callbacks.Add([r1 = std::move(r1), &calls] { calls.push_back(*r1); });
    callbacks.Add([r2 = std::move(r2), &calls] { calls.push_back(*r2); });
    OnceCallbacks extracted;
    swap(callbacks, extracted);
    EXPECT_FALSE(weak_r1.expired());
    EXPECT_FALSE(weak_r2.expired());
  }
  EXPECT_TRUE(weak_r1.expired());
  EXPECT_TRUE(weak_r2.expired());
  EXPECT_THAT(calls, IsEmpty());
}

TEST(OnceCallbacksTest, MoveAssignmentDestroysTargetNodes) {
  auto r1 = std::make_shared<int>(1);
  auto r2 = std::make_shared<int>(2);
  std::weak_ptr<int> weak_r1 = r1;
  std::weak_ptr<int> weak_r2 = r2;
  std::vector<int> calls;

  OnceCallbacks list1;
  list1.Add([r1 = std::move(r1), &calls] { calls.push_back(*r1); });
  OnceCallbacks list2;
  list2.Add([r2 = std::move(r2), &calls] { calls.push_back(*r2); });

  list1 = std::move(list2);
  EXPECT_TRUE(weak_r1.expired());
  EXPECT_FALSE(weak_r2.expired());
  EXPECT_TRUE(list2.empty());

  std::move(list1).Notify();
  EXPECT_TRUE(weak_r2.expired());
  EXPECT_THAT(calls, ElementsAre(2));
}

}  // namespace
}  // namespace arolla
