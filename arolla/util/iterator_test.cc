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
#include "arolla/util/iterator.h"

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

using ::testing::ElementsAre;

namespace arolla {
namespace {

// "Array" which generates double values based on the array index.
class FloatGeneratorArray {
 public:
  // Minimal type definitions required by ConstArrayIterator.
  using value_type = float;
  using size_type = int64_t;
  using difference_type = int64_t;
  using const_iterator = ConstArrayIterator<FloatGeneratorArray>;

  explicit FloatGeneratorArray(int64_t size) : size_(size) {}

  // Element accessor method. Note in this case we are returning by value
  // rather than reference (since value is computed).
  float operator[](int64_t i) const { return i * 10.0f; }

  // Having these methods let's us iterate over the values.
  const_iterator begin() const { return const_iterator{this, 0}; }
  const_iterator end() const { return const_iterator{this, size_}; }

 private:
  int64_t size_;
};

TEST(Iterator, IteratorOverFloatGeneratorArray) {
  FloatGeneratorArray array(10);

  auto iter1 = array.begin();
  EXPECT_EQ(*(iter1++), 0.0f);                 // post-increment
  EXPECT_EQ(*iter1, 10.0f);                    // dereference
  EXPECT_EQ(*(++iter1), 20.0f);                // pre-increment
  EXPECT_EQ(*(iter1--), 20.0f);                // post-decrement
  EXPECT_EQ(*(--iter1), 0.0f);                 // pre-decrement
  EXPECT_EQ(iter1[5], 50.0f);                  // operator[]
  EXPECT_EQ(iter1, array.begin());             // equality
  EXPECT_NE(iter1, array.end());               // inequality
  EXPECT_LT(array.begin(), array.end());       // less-than
  EXPECT_GT(array.end(), array.begin());       // greater-than
  EXPECT_EQ(*(iter1 += 9), 90.0f);             // plus-equals
  EXPECT_EQ(*(iter1 -= 2), 70.0f);             // minus-equals
  EXPECT_EQ(iter1, array.begin() + 7);         // addition
  EXPECT_EQ(iter1, 7 + array.begin());         // addition, reverse order
  EXPECT_EQ(iter1, array.end() - 3);           // subtraction
  EXPECT_EQ(iter1 - array.begin(), 7);         // iterator difference
  EXPECT_LE(array.begin() + 10, array.end());  // less-than-or-equal
  EXPECT_GE(array.end(), array.begin() + 10);  // greater-than-or-equal

  // It's a real container!
  EXPECT_THAT(array, ElementsAre(0.0f, 10.0f, 20.0f, 30.0f, 40.0f, 50.0f, 60.0f,
                                 70.0f, 80.0f, 90.0f));
}

TEST(Iterator, Algorithms) {
  FloatGeneratorArray array(5);

  // Copy values into an array.
  std::vector<float> copy1(array.begin(), array.end());
  EXPECT_THAT(copy1, ElementsAre(0.f, 10.f, 20.f, 30.f, 40.f));

  // Copy filtered values into an array.
  std::vector<float> copy2;
  std::copy_if(array.begin(), array.end(), std::back_inserter(copy2),
               [](float value) { return value > 12.f; });
  EXPECT_THAT(copy2, ElementsAre(20.f, 30.f, 40.f));

  // Copy using range-based for loop.
  std::vector<float> copy3;
  for (float val : array) {
    copy3.push_back(val);
  }
  EXPECT_THAT(copy3, ElementsAre(0.f, 10.f, 20.f, 30.f, 40.f));
}

TEST(Iterator, IteratorOverStdVector) {
  // It doesn't really make any sense to use this iterator on std::vector, but
  // it's useful for testing. Note in this case, the iterator's reference type
  // is really a reference. Also, we demonstrate that we are using the standard
  // container type names (value_type, size_type, etc.).
  using StringContainer = std::vector<std::string>;
  StringContainer strings{"one", "two", "three"};

  ConstArrayIterator<StringContainer> my_iter(&strings, 0);
  EXPECT_EQ(*my_iter, "one");
  EXPECT_EQ(my_iter[2], "three");
  EXPECT_EQ(my_iter->size(), 3);  // -> operator

  // Demonstrate interoperability with standard operators.
  auto strings_copy = strings;
  EXPECT_TRUE(std::equal(strings_copy.begin(), strings_copy.end(), my_iter));

  // And with gunit.
  EXPECT_THAT(strings, ElementsAre("one", "two", "three"));
}

}  // namespace
}  // namespace arolla
