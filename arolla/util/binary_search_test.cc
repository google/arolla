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
#include "arolla/util/binary_search.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <random>
#include <vector>

#include "gtest/gtest.h"
#include "absl/types/span.h"

namespace arolla {
namespace {

size_t StdLowerBound(float value, absl::Span<const float> array) {
  return std::lower_bound(array.begin(), array.end(), value) - array.begin();
}

size_t StdLowerBound(double value, absl::Span<const double> array) {
  return std::lower_bound(array.begin(), array.end(), value) - array.begin();
}

size_t StdLowerBound(int32_t value, absl::Span<const int32_t> array) {
  return std::lower_bound(array.begin(), array.end(), value) - array.begin();
}

size_t StdLowerBound(int64_t value, absl::Span<const int64_t> array) {
  return std::lower_bound(array.begin(), array.end(), value) - array.begin();
}

size_t RlGallopingLowerBound(float value, absl::Span<const float> array) {
  return GallopingLowerBound(array.begin(), array.end(), value) - array.begin();
}

TEST(Algorithms, LowerBound_General) {
  for (int n : {0, 1, 5, 7, 100, 1000}) {
    std::vector<float> thresholds(n);
    for (int i = 0; i < n; ++i) {
      thresholds[i] = 2 * i + 1;
    }
    for (int i = 0; i < static_cast<int>(2 * thresholds.size()); ++i) {
      size_t expected = StdLowerBound(i, thresholds);
      ASSERT_EQ(LowerBound(i, thresholds), expected);
      ASSERT_EQ(RlGallopingLowerBound(i, thresholds), expected);
    }
    ASSERT_EQ(LowerBound(-10 * n, thresholds),
              StdLowerBound(-10 * n, thresholds));
    ASSERT_EQ(LowerBound(10 * n, thresholds),
              StdLowerBound(10 * n, thresholds));
  }
}

TEST(Algorithms, LowerBound_Duplicates) {
  for (int n : {2, 140}) {
    std::vector<float> thresholds(n, 0.);
    ASSERT_EQ(LowerBound(-1, thresholds), 0);
    ASSERT_EQ(LowerBound(0., thresholds), 0);
    ASSERT_EQ(LowerBound(1., thresholds), n);
    ASSERT_EQ(RlGallopingLowerBound(-1, thresholds), 0);
    ASSERT_EQ(RlGallopingLowerBound(0., thresholds), 0);
    ASSERT_EQ(RlGallopingLowerBound(1., thresholds), n);
  }
}

TEST(Algorithms, LowerBound_Infs) {
  const auto kInf = std::numeric_limits<float>::infinity();
  for (int n : {2, 140}) {
    std::vector<float> thresholds(n);
    for (int i = 0; i < n; ++i) {
      thresholds.push_back(i);
    }
    thresholds.front() = -kInf;
    thresholds.back() = kInf;
    ASSERT_EQ(LowerBound(-kInf, thresholds), StdLowerBound(-kInf, thresholds));
    ASSERT_EQ(LowerBound(kInf, thresholds), StdLowerBound(kInf, thresholds));
    ASSERT_EQ(RlGallopingLowerBound(kInf, thresholds),
              StdLowerBound(kInf, thresholds));
  }
}

TEST(Algorithms, LowerBound_Nan) {
  const auto kNan = std::numeric_limits<float>::quiet_NaN();
  const auto kInf = std::numeric_limits<float>::infinity();
  for (int n : {2, 140}) {
    std::vector<float> thresholds;
    for (int i = 0; i < n; ++i) {
      thresholds.push_back(i);
    }
    thresholds.front() = -kInf;
    thresholds.back() = kInf;
    ASSERT_EQ(LowerBound(kNan, thresholds), StdLowerBound(kNan, thresholds));
    ASSERT_EQ(RlGallopingLowerBound(kNan, thresholds),
              StdLowerBound(kNan, thresholds));
  }
}

size_t StdUpperBound(float value, absl::Span<const float> array) {
  return std::upper_bound(array.begin(), array.end(), value) - array.begin();
}

size_t StdUpperBound(double value, absl::Span<const double> array) {
  return std::upper_bound(array.begin(), array.end(), value) - array.begin();
}

size_t StdUpperBound(int32_t value, absl::Span<const int32_t> array) {
  return std::upper_bound(array.begin(), array.end(), value) - array.begin();
}

size_t StdUpperBound(int64_t value, absl::Span<const int64_t> array) {
  return std::upper_bound(array.begin(), array.end(), value) - array.begin();
}

TEST(Algorithms, UpperBound_General) {
  for (int n : {0, 1, 5, 7, 100, 1000}) {
    std::vector<float> thresholds(n);
    for (int i = 0; i < n; ++i) {
      thresholds[i] = 2 * i + 1;
    }
    for (int i = 0; i < static_cast<int>(2 * thresholds.size()); ++i) {
      ASSERT_EQ(UpperBound(i, thresholds), StdUpperBound(i, thresholds));
    }
    ASSERT_EQ(UpperBound(-10 * n, thresholds),
              StdUpperBound(-10 * n, thresholds));
    ASSERT_EQ(UpperBound(10 * n, thresholds),
              StdUpperBound(10 * n, thresholds));
  }
}

TEST(Algorithms, UpperBound_Duplicates) {
  for (int n : {2, 140}) {
    std::vector<float> thresholds(n, 0.);
    ASSERT_EQ(UpperBound(-1, thresholds), StdUpperBound(-1., thresholds));
    ASSERT_EQ(UpperBound(0., thresholds), StdUpperBound(0., thresholds));
  }
}

TEST(Algorithms, UpperBound_Infs) {
  const auto kInf = std::numeric_limits<float>::infinity();
  for (int n : {2, 140}) {
    std::vector<float> thresholds(n);
    for (int i = 0; i < n; ++i) {
      thresholds.push_back(i);
    }
    thresholds.front() = -kInf;
    thresholds.back() = kInf;
    ASSERT_EQ(UpperBound(-kInf, thresholds), StdUpperBound(-kInf, thresholds));
    ASSERT_EQ(UpperBound(kInf, thresholds), StdUpperBound(kInf, thresholds));
  }
}

TEST(Algorithms, UpperBound_Nan) {
  const auto kNan = std::numeric_limits<float>::quiet_NaN();
  const auto kInf = std::numeric_limits<float>::infinity();
  for (int n : {2, 140}) {
    std::vector<float> thresholds;
    for (int i = 0; i < n; ++i) {
      thresholds.push_back(i);
    }
    thresholds.front() = -kInf;
    thresholds.back() = kInf;
    ASSERT_EQ(UpperBound(kNan, thresholds), StdUpperBound(kNan, thresholds));
  }
}

// Stress tests

template <typename T>
std::vector<T> RandomVector(size_t seed, size_t size) {
  std::mt19937 gen(seed);
  std::vector<T> result(size);
  if constexpr (std::is_integral_v<T>) {
    std::uniform_int_distribution<T> uniform(0, 1 << 30);
    for (auto& x : result) {
      x = uniform(gen);
    }
  } else {
    std::uniform_real_distribution<T> uniform01;
    for (auto& x : result) {
      x = uniform01(gen);
    }
  }
  return result;
}

template <typename T>
std::vector<T> Sorted(std::vector<T> vec) {
  std::sort(vec.begin(), vec.end());
  return vec;
}

template <typename T>
using AlgoFn = std::function<size_t(T, const std::vector<T>&)>;

template <typename T>
void BinarySearchStressTest(size_t size, AlgoFn<T> algoFn,
                            AlgoFn<T> referenceAlgoFn) {
  const auto seed = 34 + size;
  const auto array = Sorted(RandomVector<T>(seed, size));
  for (auto value : RandomVector<T>(seed, 2 * size)) {
    const auto actual_value = algoFn(value, array);
    const auto expected_value = referenceAlgoFn(value, array);
    if (actual_value != expected_value) {
      ADD_FAILURE() << "Actual value: " << actual_value << '\n'
                    << "Expected value: " << expected_value << '\n'
                    << "size: " << size;
      return;
    }
  }
}

TEST(Algorithms, LowerBound_Stress) {
  for (int size : {10, 100, 1000, 100000}) {
    BinarySearchStressTest<float>(
        size,
        [](float value, absl::Span<const float> array) {
          return LowerBound(value, array);
        },
        [](float value, absl::Span<const float> array) {
          return StdLowerBound(value, array);
        });
    BinarySearchStressTest<float>(
        size,
        [](float value, absl::Span<const float> array) {
          return RlGallopingLowerBound(value, array);
        },
        [](float value, absl::Span<const float> array) {
          return StdLowerBound(value, array);
        });
    BinarySearchStressTest<double>(
        size,
        [](double value, absl::Span<const double> array) {
          return LowerBound(value, array);
        },
        [](double value, absl::Span<const double> array) {
          return StdLowerBound(value, array);
        });
    BinarySearchStressTest<int32_t>(
        size,
        [](int32_t value, absl::Span<const int32_t> array) {
          return LowerBound(value, array);
        },
        [](int32_t value, absl::Span<const int32_t> array) {
          return StdLowerBound(value, array);
        });
    BinarySearchStressTest<int64_t>(
        size,
        [](int64_t value, absl::Span<const int64_t> array) {
          return LowerBound(value, array);
        },
        [](int64_t value, absl::Span<const int64_t> array) {
          return StdLowerBound(value, array);
        });
  }
}

TEST(Algorithms, UpperBound_Stress) {
  for (int size : {10, 100, 1000, 100000}) {
    BinarySearchStressTest<float>(
        size,
        [](float value, absl::Span<const float> array) {
          return UpperBound(value, array);
        },
        [](float value, absl::Span<const float> array) {
          return StdUpperBound(value, array);
        });
    BinarySearchStressTest<double>(
        size,
        [](double value, absl::Span<const double> array) {
          return UpperBound(value, array);
        },
        [](double value, absl::Span<const double> array) {
          return StdUpperBound(value, array);
        });
    BinarySearchStressTest<int32_t>(
        size,
        [](int32_t value, absl::Span<const int32_t> array) {
          return UpperBound(value, array);
        },
        [](int32_t value, absl::Span<const int32_t> array) {
          return StdUpperBound(value, array);
        });
    BinarySearchStressTest<int64_t>(
        size,
        [](int64_t value, absl::Span<const int64_t> array) {
          return UpperBound(value, array);
        },
        [](int64_t value, absl::Span<const int64_t> array) {
          return StdUpperBound(value, array);
        });
  }
}

}  // namespace
}  // namespace arolla
