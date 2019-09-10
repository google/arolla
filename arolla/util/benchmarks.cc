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
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <random>
#include <vector>

#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/util/algorithms.h"
#include "arolla/util/binary_search.h"
#include "arolla/util/memory.h"
#include "arolla/util/status.h"

namespace arolla {

void BM_CopyBits(benchmark::State& state, int src_bit_offset,
                 int dest_bit_offset) {
  size_t num_bits = state.range(0);
  size_t src_words = (src_bit_offset + num_bits + 31) / 32;
  size_t dest_words = (dest_bit_offset + num_bits + 31) / 32;
  std::vector<uint32_t> src(src_words);
  std::vector<uint32_t> dest(dest_words);

  for (auto _ : state) {
    CopyBits(num_bits, src.data(), src_bit_offset, dest.data(),
             dest_bit_offset);
  }
}

void BM_CopyBitsWithShift(benchmark::State& state) {
  BM_CopyBits(state, 13, 21);
}

void BM_CopyBitsWithoutShift(benchmark::State& state) {
  BM_CopyBits(state, 13, 13);
}

namespace {

auto CreateBitGen() { return std::mt19937(34); }

template <typename T>
std::vector<T> Sorted(std::vector<T> vec) {
  std::sort(vec.begin(), vec.end());
  return vec;
}

template <typename T>
std::vector<T> RandomVector01(size_t n) {
  auto gen = CreateBitGen();
  std::uniform_real_distribution<T> uniform01;
  std::vector<T> result(n);
  for (auto& x : result) {
    x = uniform01(gen);
  }
  return result;
}

template <typename T>
std::vector<T> RandomVectorInt(size_t n, T mn, T mx) {
  auto gen = CreateBitGen();
  std::uniform_int_distribution<T> uniform(mn, mx);
  std::vector<T> result(n);
  for (auto& x : result) {
    x = uniform(gen);
  }
  return result;
}

}  // namespace

template <typename T, typename UpperBoundFn>
void UpperBoundBenchmark(benchmark::State& state, UpperBoundFn fn) {
  const auto thresholds = Sorted(RandomVector01<T>(state.range(0)));
  const auto values = RandomVector01<T>(65536);
  uint16_t i = 0;
  for (auto _ : state) {
    auto x = fn(values[i], thresholds);
    benchmark::DoNotOptimize(x);
    ++i;
  }
}

template <typename T, typename UpperBoundFn>
void UpperBoundBenchmarkInt(benchmark::State& state, UpperBoundFn fn) {
  const auto thresholds =
      Sorted(RandomVectorInt<T>(state.range(0), 0, 1 << 30));
  const auto values = RandomVectorInt<T>(65536, 0, 1 << 30);
  uint16_t i = 0;
  for (auto _ : state) {
    auto x = fn(values[i], thresholds);
    benchmark::DoNotOptimize(x);
    ++i;
  }
}

void BM_UpperBound_Float32(benchmark::State& state) {
  UpperBoundBenchmark<float>(
      state, [](float value, absl::Span<const float> thresholds) {
        return UpperBound(value, thresholds);
      });
}

void BM_UpperBound_Float64(benchmark::State& state) {
  UpperBoundBenchmark<double>(
      state, [](double value, absl::Span<const double> thresholds) {
        return UpperBound(value, thresholds);
      });
}

void BM_UpperBound_Int32(benchmark::State& state) {
  UpperBoundBenchmarkInt<int32_t>(
      state, [](int32_t value, absl::Span<const int32_t> thresholds) {
        return UpperBound(value, thresholds);
      });
}

void BM_UpperBound_Int64(benchmark::State& state) {
  UpperBoundBenchmarkInt<int64_t>(
      state, [](int64_t value, absl::Span<const int64_t> thresholds) {
        return UpperBound(value, thresholds);
      });
}

void BM_StdUpperBound_Float32(benchmark::State& state) {
  UpperBoundBenchmark<float>(
      state, [](float value, absl::Span<const float> thresholds) {
        return std::upper_bound(thresholds.begin(), thresholds.end(), value) -
               thresholds.begin();
      });
}

void BM_StdUpperBound_Float64(benchmark::State& state) {
  UpperBoundBenchmark<double>(
      state, [](double value, absl::Span<const double> thresholds) {
        return std::upper_bound(thresholds.begin(), thresholds.end(), value) -
               thresholds.begin();
      });
}

void BM_StdUpperBound_Int32(benchmark::State& state) {
  UpperBoundBenchmarkInt<int32_t>(
      state, [](int32_t value, absl::Span<const int32_t> thresholds) {
        return std::upper_bound(thresholds.begin(), thresholds.end(), value) -
               thresholds.begin();
      });
}

void BM_StdUpperBound_Int64(benchmark::State& state) {
  UpperBoundBenchmarkInt<int64_t>(
      state, [](int64_t value, absl::Span<const int64_t> thresholds) {
        return std::upper_bound(thresholds.begin(), thresholds.end(), value) -
               thresholds.begin();
      });
}

void BM_GallopingLowerBound_Float32(benchmark::State& state) {
  const float bias_coef = 1.0 / state.range(0);
  const auto thresholds = Sorted(RandomVector01<float>(state.range(1)));
  const auto values = RandomVector01<float>(65536);
  uint16_t i = 0;
  for (auto _ : state) {
    auto x = GallopingLowerBound<float>(thresholds.begin(), thresholds.end(),
                                        values[i] * bias_coef);
    benchmark::DoNotOptimize(x);
    ++i;
  }
}

void BM_AlignedAllocDefaultAlignment(benchmark::State& state) {
  for (auto _ : state) {
    auto x = AlignedAlloc(Alignment{sizeof(size_t)}, 32);
    benchmark::DoNotOptimize(x);
  }
}
void BM_AlignedAllocBigAlignment(benchmark::State& state) {
  for (auto _ : state) {
    auto x = AlignedAlloc(Alignment{64}, 32);
    benchmark::DoNotOptimize(x);
  }
}

void BM_IsAlignedPtr(benchmark::State& state) {
  std::vector<MallocPtr> memory_blocks(65536);
  for (auto& item : memory_blocks) {
    item = AlignedAlloc(Alignment{16}, 16);
  }
  uint16_t i = 0;
  for (auto _ : state) {
    auto x = IsAlignedPtr(Alignment{16}, memory_blocks[i].get());
    benchmark::DoNotOptimize(x);
    ++i;
  }
}

void BM_CheckInputStatusOk(benchmark::State& state) {
  absl::StatusOr<int> x = 5;
  absl::Status y = absl::OkStatus();
  for (auto _ : state) {
    CHECK_OK(CheckInputStatus(x, y, 7.0f));
  }
}

void BM_UnStatusCaller(benchmark::State& state) {
  absl::StatusOr<int> x = 5;
  absl::StatusOr<int> y = 6;
  auto fn = [](int x, int y, float z) { return x + y + z; };
  UnStatusCaller<decltype(fn)> fn_wrap{fn};
  for (auto _ : state) {
    CHECK_OK(fn_wrap(x, y, 7.0f).status());
  }
}

// Show supremacy of UpperBound():
BENCHMARK(BM_UpperBound_Float32)
    ->DenseRange(1, 16)
    ->Arg(32)
    ->RangeMultiplier(10)
    ->Range(100, 100'000);
BENCHMARK(BM_StdUpperBound_Float32)
    ->DenseRange(1, 16)
    ->Arg(32)
    ->RangeMultiplier(10)
    ->Range(100, 100'000);

BENCHMARK(BM_UpperBound_Float64)
    ->DenseRange(1, 16)
    ->Arg(32)
    ->RangeMultiplier(10)
    ->Range(100, 100'000);
BENCHMARK(BM_StdUpperBound_Float64)
    ->DenseRange(1, 16)
    ->Arg(32)
    ->RangeMultiplier(10)
    ->Range(100, 100'000);

BENCHMARK(BM_UpperBound_Int32)
    ->DenseRange(1, 16)
    ->Arg(32)
    ->RangeMultiplier(10)
    ->Range(100, 100'000);
BENCHMARK(BM_StdUpperBound_Int32)
    ->DenseRange(1, 16)
    ->Arg(32)
    ->RangeMultiplier(10)
    ->Range(100, 100'000);

BENCHMARK(BM_UpperBound_Int64)
    ->DenseRange(1, 16)
    ->Arg(32)
    ->RangeMultiplier(10)
    ->Range(100, 100'000);
BENCHMARK(BM_StdUpperBound_Int64)
    ->DenseRange(1, 16)
    ->Arg(32)
    ->RangeMultiplier(10)
    ->Range(100, 100'000);

BENCHMARK(BM_GallopingLowerBound_Float32)
    ->ArgPair(1, 1000)
    ->ArgPair(4, 1000)
    ->ArgPair(16, 1000)
    ->ArgPair(64, 1000)
    ->ArgPair(256, 1000)
    ->ArgPair(512, 1000);

// Show degradation of UpperBound():
// (* presumably caused by the  lack of memory prefetching, and
//    because cache-misses are more costly than branch misprediction *)
BENCHMARK(BM_UpperBound_Float32)->Arg(1'000'000)->Arg(10'000'000);
BENCHMARK(BM_StdUpperBound_Float32)->Arg(1'000'000)->Arg(10'000'000);

BENCHMARK(BM_AlignedAllocDefaultAlignment);
BENCHMARK(BM_AlignedAllocBigAlignment);
BENCHMARK(BM_IsAlignedPtr);
BENCHMARK(BM_CheckInputStatusOk);
BENCHMARK(BM_UnStatusCaller);

}  // namespace arolla
