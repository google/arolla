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
#include <cstddef>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"
#include "absl/hash/hash.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "arolla/util/lru_cache.h"

namespace arolla {
namespace {

void BM_LookupHit(benchmark::State& state) {
  const size_t capacity = state.range(0);
  LruCache<int, int> cache(capacity);
  for (size_t i = 0; i < capacity; ++i) {
    cache.Put(i, static_cast<int>(i));
  }
  size_t key = 0;
  for (auto _ : state) {
    const int* val = cache.LookupOrNull(key);
    benchmark::DoNotOptimize(val);
    key = (key + 1) % capacity;
  }
}
BENCHMARK(BM_LookupHit)->Arg(10)->Arg(100)->Arg(1000);

void BM_LookupMiss(benchmark::State& state) {
  const size_t capacity = state.range(0);
  LruCache<int, int> cache(capacity);
  for (size_t i = 0; i < capacity; ++i) {
    cache.Put(i, static_cast<int>(i));
  }
  size_t key = 0;
  for (auto _ : state) {
    const int* val = cache.LookupOrNull(capacity + key);
    benchmark::DoNotOptimize(val);
    key = (key + 1) % capacity;
  }
}
BENCHMARK(BM_LookupMiss)->Arg(10)->Arg(100)->Arg(1000);

void BM_PutInsertEvict(benchmark::State& state) {
  const size_t capacity = state.range(0);
  LruCache<int, int> cache(capacity);
  int key = 0;
  for (auto _ : state) {
    const int* val = cache.Put(key++, 1);
    benchmark::DoNotOptimize(val);
  }
}
BENCHMARK(BM_PutInsertEvict)->Arg(10)->Arg(100)->Arg(1000);

void BM_PutOverwrite(benchmark::State& state) {
  const size_t capacity = state.range(0);
  LruCache<int, int> cache(capacity);
  for (size_t i = 0; i < capacity; ++i) {
    cache.Put(i, static_cast<int>(i));
  }
  size_t key = 0;
  for (auto _ : state) {
    const int* val = cache.Put(key, 42);
    benchmark::DoNotOptimize(val);
    key = (key + 1) % capacity;
  }
}
BENCHMARK(BM_PutOverwrite)->Arg(10)->Arg(100)->Arg(1000);

void BM_LookupHit_StringView(benchmark::State& state) {
  const size_t capacity = state.range(0);
  LruCache<std::string, int, absl::Hash<absl::string_view>> cache(capacity);
  std::vector<std::string> keys;
  keys.reserve(capacity);
  for (size_t i = 0; i < capacity; ++i) {
    keys.push_back(absl::StrCat("key_", i));
    cache.Put(keys.back(), static_cast<int>(i));
  }
  size_t idx = 0;
  for (auto _ : state) {
    absl::string_view key = keys[idx];
    const int* val = cache.LookupOrNull(key);
    benchmark::DoNotOptimize(val);
    idx = (idx + 1) % capacity;
  }
}
BENCHMARK(BM_LookupHit_StringView)->Arg(10)->Arg(100)->Arg(1000);

}  // namespace
}  // namespace arolla
