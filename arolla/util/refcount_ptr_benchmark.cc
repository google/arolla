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
#include <array>
#include <memory>
#include <utility>

#include "benchmark/benchmark.h"
#include "arolla/util/refcount_ptr.h"

namespace arolla {
namespace {

struct Object {
  int value;
};

struct RefcountedObject : RefcountedBase {
  int value;
};

using RefcountedObjectPtr = RefcountPtr<RefcountedObject>;

void BM_StdSharedPtr_Alloc_Dealloc(benchmark::State& state) {
  for (auto _ : state) {
    auto ptr = std::make_shared<Object>();
    benchmark::DoNotOptimize(ptr->value);
  }
}

BENCHMARK(BM_StdSharedPtr_Alloc_Dealloc);

void BM_RefcountPtr_Alloc_Dealloc(benchmark::State& state) {
  for (auto _ : state) {
    auto ptr = RefcountedObjectPtr::Own(std::make_unique<RefcountedObject>());
    benchmark::DoNotOptimize(ptr->value);
  }
}

BENCHMARK(BM_RefcountPtr_Alloc_Dealloc);

void BM_StdSharedPtr_Alloc_Copy_Dealloc(benchmark::State& state) {
  for (auto _ : state) {
    auto ptr1 = std::make_shared<Object>();
    benchmark::DoNotOptimize(ptr1->value);
    auto ptr2 = ptr1;
    benchmark::DoNotOptimize(ptr2->value);
  }
}

BENCHMARK(BM_StdSharedPtr_Alloc_Copy_Dealloc);

void BM_RefcountPtr_Alloc_Copy_Dealloc(benchmark::State& state) {
  for (auto _ : state) {
    auto ptr1 = RefcountedObjectPtr::Own(std::make_unique<RefcountedObject>());
    benchmark::DoNotOptimize(ptr1->value);
    auto ptr2 = ptr1;
    benchmark::DoNotOptimize(ptr2->value);
  }
}

BENCHMARK(BM_RefcountPtr_Alloc_Copy_Dealloc);

void BM_StdSharedPtr_Alloc_Move_Dealloc(benchmark::State& state) {
  for (auto _ : state) {
    auto ptr1 = std::make_shared<Object>();
    std::shared_ptr<Object> ptr2;
    benchmark::DoNotOptimize(ptr1->value);
    benchmark::DoNotOptimize(ptr2);
    ptr2 = std::move(ptr1);
    benchmark::DoNotOptimize(ptr2->value);
  }
}

BENCHMARK(BM_StdSharedPtr_Alloc_Move_Dealloc);

void BM_RefcountPtr_Alloc_Move_Dealloc(benchmark::State& state) {
  for (auto _ : state) {
    auto ptr1 = RefcountedObjectPtr::Own(std::make_unique<RefcountedObject>());
    RefcountedObjectPtr ptr2;
    benchmark::DoNotOptimize(ptr1->value);
    benchmark::DoNotOptimize(ptr2);
    ptr2 = std::move(ptr1);
    benchmark::DoNotOptimize(ptr2->value);
  }
}

BENCHMARK(BM_RefcountPtr_Alloc_Move_Dealloc);

void BM_StdSharedPtr_Copy_Reset_100(benchmark::State& state) {
  auto ptr = std::make_shared<Object>();
  for (auto _ : state) {
    std::array<std::shared_ptr<Object>, 100> array;
    for (auto& item : array) {
      item = ptr;
    }
  }
}

BENCHMARK(BM_StdSharedPtr_Copy_Reset_100);

void BM_RefcountPtr_Copy_Reset_100(benchmark::State& state) {
  auto ptr = RefcountedObjectPtr::Own(std::make_unique<RefcountedObject>());
  for (auto _ : state) {
    std::array<RefcountedObjectPtr, 100> array;
    for (auto& item : array) {
      item = ptr;
    }
  }
}

BENCHMARK(BM_RefcountPtr_Copy_Reset_100);

void BM_StdSharedPtr_Alloc_Swap_Dealloc(benchmark::State& state) {
  for (auto _ : state) {
    auto ptr1 = std::make_shared<Object>();
    std::shared_ptr<Object> ptr2;
    benchmark::DoNotOptimize(ptr1->value);
    benchmark::DoNotOptimize(ptr2);
    using std::swap;
    swap(ptr1, ptr2);
    benchmark::DoNotOptimize(ptr1);
    benchmark::DoNotOptimize(ptr2->value);
  }
}

BENCHMARK(BM_StdSharedPtr_Alloc_Swap_Dealloc);

void BM_RefcountPtr_Alloc_Swap_Dealloc(benchmark::State& state) {
  for (auto _ : state) {
    auto ptr1 = RefcountedObjectPtr::Own(std::make_unique<RefcountedObject>());
    RefcountedObjectPtr ptr2;
    benchmark::DoNotOptimize(ptr1->value);
    benchmark::DoNotOptimize(ptr2);
    using std::swap;
    swap(ptr1, ptr2);
    benchmark::DoNotOptimize(ptr1);
    benchmark::DoNotOptimize(ptr2->value);
  }
}

BENCHMARK(BM_RefcountPtr_Alloc_Swap_Dealloc);

}  // namespace
}  // namespace arolla
