# DenseArray

## Overview

`DenseArray` is a container for immutable data with support of missing values.
`DenseArray<T>` is an analogue of `const std::vector<std::optional<T>>`.

The package also includes:

*   Simple and performance efficient way to define operations on DenseArray's.
*   Helpers to create and to iterate over DenseArray.
*   QType for DenseArray and all the stuff that is needed to use it in QExpr.

## Data structure

DenseArray consists of `values`, `bitmap`, and `bitmap_bit_offset`:

```c++
template <typename T>
struct DenseArray {
  Buffer<T> values;
  Buffer<int32_t> bitmap;  // Presence bitmap. Empty means all present.
  int bitmap_bit_offset = 0;  // Offset of the first element bit in the bitmap.
  ...
};
```

Information about missing values is stored as a bitmap packed in a
`Buffer<int32_t>` where every buffer value stores 32 bits. If a bit in `bitmap`
is not set, the value is missing and the corresponding element in`values` is in
undefined state. Empty `bitmap` (i.e. `bitmap.size()==0`) means that all values
are present. Size of `DenseArray` is, by definition, the size of its `values`
buffer. So empty `values` means empty `DenseArray`.

`bitmap_bit_offset` should always be in range `[0, 31)`. In most cases it is
zero. Non-zero value means that `bitmap` is not aligned with `values`. It can
happen if the array is a slice of a bigger array or an external data.

Since `DenseArray` is based on `arolla::Buffer`, for `DenseArray` also applies
that:

*   Data is immutable.
*   Data is accessed via shared pointer, so copying is not expensive.
*   There is a special implementation for string types. All chars are stored in
    a single buffer, so no separate allocation per string is required.

## Creating DenseArray

There are several ways to create `DenseArray`. Most of them accepts a custom
`RawBufferFactory` as an optional argument.

**From span**

Intended for tests and not performance critical code. Always copies data.

```c++
DenseArray<int> array = CreateDenseArray<int>({1, std::nullopt, 2, 3});
```

**Constant array**

```c++
// 10 links to the same data
DenseArray<Text> arr1 = CreateConstDenseArray<Text>(/*size=*/ 10, "Hello");
// 10 missing values
DenseArray<Text> arr2 = CreateEmptyDenseArray<Text>(/*size=*/ 10);
```

**From container of std::optional**

```c++
// With type casting `float` -> `double`.
std::vector<std::optional<float>> data = GetData1();
DenseArray<double> arr = CreateDenseArray<double>(data.begin(), data.end());
```

**Full array from container of non-optional**

```c++
std::vector<float> data = GetData();
DenseArray<double> arr1 = CreateFullDenseArray<double>(data);
DenseArray<double> arr2 = CreateFullDenseArray<double>(data.begin(), data.end());
// std::vector<T> can be moved inside without a memory allocation.
DenseArray<double> arr3 = CreateFullDenseArray<double>(std::move(data));
```

**DenseArrayBuilder**

Universal, but not the most efficient way.

```c++
DenseArrayBuilder<float> builder(/*size=*/ 10);
builder.Set(2, 3.0);
builder.Set(7, 4.0);
builder.Set(9, 5.0);
DenseArray<float> array = std::move(builder).Build();
```

**"Almost full" bitmap builder**

Efficient if there are only a few missing values. Values and bitmap are created
separately. Bitmap will be empty (i.e. no allocation) if no values are missing.

```c++
bitmap::AlmostFullBuilder bitmap_builder(10);
Buffer<float>::Builder values_builder(10);
auto values_inserter = values_builder.GetInserter();
for (int i = 0; i < 10) {
  values_inserter.Add(i * i);
  if ((i & 3) == 0) bitmap_builder.AddMissed(i);
}
DenseArray<float> array{std::move(values_builder).Build(),
                        std::move(bitmap_builder).Build()};
```

**Bitmap builder with a callback**

```c++
bitmap::Builder bitmap_builder(10);
Buffer<float>::Builder values_builder(10);
auto values_inserter = values_builder.GetInserter();
std::array<int> data{10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
bitmap_builder.AddForEach(data, [&](int i) {
  values_inserter.Add(i * i);
  return (i & 3) != 0;
});
DenseArray<float> array{std::move(values_builder).Build(),
                        std::move(bitmap_builder).Build()};
// result: { 100, 81, nullopt, 49, 36, 25, nullopt, 9, 4, 1 }
```

**From external data without ownership**

```c++
absl::Span<const float> values = ...;
RawBufferPtr holder = nullptr;  // no ownership
DenseArray<float> array{Buffer<float>(holder, values)};
```

**Transfer ownership from std::vector**

Converts non-optional `std::vector` to `DenseArray` without copying data.
Doesn't work for `bool` and string types (`std::string`, `Bytes`, `Text`).

```c++
std::vector<int> v;
...
DenseArray<int> array{Buffer<int>::Create(std::move(v))};
```

**Or using any other way to create arolla::Buffer**

See `memory/buffer.h`.

For example `DenseArray<Bytes>` with string interning:

```c++
StringsBuffer::Builder values_builder(7);
bitmap::AlmostFullBuilder bitmap_builder(7);
values_builder.Set(0, "abaca");
values_builder.Set(1, "bbaa");
bitmap_builder.AddMissed(2);
values_builder.Copy(0, 3);
values_builder.Copy(1, 4);
bitmap_builder.AddMissed(5);
values_builder.Copy(0, 6);
DenseArray<Bytes> array{std::move(values_builder).Build(),
                        std::move(bitmap_builder).Build()};
// array is {"abaca", "bbaa", nullopt, "abaca", "bbaa", nullopt, "abaca"}.
// array[0], array[3], array[6] point to the same memory.
```

From another `DenseArray` using `ReshuffleBuilder` (preserves string interning):

```c++
template <typename T>
DenseArray<T> present_values(const DenseArray<T>& array) {
  typename Buffer<T>::ReshuffleBuilder values_builder(
      array.size(), array.values, std::nullopt);
  int64_t new_id = 0;

  array.ForEachPresent([&](int64_t old_id, view_type_t<T> /*value*/) {
    values_builder.CopyValue(new_id++, old_id);
  });

  return {std::move(values_builder).Build(new_id)};
}
```

## Accessing elements and iterating over DenseArray

DenseArray (as well as Buffer) returns `view_type_t<T>` rather than `T`. For all
trivial types it is the same as `T`, but for all string types (i.e.
`std::string`, `arolla::Bytes`, `arolla::Text`) it is `absl::string_view` (see
`util/view_types.h`). Views are valid only if the underlying `values` buffer is
valid.

**Accessing elements**

```c++
auto array = CreateDenseArray<Text>({"Hello", std::nullopt, "World"});

array[0];  // OptionalValue<absl::string_view>("Hello")
array[1];  // OptionalValue<absl::string_view>(std::nullopt)

array.present(0);  // true
array.present(1);  // false

array.values[0];  // absl::string_view("Hello")

// not defined, but doesn't fail and points to a valid memory
array.values[1];
```

**Iterating**

It is possible to iterate over DenseArray with a simple loop:

```c++
int64_t id = 0;
for (OptionalValue<view_type<T>> v : array) {
  // When v.present is false, v.value is in an undefined state.
  if (v.present) {
    std::cout << id << " -> " << v.value << std::endl;
  } else {
    std::cout << id << " is missing" << std::endl;
  }
  id++;
}
```

But `DenseArray::ForEach` is a more efficient. Since Bitmap is a
`Buffer<uint32_t>` and one `uint32_t` has 32 bits corresponding to 32 values,
when accessing value by index, we need to 1. get the corresponding `uint32_t`
from the bitmap, 2. get the corresponding bit index in `uint32_t`, and 3. get
the specific bit value in `uint32_t`.

In comparison, `ForEach` loads one `uint32_t` for every 32 values and get bit
values from `uint32_t` sequentially.

```c++
array.ForEach([&](int64_t id, bool present, view_type_t<T> value) {
  if (present) {
    std::cout << id << " -> " << value << std::endl;
  } else {
    // here `value` is in an unspecified state. In case of string_view value
    // it is guaranteed that it points to valid memory, but no guarantee on what
    // it contains.
    std::cout << id << " is missing" << std::endl;
  }
});
```

`ForEachPresent` is almost the same as `ForEach`, but does the `if (present)`
internally:

```c++
array.ForEachPresent([&](int64_t id, view_type_t<T> value) {
  std::cout << id << " -> " << value << std::endl;
});
```

**Iterating over several DenseArrays at same time**

In `dense_array/ops/dense_ops.h` there are functions `DenseArraysForEach` and
`DenseArraysForEachPresent` that are similar to `ForEach`/`ForEachPresent`, but
work with multiple arrays.

`DenseArraysForEachPresent` evaluates the callback only for rows in which all
non-optional arguments of the callback are present.

Usage example:

```c++
DenseArraysForEachPresent(
  [&](int64_t id, float v1, absl::string_view v2, OptionalValue<int> v3) {
    // Since v3 is OptionalValue, rows with missing v3 are not skipped.
    if (v3.present) {
      std::cout << absl::StrFormat("arr1[%d]=%f, arr2[%d]=%s, arr3[%d]=%d",
                                   id, v1, id, v2, id, v3.value) << std::endl;
    } else {
      std::cout << absl::StrFormat("arr1[%d]=%f, arr2[%d]=%s, arr3[%d]=nullopt",
                                   id, v1, id, v2, id) << std::endl;
    }
  }, arr1, arr2, arr3);
```

## Operations on DenseArray

Performance efficient operations on DenseArray's are the most important part of
the package. It is heavily optimized and in most cases the operations are
significantly faster than a simple loop over several
`std::vector<std::optional<T>>`.

### Pointwise operations

Main header is `dense_array/ops/dense_ops.h`.

Pointwise operation can be defined by a functor or by a lambda function. All
input arrays must be the same size.

**Simple case**

Lambda function:

```c++
auto arr1 = CreateDenseArray<int>({1, {}, 2, 3});
auto arr2 = CreateDenseArray<int>({5, 2, {}, 1});
auto op = CreateDenseOp([](int a, int b) { return a + b; });
ASSIGN_OR_RETURN(DenseArray<int> res, op(arr1, arr2));
// res: {6, std::nullopt, std::nullopt, 4}
```

Functor:

```c++
template <class T>
struct Sum3Fn {
  // The function itself shouldn't be templated because it's prototype is used
  // for types deduction.
  T operator()(T a, T b, T c) { return a + b + c; }
};

auto op = CreateDenseOp(Sum3Fn<int>());
ASSIGN_OR_RETURN(DenseArray<int> res, op(arr1, arr2, arr3));
```

**Optional arguments**

If some arguments of a functor are `OptionalValue`, then the operation will be
evaluated even for rows where the corresponding values are missing.

For example here `a` is optional and `b` is required:

```c++
auto arr1 = CreateDenseArray<int>({1, {}, 2, 3});
auto arr2 = CreateDenseArray<int>({5, 2, {}, 1});
auto fn = [](OptionalValue<int> a, int b) {
  return a.present ? a.value : b;
};
auto op = CreateDenseOp(fn);
ASSIGN_OR_RETURN(DenseArray<int> res, op(arr1, arr2));
// res: {1, 2, std::nullopt, 3}
```

**Missing result**

Result can be missing even if all arguments are present:

```c++
auto fn = [](int a, int b) -> OptionalValue<int> {
  if (b == 0) return std::nullopt;
  return a / b;
};
auto div_op = CreateDenseOp(fn);
```

**Status propagation**

`CreateDenseOp` can propagate `Status`:

```c++
auto fn = [](float a) -> absl::StatusOr<float> {
  if (a < 0) {
    return absl::InvalidArgumentError("value should be >= 0");
  } else {
    return std::sqrt(a);
  }
};
auto op = CreateDenseOp(fn);
ASSIGN_OR_RETURN(DenseArrat<float> res, op(array));
```

**Strings**

String (`Text`, `Bytes`, `std::string`) arguments should be used in the form of
`absl::string_view` or `OptionalValue<absl::string_view>`:

```c++
auto fn = [](absl::string_view a, OptionalValue<absl::string_view> b) -> Text {
  if (b.present) {
    return Text(absl::StrCat(a, b.value));
  } else {
    return Text(a);
  }
};
auto op = CreateDenseOp(fn);

DenseArray<Text> array1 = ...;
DenseArray<Text> array2 = ...;
ASSIGN_OR_RETURN(DenseArray<Text> res, op(array1, array2));
```

In the example above there is one allocation per row (i.e. creation of the
`Text`). It can be avoided if the function returns `absl::string_view`:

```c++
struct StrCatFn {
  std::string buf;

  absl::string_view operator()(absl::string_view a,
                               OptionalValue<absl::string_view> b) {
    if (b.present) {
      buf.clear();
      absl::StrAppend(&buf, a, b.value);
      return buf;
    } else {
      return a;
    }
  }
};
auto op = CreateDenseOp<StrCatFn, Text>(StrCatFn());

DenseArray<Text> array1 = ...;
DenseArray<Text> array2 = ...;
ASSIGN_OR_RETURN(DenseArray<Text> res, op(array1, array2));
```

Important notes about the last example:

*   Here `op` can not be used simultaneously from several threads because it has
    a buffer inside. Use `thread_local` with `std::string buf` if thread safety
    is needed.
*   OptionalValue propagation and Status propagation are supported here as well.
    In these cases the return value of the function can be
    `OptionalValue<absl::string_view>`, `StatusOr<absl::string_view>`, or even
    `StatusOr<OptionalValue<absl::string_view>>`.
*   Type of the output DenseArray can not be deduced from the return type of the
    function, so it should be explicitly specified as template argument of
    `CreateDenseOp`. Otherwise `DenseArray<absl::string_view>` will be created.

**From operation on spans**

It also possible to define an unary or binary operation from a spanwise
function. In most cases it is not needed, but can be useful if we want to do
something non-trivial in the inner loop (e.g., using external library like
Eigen; see `qexpr/operators/math/batch_arithmetic.h`).

```c++
auto span_fn = [](absl::Span<float> res,
                  absl::Span<const float> arg1,
                  absl::Span<const int> arg2) {
  // See: openmp.org
  #pragma omp parallel for
  for (size_t i = 0; i < res.size(); ++i) {
    res[i] = arg1[i] + arg2[i];
  }
};
auto op = CreateDenseBinaryOpFromSpanOp<float>(span_fn);
ASSIGN_OR_RETURN(DenseArray<float> res, op(array1, array2));
```

It doesn't support optional arguments, optional result, and status propagation.
It can return error status only if array1.size() != array2.size(). The output
presence bitmap is calculated simply as an intersection of the input bitmaps.

#### Performance

**Allocations**

An important way to speed up calculations on DenseArray's is
`UnsafeArenaBufferFactory`. It is especially important for small batches (array
sizes < 100) because without it a single allocation can take more time then the
actual calculation. It is called "unsafe" because all allocated buffers become
invalid after removing or resetting the arena.

Also in some cases it can be important to have bitmaps of input arrays "unowned"
(see `memory/buffer.h`), because copying of a shared pointer is relatively
expensive. All buffers created by `UnsafeArenaBufferFactory` are unowned.

**DenseOpFlags**

`CreateDenseOp` accepts optional flags that have some influence on performance:

*   `DenseOpFlags::kRunOnMissing` - use it for inexpensive operations without
    side effects. If this flag is set, the operation can be evaluated even for
    rows with missing required arguments, but the result will be ignored.
*   `DenseOpFlags::kNoBitmapOffset` - use it if there is a guarantee that
    `bitmap_bit_offset` is zero for all input arrays. It allows to slightly
    reduce binary size and speed up the evaluation.
*   `DenseOpFlags::kNoSizeValidation` - skip the validation that all arguments
    have the same size. Use it only if it is guaranteed that sizes are equal.

Here is an example with arena buffer factory and flags:

```c++
auto fn = [](float a, float b) -> OptionalValue<float> {
  if (a < 0 || b < 0) {
    return std::nullopt;
  } else {
    return a * b;
  }
};

UnsafeArenaBufferFactory buffer_factory(100 * 1024);  // page size is 100KB
auto op = CreateDenseOp<DenseOpFlags::kRunOnMissing |
                        DenseOpFlags::kNoBitmapOffset |
                        DenseOpFlags::kNoSizeValidation>(fn, &buffer_factory);

CHECK_EQ(array1.size(), array2.size())
DenseArray<float> tmp1 = op(array1, array2);
DenseArray<float> tmp2 = op(tmp1, array1);

DenseArray<float> result{tmp2.values.DeepCopy(), tmp2.bitmap.DeepCopy()};

buffer_factory.Reset();  // tmp1 and tmp2 become invalid at this point.
```

Note that `tmp1`, `tmp2` are `DenseArray<float>` rather than
`StatusOr<DenseArray<float>>` because size validation is disabled by
`kNoSizeValidation`.

### Group operations

Group operations are very common in Arolla. They describe operations such as
computing the sum of all columns row-by-row over a matrix.

Group operations are more flexible though as each "row" can have a different
number of "columns". In Arolla we refer to the "rows" as PARENTS and the
"columns" as CHILD.

Depending on the
`AccumulatorType`
the group operation is defined as:

-   `kAggregator`: one output for each PARENT is produced. The output is read
    after one full CHILD row is added.
-   `kPartial`: one output for each CHILD is produced. The output is read after
    each CHILD is added.
-   `kFull`: one output for each CHILD is produced. The output is read after
    after one full CHILD row is added.

See detailed description in
`qexpr/aggregation_ops_interface.h`.

Group operations on DenseArray's are implemented in a similar way as pointwise
operations, but requires an `Accumulator` instead of a lambda function. Main
class `DenseGroupOps` is defined in `dense_array/ops/dense_group_ops.h`.

Supports optional arguments and optional result. Status propagation is not
supported.

An example with `kAggregator` (it's recommended to read comments in
`ops_interface.h` first):

```c++
// For each parent row returns:
// "{prefix[0]}{value[0][0]} ({comment[0][0]}) {value[0][1]} ..."
// "{prefix[1]}{value[1][0]} ({comment[1][0]}) {value[1][1]} ..."
// ...
//
// Example:
// auto prefixes = CreateDenseArray<Text>(
//       {Text("P0 "), Text("P1 "), Text("P2 "), std::nullopt});
// auto values = CreateDenseArray<Text>(
//       {Text("V0"), std::nullopt, Text("V2"), Text("V3"), Text("V4")});
// auto comments = CreateDenseArray<Text>(
//       {std::nullopt, Text("C1"), std::nullopt, Text("C3"), Text("C4")};
// auto edge = DenseArrayEdge::FromMapping(
//       CreateDenseArray<int64_t>({1, 1, 2, 3, 3}), /*group_size=*/4));
//
// auto exp_output = CreateDenseArray<Text>(
//       {Text("P0 "), Text("P1 V0 "), Text("P2 V2"), Text("V3 (C3) V4 (C4)")});
class AggTextAccumulator final
    : public Accumulator<AccumulatorType::kAggregator,
                         Text,
                         meta::type_list<OptionalValue<Text>>,
                         meta::type_list<Text, OptionalValue<Text>>> {
 public:
  // Prepares accumulator for a new PARENT.
  void Reset(OptionalValue<absl::string_view> prefix) final {
    res_.clear();
    if (prefix.present) res_.append(prefix.value);
  }

  // Called to process the values of a CHILD.
  // Add() is not called if @value is missing in a child row.
  void Add(absl::string_view value,
           OptionalValue<absl::string_view> comment) final {
    if (comment.present) {
      absl::StrAppendFormat(&res_, "%s (%s) ", value, comment.value);
    } else {
      absl::StrAppendFormat(&res_, "%s ", value);
    }
  }

  // Called to get a PARENT value when all CHILDREN were added.
  absl::string_view GetResult() final { return res_; }

 private:
  std::string res_;
};

DenseGroupOps<AggTextAccumulator> op(GetHeapBufferFactory());
ASSIGN_OR_RETURN(auto res, op.Apply(edge, prefixes, values, comments));
```
