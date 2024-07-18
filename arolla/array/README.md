# Array

## Overview

`Array` is an immutable array with support of missing values. Implemented on top
of `DenseArray` (see
dense_array/README.md.)
It efficiently represents very sparse data and constants, but has a bigger fixed
overhead than `DenseArray`.

Like `DenseArray` for `Array` applies the following:

*   Data is immutable.
*   Data is accessed via shared pointer, so copying is not expensive.
*   Only trivially destructible types and string types are supported.
*   There is a special implementation for string types. All chars are stored in
    a single buffer, so no separate allocation per string is required.

## IdFilter

`IdFilter` is an abstraction that is essential for `Array`. It represents a
sequence of ids (`int64_t`) in ascending order. All ids are in range `[0,
size)`. Note that `IdFilter` doesn't store the size, the size is only used in
the constructor.

A high-level way to access the sequence is the functions:

*   `IdToOffset(int64_t id) -> OptionalValue<int64_t>` -- returns a position in
    the sequence if the sequence contains given id.
*   `IdsOffsetToId(int64_t offset) -> int64_t` -- returns an id from the given
    position in the sequence.

But for performance reasons the internal representation is a bit more
complicated than just a sequence of integers:

```c++
class IdFilter {
  ...
  enum Type { kEmpty, kPartial, kFull };
  using IdWithOffset = int64_t;

  Type type_;
  // Must be in ascending order. Empty if `type_` != `kPartial`.
  Buffer<IdWithOffset> ids_;
  int64_t ids_offset_ = 0;  // Used if values in ids buffer are not zero based.
};
```

It can be one of 3 types:

*   `kEmpty` -- The sequence is empty, `ids_` is empty, `IdToOffset` always
    return `nullopt`.

*   `kPartial` -- Not empty, but some ids are missing. The sequence is stored in
    `ids_` buffer. The values in the buffer are not always zero-based, because
    it can be a slice of a bigger buffer and instead of copying we just apply an
    offset. So `IdsOffsetToId(offset)` returns `ids_[offset] - ids_offset_`.

*   `kFull` -- All ids in range `[0, size)` are present, so `id` and `offset` in
    this case are equivalent. Accessing `ids_` buffer when type==kFull is an
    error.

## Array internals

Let's look what is inside a `Array`:

```c++
template <class T>
class Array {
 public:
  OptionalValue<view_type_t<T>> operator[](int64_t index) const {
    DCHECK(0 <= index && index < size_);
    OptionalValue<int64_t> offset = id_filter_.IdToOffset(index);
    if (offset.present) {
      return dense_data_[offset.value];
    } else {
      return missing_id_value_;
    }
  }
  ...
  int64_t size() const { return size_; }
  const IdFilter& id_filter() const { return id_filter_; }
  const DenseArray<T>& dense_data() const { return dense_data_; }
  const OptionalValue<T>& missing_id_value() const { return missing_id_value_; }
 private:
  int64_t size_;
  IdFilter id_filter_;
  DenseArray<T> dense_data_;
  OptionalValue<T> missing_id_value_;
};
```

If an id present in `IdFilter`, then the corresponding value is stored in the
underlying `DenseArray` (and still can be missing if `dense_data_` has a
bitmap). If an id doesn't present in `IdFilter`, then the value is
`missing_id_value_`. Size of the `DenseArray` must be equal to the number of
present ids in `IdFilter`.

Note that:

*   The word "missing" in `missing_id_value_` doesn't mean that some value is
    missing. It is actually a value for missing ids -- ids that are not present
    in IdFilter. It is often equal to `nullopt`, but not always.
*   If `missing_id_value_` is not equal to `nullopt`, it doesn't mean that all
    values are present, because `dense_data_` can have `bitmap` as an additional
    source of sparsity.

There are many possible combinations of `id_filter_`, `dense_data_`, and
`missing_id_value_`, but any of them makes sense. We use the following
terminology:

**Const form**

`Array` is in the **const form** if its `IdFilter` is empty.

```c++
bool IsConstForm() const { return id_filter_.type() == IdFilter::kEmpty; }
```

It means that `dense_data_` is also empty and all values are equal to
`missing_id_value_`. It is a canonical representation of a constant array and an
array with all values missing.

**All missing form**

**"All missing"** is a specific case of the const form with an additional
condition `missing_id_value_`==`nullopt`. A canonical representation of an array
where all values are missing.

```c++
bool IsAllMissingForm() const {
  return IsConstForm() && !missing_id_value_.present;
}
```

**Dense form**

`Array` is in the **dense form** if its `IdFilter` is full. In this form it is
equivalent to the `DenseArray` that can be accessed with `array.dense_data()`.

```c++
bool IsDenseForm() const { return id_filter_.type() == IdFilter::kFull; }
```

**Full form**

It is a specific case of the dense form with an additional condition that
`dense_data_` has no bitmap. A canonical representation of an array with all
values present.

```c++
bool IsFullForm() const { return IsDenseForm() && dense_data_.bitmap.empty(); }
```

**Sparse form**

If `IdFilter` is `kPartial`, then the `Array` is in the **sparse form**.

```c++
bool IsSparseForm() const { return id_filter_.type() == IdFilter::kPartial; }
```

It is convenient if only a small portion of elements is present, or if the most
of the elements are equal to some default value.

For example `[5, 1, 1, 7, nullopt, 1.5, 1, 1, 1, 1, ...]` can be represented as
a sparse Array this way:

```
{size = 1'000'000,
 id_filter = {type = kPartial, ids = {0, 3, 4, 5}, ids_offset = 0},
 dense_data = {5.0, 7.0, std::nullopt, 1.5},
 missing_id_value = {true, 1.0}
}
```

The functions `Is*Form` give information about the format rather than about
actual sparsity and constancy. For example this is a valid representation of a
const array `[5.0, 5.0, 5.0]`:

```
{size = 3,
 id_filter = {type = kPartial, ids = {0, 2}, ids_offset = 0},
 dense_data = {5.0, 5.0},
 missing_id_value = {true, 5.0}
}
```

But it is in the sparse form rather than in the const form - it is not a
canonical const representation.

## Creating Array

**Constructors**

*   Default constructor `Array<T>()` creates an empty Array with size=0.
*   `Array<T>(size)` -- Array in the "all missing" form.
*   `Array<T>(size, value)` -- Array in the const form (value can be `nullopt`)
*   `Array<T>(dense_data)` creates a dense Array from a `DenseArray`. See how to
    create a DenseArray in `dense_array/README.md`.
*   `Array<T>(size, id_filter, dense_data, missing_id_value)` -- universal
    low-level constructor that creates Array from its fields. For performance
    reasons it doesn't validate arguments, so can be used only if the arguments
    are guaranteed to be correct: `size` must be equal to the size that was used
    to construct `id_filter`; size of `dense_data` must be equal to the number
    of present ids in `id_filter`.

Both `IdFilter` and `DenseArray` are based on `arolla::Buffer` that allows to
share immutable data between several objects. Array constructors don't need to
copy buffers, so it always takes `O(1)` time. However it still makes sense to
`std::move` the arguments `dense_data` and `id_filter` because moving internal
shared pointers is a bit faster than copying.

**CreateArray**

A helper functions to create an Array from span of values, or from ids and
values. Ids must be in ascending order.

These functions copy data, so they are not especially performance efficient.

```c++
template <class T>
Array<T> CreateArray(absl::Span<const OptionalValue<T>> data);

// ValueT is one of T, OptionalValue<T>, std::optional<T>,
//     view_type<T>, OptionalValue<view_type<T>>, std::optional<view_type<T>>.
template <class T, class ValueT = T>
Array<T> CreateArray(int64_t size, absl::Span<const int64_t> ids,
                     absl::Span<const ValueT> values);
```

## Form conversion

Array provides functions to convert from one form to another. Array is
immutable, so these functions return copies (that can share some internal data)
instead of modifying the original Array.

```
// Changes IdFilter to the given one. For ids from the new IdFilter,
// the resulted block will contain the same values as the current block.
// All other values will be equal to `missing_id_value`.
// An example:
// IdFilter filter(8, CreateBuffer<int64_t>({1, 4, 5, 6}));
// Array<int> block(CreateDenseArray<int>({1, 2, 3, 4, nullopt, 6, 7, 8}));
// Array<int> new_block = block.WithIds(filter, 0);
//     new_block.id_filter() == filter
//     Elements are: 0, 2, 0, 0, nullopt, 6, 7, 0
Array WithIds(const IdFilter& ids, const OptionalValue<T>& missing_id_value,
              RawBufferFactory* buf_factory = GetHeapBufferFactory()) const;

Array ToDenseForm(
    RawBufferFactory* buf_factory = GetHeapBufferFactory()) const {
  return WithIds(IdFilter::kFull, std::nullopt, buf_factory);
}

// Converts Array to sparse form with given missing_id_value. If
// missing_id_value is nullopt, then the resulted Array will have no bitmap.
// Note: if input Array is in const form, then conversion to sparse makes
// no sense. Output will be either the same (if provided missing_id_value is
// the same as before), or will be converted to full form.
Array ToSparseForm(
    OptionalValue<T> missing_id_value = std::nullopt,
    RawBufferFactory* buf_factory = GetHeapBufferFactory()) const;
```

The best way to convert an Array to a DenseArray is
`array.ToDenseForm().dense_data()`.

If an Array is in the full form (i.e. `IsFullForm()` return 'true'), then it is
safe to access the values as

```c++
DCHECK(array.IsFullForm());
const Buffer<T>& values = array.dense_data().values;
```

Form conversion is an expensive operation (unless the Array is already in the
requested form), because it requires an iteration over all values.
Complexity approximation of

```
Array<T> output = input.WithIds(...)
```

is `O(input.dense_data().size() + output.dense_data().size())`.

## Accessing elements and iterating over Array

Array (as well as DenseArray and Buffer) returns `view_type_t<T>` rather than
`T`. For all trivial types it is the same as `T`, but for all string types (i.e.
`std::string`, `arolla::Bytes`, `arolla::Text`) it is `absl::string_view` (see
`util/view_types.h`). Views are valid until the Array exists.

**Accessing elements**

```c++
auto array = CreateArray<Text>({"Hello", std::nullopt, "World"});

array[0];  // OptionalValue<absl::string_view>("Hello")
array[1];  // OptionalValue<absl::string_view>(std::nullopt)
```

Complexity of `array[i]` is:

- `O(log array.dense_data().size())` if in sparse form;
- `O(1)` if in const or full form.

Note that even in the full form it has relatively big constant since
it supports all cases are has several conditional jumps. So

```c++
DCHECK(array.IsFullForm());
const Buffer<T>& values = array.dense_data().values;
auto v = values[i];
```

is several times faster than

```c++
DCHECK(array.IsFullForm());
auto v = array[i].value;
```

though both examples are `O(1)`.

**Iterating**

The correct way of iterating over Array is `Array::ForEachPresent`:

```c++
array.ForEachPresent([&](int64_t id, view_type_t<T> value) {
  std::cout << id << " -> " << value << std::endl;
});
```

Complexity is `O(array.PresentCount())`.

It is also possible to iterate over Array with a simple loop, but it is
**extremely inefficient** and should not be used outside of tests.

```c++
for (OptionalValue<view_type_t<T>> v : array) {  // extremely inefficient
  if (v.present) std::cout << v.value << std::endl;
}
```

Complexity is `O( array.size() * log array.PresentCount() )`.

In the full form case it is `O( array.size() )`, but the constant is still much
bigger than in `ForEachPresent`.

## Pointwise operations on Array

Main header is `array/pointwise_op.h`.

Pointwise operations on Array are based on DenseArray operations and have almost
the same interface. The only difference is that every Array operation returns
`StatusOr`.

The main idea behind a pointwise Array operation is the following:

1.  Create `IdFilter` as a combination of id filters of arguments (it is a
    simplificated explanation, see the code in `pointwise_op.h` for details).
2.  Convert all the arguments to use the new `IdFilter`.
3.  Apply the corresponding DenseArray operation to `dense_data` of the
    arguments.
4.  Apply pointwise operation to `missing_id_value` of the arguments.
5.  Construct resulted Array from the new `IdFilter`, the new `dense_data`, and
    the new `missing_id_value`.

If all the arguments already have common `IdFilter` (for example if all are in
the dense form), then the performance is almost the same as for the `DenseArray`
operation. Otherwise applying `Array::WithIds` adds an overhead.

The time estimation varies from `O(min( q.dense_data().size() for q in args ))`
if there is no optional arguments and no missing_id_value to `O(sum(
q.dense_data().size() for q in args ))` in the worst case. Practically the
sparse representation makes sense if the number of present elements (or
not-equal-to-default elements if `missing_id_value` is used) is less than 10-20%
of the total size of the Array.

A pointwise operation can be defined by a functor or by a lambda function. All
input arrays must be the same size.

**Simple case**

Lambda function:

```c++
auto arr1 = CreateArray<int>({1, {}, 2, 3});
auto arr2 = CreateArray<int>({5, 2, {}, 1});
auto op = CreateArrayOp([](int a, int b) { return a + b; });
ASSIGN_OR_RETURN(Array<int> res, op(arr1, arr2));
// res: {6, std::nullopt, std::nullopt, 4}
```

Functor:

```c++
template <class T>
struct Sum3Fn {
  // The function itself shouldn't be templated because its prototype is used
  // for types deduction.
  T operator()(T a, T b, T c) { return a + b + c; }
};

auto op = CreateArrayOp(Sum3Fn<int>());
ASSIGN_OR_RETURN(Array<int> res, op(arr1, arr2, arr3));
```

**Optional arguments**

If some arguments of a functor are `OptionalValue`, then the operation will be
evaluated even for rows where the corresponding values are missing.

For example here `a` is optional and `b` is required:

```c++
auto arr1 = CreateArray<int>({1, {}, 2, 3});
auto arr2 = CreateArray<int>({5, 2, {}, 1});
auto fn = [](OptionalValue<int> a, int b) {
  return a.present ? a.value : b;
};
auto op = CreateArrayOp(fn);
ASSIGN_OR_RETURN(Array<int> res, op(arr1, arr2));
// res: {1, 2, std::nullopt, 3}
```

**Missing result**

Result can be missing even if all arguments are present:

```c++
auto fn = [](int a, int b) -> OptionalValue<int> {
  if (b == 0) return std::nullopt;
  return a / b;
};
auto div_op = CreateArrayOp(fn);
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
auto op = CreateArrayOp(fn);

Array<Text> array1 = ...;
Array<Text> array2 = ...;
ASSIGN_OR_RETURN(Array<Text> res, op(array1, array2));
```

**From operation on DenseArrays**

In rare cases if we want to do something non-trivial in the inner loop, it can
be useful to create an Array operation not from a pointwise functor, but
directly from a DenseArray operation. It can be done this way:

```c++
// Create custom DenseArray operation.
auto span_fn = [](absl::Span<float> res,
                  absl::Span<const float> arg1,
                  absl::Span<const int> arg2) {
  #pragma omp parallel for
  for (size_t i = 0; i < res.size(); ++i) {
    res[i] = arg1[i] + arg2[i];
  }
};
auto dense_op = CreateDenseBinaryOpFromSpanOp<float>(span_fn);

// Operation on optionals -- needed to process missing_id_value.
auto optional_fn = WrapFnToAcceptOptionalArgs([](float arg1, int arg2) {
  return arg1 + arg2;
});

// Create operation on Arrays from dense_op and optional_fn.
using Op = ArrayPointwiseOp<float, decltype(dense_op), decltype(optional_fn),
                            meta::type_list<float, int>>;
auto array_op = Op(dense_op, optional_fn, GetHeapBufferFactory());

ASSIGN_OR_RETURN(Array<float> res, op(array1, array2));
```

### Group operations

A Group Operation is an operation accepting as input groups of values from zero
or more data columns in a Child ID Space and individual values from zero or more
data column in a Parent ID Space. A Group Operation's output is a single data
column in either the Parent ID Space or the Child ID Space.

See detailed description in `qexpr/aggregation_ops_interface.h`.

Group operations on Arrays have completely the same interface as operations on
DenseArrays. Main class `ArrayGroupOp` is defined in `array/group_op.h`.

To define an operation create a class derived from `Accumulator` and use it as a
template argument in `ArrayGroupOp`. Note that for good performance in sparse
and const cases `Accumulator::AddN` should be implemented.

An example:

```c++
class AggSumAccumulator final
    : public Accumulator<AccumulatorType::kAggregator, float,
                         meta::type_list<>, meta::type_list<float>> {
 public:
  void Reset() final { res_ = 0; }

  void Add(float v) final { res_ += v; }
  void AddN(int64_t n, float v) final { res_ += n * static_cast<double>(v); }

  float GetResult() final { return static_cast<float>(res_); }

 private:
  double res_;
};

ArrayGroupOp<AggSumAccumulator> op(GetHeapBufferFactory());
ASSIGN_OR_RETURN(auto res, op.Apply(edge, values));
```
