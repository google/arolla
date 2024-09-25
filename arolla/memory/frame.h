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
#ifndef AROLLA_MEMORY_FRAME_H_
#define AROLLA_MEMORY_FRAME_H_

#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <initializer_list>
#include <ostream>
#include <tuple>
#include <type_traits>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/util/algorithms.h"
#include "arolla/util/demangle.h"
#include "arolla/util/is_bzero_constructible.h"
#include "arolla/util/memory.h"
#include "arolla/util/struct_field.h"

namespace arolla {

// FrameLayout contain a structure definition which can be constructed
// dynamically. FrameLayout instances are created using FrameLayout::Builder.
// For example:
//
//    FrameLayout::Builder bldr;
//    auto dbl_slot = bldr.AddSlot<double>();
//    auto int_vec_slot = bldr.AddSlot<std::vector<int>>();
//    FrameLayout desc = std::move(bldr).Build();
//
// Once created, a FrameLayout can be used to initialize a suitably aligned
// block of memory, and the individual fields can be accessed using the slot
// objects.
class FrameLayout {
 public:
  //
  // Forward declarations for nested classes defined below.
  //
  class Builder;

  template <typename T>
  class Slot;

  FrameLayout() = default;

  // Returns the number of bytes required by this frame layout.
  size_t AllocSize() const { return alloc_size_; }

  // Returns the alignment required by this frame layout.
  Alignment AllocAlignment() const { return alloc_alignment_; }

  // Initialize a block of memory with the fields defined within this
  // frame layout. The provided alloc must be suitably aligned.
  // Initialize clears the memory to all zeros, and calls the constructor
  // for all non-trivial types.
  void InitializeAlignedAlloc(void* alloc) const;

  // Calls destructors on all of the non-trivial objects within this frame
  // layout on the provided block of memory. Assumes InitializeAlloc() was
  // previously called on the alloc.
  void DestroyAlloc(void* alloc) const;

  // Initialize a sequence of memory blocks using the memory layout.
  // The provided alloc must be suitably aligned.
  // Initialize clears the memory to all zeros, and calls the constructor
  // for all non-trivial types.
  void InitializeAlignedAllocN(void* alloc, size_t n) const;

  // Calls destructors on all of objects in a sequence. Assumes
  // InitializeAllocN() was previously called on the alloc.
  void DestroyAllocN(void* alloc, size_t n) const;

  // Returns true iff the field registered by the given offset and type.
  // This can be used to perform runtime type checking.
  bool HasField(size_t offset, const std::type_info& type) const;

 private:
  // Called by FrameLayout::Builder::Build().
  explicit FrameLayout(Builder&& builder);

  // Field initializer and destroyer.
  class FieldFactory;

  // Initializers for fields of non-trivial type, ie. fields which require
  // construction or destruction beyond zeroing memory.
  struct FieldInitializers {
    // Adds offset to the given type. If factory is not present, empty is added.
    // Extracted to cc file to avoid binary and linkage size bloat.
    void AddOffsetToFactory(size_t offset, FieldFactory empty_factory);

    // Adds all initialization from another FieldInitializers with extra offset.
    void AddDerived(size_t extra_offset,
                    const FieldInitializers& derived_initializers);

    // Adds field initialization for given type.
    template <class T>
    void Add(size_t offset);

    // InitializeAlloc and DestroyAlloc iterating over all factories, so
    // they need to be stored in a simple data structure with fast iteration.
    std::vector<FieldFactory> factories;
    absl::flat_hash_map<std::type_index, size_t> type2factory;
  };

#ifndef NDEBUG
  // When the NDEBUG macro is not defined, we enable runtime checks for field
  // access. Unfortunately, `absl::flat_hash_set` works significant slower
  // without NDEBUG. For this reason, we choose a different container (tested
  // using: `%timeit arolla.testing.detect_qtype_signatures(M.math.is_close)`):
  //
  //   * fast-build:
  //     - absl::btree_set      8.3s
  //     - std::unordered_map   9.5s
  //     - absl::flat_hash_set  16.1s
  //
  //   * opt-build (with `--copt=-UNDEBUG`):
  //     - absl::btree_set      5.8s
  //     - std::unordered_map   7.2s
  //     - absl::flat_hash_set  11.3s
  //
  using RegisteredField = std::pair<size_t, std::type_index>;
  using RegisteredFields = absl::btree_set<RegisteredField>;

  // Set of pair<offset, type_index> for all registered fields
  // from AddSlot or RegisterUnsafeSlot.
  RegisteredFields registered_fields_;
#endif

  FieldInitializers initializers_;

  // Total required allocation size in bytes. This may be larger than the sum
  // of the individual field sizes, due to padding required for alignment.
  size_t alloc_size_{0};

  // Alignment requirement for the allocation. Cannot be less than alignments of
  // the individual fields.
  Alignment alloc_alignment_{1};
};

template <typename T>
struct TypesToSlotTypes;

template <typename... Ts>
struct TypesToSlotTypes<std::tuple<Ts...>> {
  using type = std::tuple<FrameLayout::Slot<Ts>...>;
};

// A lightweight typed handle to a field within a FrameLayout.
// Note that a FrameLayout::Builder generates a single FrameLayout
// and a collection of Slots. The Slots are used to index only into
// allocations initialized by the corresponding FrameLayout. Accessing
// an incompatible or uninitialized allocation is unsafe.
template <typename T>
class FrameLayout::Slot {
  static constexpr size_t kNumSubslots = StructFieldCount<T>();
  static_assert(std::is_standard_layout<T>::value || (kNumSubslots == 0),
                "Only standard layout classes support access to subfields.");

 public:
  using value_type = T;

  Slot(const Slot& other) = default;

  // The offset, in bytes, of this slot within an allocation.
  size_t byte_offset() const { return byte_offset_; }

  static Slot<T> UnsafeSlotFromOffset(size_t byte_offset) {
    return Slot(byte_offset);
  }

  static constexpr size_t kUninitializedOffset = ~static_cast<size_t>(0);

  // Creates an uninitialized slot that can not be used to access a value and
  // can not be registered in frame layout.
  static Slot<T> UnsafeUninitializedSlot() {
    return Slot(kUninitializedOffset);
  }

  friend std::ostream& operator<<(std::ostream& stream,
                                  const FrameLayout::Slot<T>& slot) {
    return stream << "Slot<" << TypeName<T>() << ">(" << slot.byte_offset()
                  << ")";
  }

  // Returns the number of subfields for slots of this type.
  static constexpr size_t NumSubslots() { return kNumSubslots; }

  // Returns a Slot<> corresponding to this slot's I'th subfield as defined
  // in FrameLayoutFieldTraits<T>.
  template <size_t I>
  auto GetSubslot() {
    static_assert(I < kNumSubslots, "Subslot Index out of range.");
    const auto& struct_field = std::get<I>(GetStructFields<T>());
    return FrameLayout::Slot<
        typename std::decay_t<decltype(struct_field)>::field_type>::
        UnsafeSlotFromOffset(byte_offset_ + struct_field.field_offset);
  }

 private:
  friend class Builder;
  friend class FramePtr;
  friend class ConstFramePtr;

  // Constructor is private to ensure new slots are only created by the
  // Builder class.
  explicit Slot(size_t byte_offset) : byte_offset_(byte_offset) {}

  // Returns a constant reference to this slot's field within an allocation.
  decltype(auto) UnsafeGet(const void* alloc) const {
    DCHECK_NE(byte_offset_, kUninitializedOffset);
    return *reinterpret_cast<const T*>(static_cast<const char*>(alloc) +
                                       byte_offset_);
  }

  // Returns a mutable pointer to this slot's field within an allocation.
  T* UnsafeGetMutable(void* alloc) const {
    DCHECK_NE(byte_offset_, kUninitializedOffset);
    return reinterpret_cast<T*>(static_cast<char*>(alloc) + byte_offset_);
  }

  size_t byte_offset_;
};

// A factory class that is responsible for initialization and destruction of
// the fields within a previously allocated block of memory.
class FrameLayout::FieldFactory {
 public:
  // Returns a type-specific field-factory object.
  template <typename T>
  static FieldFactory Create() {
    static_assert(!is_bzero_constructible<T>() ||
                  !std::is_trivially_destructible<T>());
    // BZero constructible types don't need extra initialization because
    // FrameLayout::InitializeAlloc() always zeroes the memory.
    //
    // Examples: OptionalValue<int32>, OptionalValue<float>.
    FactoryFn construct;
    FactoryNFn construct_n;
    if constexpr (is_bzero_constructible<T>()) {
      construct = [](void*, absl::Span<const size_t>) {};
      construct_n = [](void*, absl::Span<const size_t>, size_t, size_t) {};
    } else {
      construct = [](void* ptr, absl::Span<const size_t> offsets) {
        for (size_t offset : offsets) {
          void* shifted_ptr = static_cast<char*>(ptr) + offset;
          new (shifted_ptr) T;
        }
      };
      construct_n = [](void* ptr, absl::Span<const size_t> offsets,
                       size_t block_size, size_t n) {
        for (size_t i = 0; i < n; ++i) {
          for (size_t offset : offsets) {
            void* shifted_ptr =
                static_cast<char*>(ptr) + offset + i * block_size;
            new (shifted_ptr) T;
          }
        }
      };
    }
    // Trivially destructible types types don't need extra de-initialization.
    FactoryFn destruct;
    FactoryNFn destruct_n;
    if constexpr (std::is_trivially_destructible<T>()) {
      destruct = [](void*, absl::Span<const size_t>) {};
      destruct_n = [](void*, absl::Span<const size_t>, size_t, size_t) {};
    } else {
      destruct = [](void* ptr, absl::Span<const size_t> offsets) {
        for (size_t offset : offsets) {
          void* shifted_ptr = static_cast<char*>(ptr) + offset;
          static_cast<T*>(shifted_ptr)->~T();
        }
      };
      destruct_n = [](void* ptr, absl::Span<const size_t> offsets,
                      size_t block_size, size_t n) {
        for (size_t i = 0; i < n; ++i) {
          for (size_t offset : offsets) {
            void* shifted_ptr =
                static_cast<char*>(ptr) + offset + i * block_size;
            static_cast<T*>(shifted_ptr)->~T();
          }
        }
      };
    }
    return FieldFactory(std::type_index(typeid(T)), construct, destruct,
                        construct_n, destruct_n);
  }

  // Returns type associated with the FieldFactory.
  std::type_index type_index() const;

  // Adds an offset to construct T used in Create<T>.
  void Add(size_t offset);

  // Adds all fields from another FieldFactory.
  void AddDerived(const FieldFactory& derived_factory);

  // Returns a copy of the factory with adjusted offset.
  FieldFactory Derive(size_t offset) const;

  // Initializes fields within the provided block of storage.
  void Construct(void* ptr) const { construct_(ptr, offsets_); }

  // Destroys fields within the provided block of storage.
  void Destroy(void* ptr) const { destruct_(ptr, offsets_); }

  // Initializes fields within the provided storage block sequence.
  void ConstructN(void* ptr, size_t block_size, size_t n) const {
    construct_n_(ptr, offsets_, block_size, n);
  }

  // Destroys fields within the provided storage block sequence.
  void DestroyN(void* ptr, size_t block_size, size_t n) const {
    destruct_n_(ptr, offsets_, block_size, n);
  }

 private:
  using FactoryFn = void (*)(void*, absl::Span<const size_t>);
  using FactoryNFn = void (*)(void*, absl::Span<const size_t>, size_t, size_t);

  FieldFactory(std::type_index tpe, FactoryFn construct, FactoryFn destruct,
               FactoryNFn construct_n, FactoryNFn destruct_n)
      : type_(tpe),
        construct_(construct),
        destruct_(destruct),
        construct_n_(construct_n),
        destruct_n_(destruct_n) {}

  std::type_index type_;
  FactoryFn construct_;
  FactoryFn destruct_;
  std::vector<size_t> offsets_;
  FactoryNFn construct_n_;
  FactoryNFn destruct_n_;
};

template <class T>
void FrameLayout::FieldInitializers::Add(size_t offset) {
  AddOffsetToFactory(offset, FieldFactory::Create<T>());
}

inline void FrameLayout::InitializeAlignedAlloc(void* alloc) const {
  DCHECK(IsAlignedPtr(alloc_alignment_, alloc)) << "invalid alloc alignment";
  memset(alloc, 0, alloc_size_);
  // Call constructor on each non-trivial field.
  for (const auto& factory : initializers_.factories) {
    factory.Construct(alloc);
  }
}

inline void FrameLayout::DestroyAlloc(void* alloc) const {
  // Call destructor on each non-trivial field.
  for (const auto& factory : initializers_.factories) {
    factory.Destroy(alloc);
  }
}

inline void FrameLayout::InitializeAlignedAllocN(void* alloc, size_t n) const {
  DCHECK(IsAlignedPtr(alloc_alignment_, alloc)) << "invalid alloc alignment";
  memset(alloc, 0, alloc_size_ * n);
  for (const auto& factory : initializers_.factories) {
    factory.ConstructN(alloc, alloc_size_, n);
  }
}

inline void FrameLayout::DestroyAllocN(void* alloc, size_t n) const {
  for (const auto& factory : initializers_.factories) {
    factory.DestroyN(alloc, alloc_size_, n);
  }
}

// Builder for creating FrameLayout and associated Slots.
class FrameLayout::Builder {
 public:
  // Allocates storage in the layout for holding a type T parameter.
  // Consecutive calls of AddSlot<T> with the same T guaranteed to form a layout
  // that is compatible with std::array<T>.
  template <typename T>
  ABSL_ATTRIBUTE_ALWAYS_INLINE Slot<T> AddSlot() {
    // TODO: Consider supporting strongly aligned types.
    static_assert(alignof(T) <= 16,
                  "Types with strong alignments are not supported.");
    alloc_size_ = RoundUp(alloc_size_, alignof(T));
    size_t offset = alloc_size_;
    Slot<T> slot(offset);
    alloc_size_ += sizeof(T);
    alloc_alignment_ = std::max(alloc_alignment_, alignof(T));
    if constexpr (!is_bzero_constructible<T>() ||
                  !std::is_trivially_destructible<T>()) {
      initializers_.Add<T>(offset);
    }
    auto status = RegisterSlot(slot.byte_offset(), sizeof(T), typeid(T));
    DCHECK(status.ok()) << status.message()
                        << "Internal error during RegisterSlot.";
    status =
        RegisterSubslots(slot, std::make_index_sequence<slot.NumSubslots()>());
    DCHECK(status.ok()) << status.message()
                        << "Internal error during RegisterSubslots.";
    return slot;
  }

  // Allocates storage in the layout for holding a sub-frame.
  Slot<void> AddSubFrame(const FrameLayout& subframe);

  // Register additional slot to pass runtime type checks.
  // Non-trivial fields registered this way are expected to be initialized and
  // destroyed by their containing object.
  // Caller is responsible for correctness of the provided slot.
  // Examples:
  //   FrameLayout::Builder builder;
  //   auto int_slot = builder.AddSlot<int>();
  //   builder.RegisterUnsafeSlot(
  //       int_slot.byte_offset(), sizeof(int), typeid(char));
  absl::Status RegisterUnsafeSlot(size_t byte_offset, size_t byte_size,
                                  const std::type_info& type);

  // Static version of RegisterUnsafeSlot.
  // Caller need to use UnsafeSlotFromOffset and is responsible for correctness.
  // Examples:
  //   FrameLayout::Builder builder;
  //   auto int_slot = builder.AddSlot<int>();
  //   auto first_byte_slot = FrameLayout::Slot<char>::UnsafeSlotFromOffset(
  //       int_slot.byte_offset());
  //   builder.RegisterUnsafeSlot(first_byte_slot);
  template <typename T>
  absl::Status RegisterUnsafeSlot(const Slot<T>& slot,
                                  bool allow_duplicates = false) {
    return RegisterSlot(slot.byte_offset(), sizeof(T), typeid(T),
                        allow_duplicates);
  }

  // Creates a layout. Since this destroys the Builder, it must be called
  // using std::move(), for example:
  //
  // auto layout = std::move(builder).Build();
  FrameLayout Build() && {
    alloc_size_ = RoundUp(alloc_size_, alloc_alignment_);
    return FrameLayout(std::move(*this));
  }

 private:
  friend class FrameLayout;

  template <typename T, size_t... Is>
  absl::Status RegisterSubslots(
      Slot<T> slot ABSL_ATTRIBUTE_UNUSED,  // unused iff sizeof...(Is)==0.
      std::index_sequence<Is...>) {
    ABSL_ATTRIBUTE_UNUSED auto register_slot_recursively =
        [&](auto subslot) -> absl::Status {
      absl::Status status = RegisterUnsafeSlot(subslot);
      if constexpr (decltype(subslot)::NumSubslots() != 0) {
        if (status.ok()) {
          status = RegisterSubslots(
              subslot,
              std::make_index_sequence<decltype(subslot)::NumSubslots()>());
        }
      }
      return status;
    };
    for (absl::Status status : std::initializer_list<absl::Status>{
             register_slot_recursively(slot.template GetSubslot<Is>())...}) {
      if (!status.ok()) return status;
    }
    return absl::OkStatus();
  }

  absl::Status RegisterSlot(size_t byte_offset, size_t byte_size,
                            const std::type_info& type,
                            bool allow_duplicates = false);

#ifndef NDEBUG
  RegisteredFields registered_fields_;
#endif
  FieldInitializers initializers_;
  size_t alloc_size_{0};
  size_t alloc_alignment_{1};
};

// Creates a frame layout for type `T`.
template <typename T>
FrameLayout MakeTypeLayout() {
  FrameLayout::Builder builder;
  auto slot = builder.AddSlot<T>();
  // We expect a slot right in front of the frame.
  DCHECK_EQ(slot.byte_offset(), size_t{0});
  return std::move(builder).Build();
}

// Pointer to an instance of FrameLayout. Doesn't own data.
class FramePtr {
 public:
  FramePtr(void* base_ptr, const FrameLayout* layout);

  // Gets a mutable pointer to value in given slot.
  template <typename T>
  T* GetMutable(FrameLayout::Slot<T> slot) const {
    DCheckFieldType(slot.byte_offset(), typeid(T));
    return slot.UnsafeGetMutable(base_ptr_);
  }

  // Sets value in given slot.
  template <typename T, typename S = T>
  void Set(FrameLayout::Slot<T> slot, S&& value) const {
    DCheckFieldType(slot.byte_offset(), typeid(T));
    *GetMutable(slot) = std::forward<S>(value);
  }

  // Gets value from given slot.
  template <typename T>
  const T& Get(FrameLayout::Slot<T> slot) const {
    DCheckFieldType(slot.byte_offset(), typeid(T));
    return slot.UnsafeGet(base_ptr_);
  }

  // Returns a raw pointer to element at given offset.
  void* GetRawPointer(size_t byte_offset) const {
    return static_cast<char*>(base_ptr_) + byte_offset;
  }

  // Checks type of slot matches frame layout. Only enabled
  // for debug builds for performance.
  void DCheckFieldType(size_t offset, const std::type_info& type) const;

 private:
  friend class ConstFramePtr;

  void* base_ptr_;

#ifndef NDEBUG
  // Keep FrameLayout here for debug builds only to make this object
  // as lightweight as a void* in opt builds.
  const FrameLayout* layout_;
#endif
};

// Pointer to a constant instance of FrameLayout. Doesn't own data.
class ConstFramePtr {
 public:
  ConstFramePtr(const void* base_ptr, const FrameLayout* layout);
  /* implicit */ ConstFramePtr(FramePtr frame_ptr);

  // Gets value from given slot.
  template <typename T>
  const T& Get(FrameLayout::Slot<T> slot) const {
    DCheckFieldType(slot.byte_offset(), typeid(T));
    return slot.UnsafeGet(base_ptr_);
  }

  // Returns a const raw pointer to element at given offset.
  const void* GetRawPointer(size_t byte_offset) const {
    return static_cast<const char*>(base_ptr_) + byte_offset;
  }

  // Checks type of slot matches frame layout. Only enabled
  // for debug builds for performance.
  void DCheckFieldType(size_t offset, const std::type_info& type) const;

 private:
  const void* base_ptr_;

#ifndef NDEBUG
  // Keep FrameLayout here for debug builds only to make this object
  // as lightweight as a void* in opt builds.
  const FrameLayout* layout_;
#endif
};

#ifndef NDEBUG

inline bool FrameLayout::HasField(size_t offset,
                                  const std::type_info& type) const {
  return registered_fields_.contains({offset, std::type_index(type)});
}

inline FrameLayout::FrameLayout(Builder&& builder)
    : registered_fields_(std::move(builder.registered_fields_)),
      initializers_(std::move(builder.initializers_)),
      alloc_size_(builder.alloc_size_),
      alloc_alignment_{builder.alloc_alignment_} {}

inline FramePtr::FramePtr(void* base_ptr, const FrameLayout* layout)
    : base_ptr_(base_ptr), layout_(layout) {}

inline void FramePtr::DCheckFieldType(size_t offset,
                                      const std::type_info& type) const {
  DCHECK(layout_->HasField(offset, type))
      << "Field with given offset and type not found: Slot<" << type.name()
      << ">(" << offset << ")";
}

inline ConstFramePtr::ConstFramePtr(const void* base_ptr,
                                    const FrameLayout* layout)
    : base_ptr_(base_ptr), layout_(layout) {}

inline ConstFramePtr::ConstFramePtr(FramePtr frame_ptr)
    : base_ptr_(frame_ptr.base_ptr_), layout_(frame_ptr.layout_) {}

inline void ConstFramePtr::DCheckFieldType(size_t offset,
                                           const std::type_info& type) const {
  DCHECK(layout_->HasField(offset, type))
      << "Field with given offset and type not found: Slot<" << type.name()
      << ">(" << offset << ")";
}

#else   // NDEBUG

inline bool FrameLayout::HasField(size_t, const std::type_info&) const {
  return true;
}

inline FrameLayout::FrameLayout(Builder&& builder)
    : initializers_(std::move(builder.initializers_)),
      alloc_size_(builder.alloc_size_),
      alloc_alignment_{builder.alloc_alignment_} {}

inline FramePtr::FramePtr(void* base_ptr, const FrameLayout* /*layout*/)
    : base_ptr_(base_ptr) {}

inline void FramePtr::DCheckFieldType(size_t /*offset*/,
                                      const std::type_info& /*type*/) const {}

inline ConstFramePtr::ConstFramePtr(const void* base_ptr,
                                    const FrameLayout* /*layout*/)
    : base_ptr_(base_ptr) {}

inline ConstFramePtr::ConstFramePtr(FramePtr frame_ptr)
    : base_ptr_(frame_ptr.base_ptr_) {}

inline void ConstFramePtr::DCheckFieldType(
    size_t /*offset*/, const std::type_info& /*type*/) const {}
#endif  // NDEBUG

}  // namespace arolla

#endif  // AROLLA_MEMORY_FRAME_H_
