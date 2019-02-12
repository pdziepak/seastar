/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2019 ScyllaDB Ltd.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <type_traits>

#include <seastar/core/align.hh>

namespace seastar {

/// Temporary memory allocator
///
/// Monotonic buffer memory allocator optimised for short-lived objects.
/// Small objects (<= max_object_size 32kB) are allocated by incrementing the
/// pointer inside the current block (128kB). Once the current block is exhausted
/// a new one gets allocated. Each block counts the number of live objects inside it
/// the behaviour of that counter depends on whether the block is open (the one
/// currently used by the allocator) or closed (no new objects are allocated)
///
/// - If a block is open it may still receive new objects. Its counter is not positive
///   and, effectively counts deallocations. The number of allocations is kept in another
///   variable as a part of the allocator state. Once the block get closed the allocations
///   are added to the block counter. If the final value is zero it means that all objects
///   were already deallocated and the block can be freed as well.
/// - If a block is closed it means then the live object count is always positive. Once
///   it reaches zero the block is freed.
/// All blocks are aligned to their size (128kB) which makes it easy for the deallocation
/// function to find the block header.
///
/// Large objects (> 32kB) are store in individual allocations but are prepended with the
/// same block header and are aligned to the block alignment. The object count is set to one,
/// which means that the same deallocation function works for them as for any other object.
/// The allocator is however not optimised for their use and they should be avoided.
class temporary_memory_allocator {
    static constexpr size_t alignment = __STDCPP_DEFAULT_NEW_ALIGNMENT__;
    static constexpr size_t block_size = 128 * 1024;
    static constexpr size_t max_object_size = 32 * 1024;
private:
    struct alignas(alignment) block_header {
        int32_t _use_count = 0;
    };
    static_assert(max_object_size <= block_size - sizeof(block_header));
    static_assert(block_size <= std::numeric_limits<int32_t>::max());
    static_assert(std::is_trivially_destructible_v<block_header>);
private:
    block_header* _current = nullptr;
    std::byte* _position_in_current = nullptr;
    std::byte* _current_end = nullptr;
    int32_t _current_use_count = 0;
private:
    void close_current() noexcept;

    [[gnu::noinline]] [[gnu::cold]]
    void* allocate_new_block(size_t size);

    [[gnu::noinline]] [[gnu::cold]]
    void* allocate_large_object(size_t size);
public:
    temporary_memory_allocator() = default;

    temporary_memory_allocator(const temporary_memory_allocator&) = delete;
    temporary_memory_allocator(temporary_memory_allocator&& other) = delete;
    void operator=(const temporary_memory_allocator&) = delete;
    void operator=(temporary_memory_allocator&& other) = delete;

    // FIXME
    // Making the allocator non-trivial will cause the compiler to emit much
    // more code each time it is accessed when a thread-local variable.
    // The solution can be as follows:
    // - have non-trivial temporary_memory_allocator which is never used for
    //   global instances, but, for example, in per-client request scenarios
    // - have another private type implementing the same behaviour that is
    //   trivial, since it is going to be used in a single place known to us,
    //   seastar initialisation code can make sure it is properly destroyed.
    //~temporary_memory_allocator() { close_current(); }

    void* alloc(size_t size) {
        if (__builtin_expect(size > max_object_size, false)) {
            return allocate_large_object(size);
        }
        auto pos = _position_in_current;
        auto end_pos = pos + size;
        if (__builtin_expect(end_pos > _current_end, false)) {
            return allocate_new_block(size);
        }
        _position_in_current = align_up(end_pos, alignment);
        _current_use_count++;
        return pos;
    }

    void free(void* ptr) noexcept {
        auto p = static_cast<std::byte*>(ptr);
        auto hdr = reinterpret_cast<block_header*>(align_down(p, block_size));
        if (__builtin_expect(!--hdr->_use_count, false)) {
            ::free(hdr);
        }
    }

    void free(void* ptr, size_t) noexcept {
        free(ptr);
    }
};

inline thread_local temporary_memory_allocator global_temporary_memory_allocator;

struct use_temporary_allocator {
    // NOTE: I've seen various changes in the performance depending on the exactu
    // use of [[gnu::always_inline]]. However, we really shouldn't get overboard
    // with that attribute and it'd be much better to leave that to the PGO.

    static void* operator new(size_t, void* ptr) {
        return ptr;
    }
    static void* operator new(size_t n) {
        return global_temporary_memory_allocator.alloc(n);
    }
    static void operator delete(void* ptr) {
        return global_temporary_memory_allocator.free(ptr);
    }
    static void operator delete(void* ptr, size_t n) {
        return global_temporary_memory_allocator.free(ptr, n);
    }
};

}
