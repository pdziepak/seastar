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

#include <seastar/core/temporary_memory_allocator.hh>

#include <new>

namespace seastar {

void temporary_memory_allocator::close_current() noexcept {
    if (_current) {
        _current->_use_count += _current_use_count;
        if (!_current->_use_count) {
            ::free(_current);
        }
        _current = nullptr;
        _position_in_current = 0;
        _current_use_count = 0;
    }
}

[[gnu::noinline]] [[gnu::cold]]
void* temporary_memory_allocator::allocate_new_block(size_t size) {
    auto block = static_cast<std::byte*>(::aligned_alloc(block_size, block_size));
    if (!block) {
        throw std::bad_alloc();
    }
    close_current();
    auto header = new (block) block_header;
    _current = header;
    _position_in_current = block + sizeof(block_header) + align_up(size, alignment);
    _current_end = block + block_size;
    _current_use_count = 1;
    return block + sizeof(block_header);
}

[[gnu::noinline]] [[gnu::cold]]
void* temporary_memory_allocator::allocate_large_object(size_t size) {
    auto actual_size = sizeof(block_header) + size;
    void* ptr = nullptr;
    auto ret = ::posix_memalign(&ptr, block_size, actual_size);
    if (ret) {
        throw std::bad_alloc();
    }
    auto block = static_cast<std::byte*>(ptr);
    auto header = new (block) block_header;
    header->_use_count = 1;
    return block + sizeof(block_header);
}

}
