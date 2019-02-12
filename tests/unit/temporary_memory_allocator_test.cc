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

#define BOOST_TEST_MODULE temporary_memory_allocator

#include <boost/test/included/unit_test.hpp>

#include <algorithm>
#include <random>
#include <vector>

#include <seastar/core/temporary_memory_allocator.hh>

BOOST_AUTO_TEST_CASE(test_small_objects) {
    auto tmp = seastar::temporary_memory_allocator{};

    auto objs = std::vector<void*>{};
    for (auto i = 0u; i < 1024 * 1024; ++i) {
        objs.emplace_back(tmp.alloc(16));
    }
    auto eng = std::default_random_engine{std::random_device{}()};
    std::shuffle(objs.begin(), objs.end(), eng);
    for (auto ptr : objs) {
        tmp.free(ptr);
    }
}

BOOST_AUTO_TEST_CASE(test_large_objects) {
    auto tmp = seastar::temporary_memory_allocator{};

    auto objs = std::vector<void*>{};
    for (auto i = 0u; i < 8; ++i) {
        objs.emplace_back(tmp.alloc(512 * 1024));
    }
    auto eng = std::default_random_engine{std::random_device{}()};
    std::shuffle(objs.begin(), objs.end(), eng);
    for (auto ptr : objs) {
        tmp.free(ptr);
    }
}

// TODO: more tests
