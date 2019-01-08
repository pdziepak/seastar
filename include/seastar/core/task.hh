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
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include <memory>
#include <seastar/core/scheduling.hh>

namespace seastar {

class task {
    scheduling_group _sg;
public:
    struct deleter {
        void operator()(task* t) const noexcept {
            t->dispose();
        }
    };
protected:
    virtual ~task() noexcept {}
public:
    explicit task(scheduling_group sg = current_scheduling_group()) : _sg(sg) {}

    virtual void run_and_dispose() noexcept = 0;
    virtual void dispose() noexcept { delete this; }
    scheduling_group group() const { return _sg; }
};

using task_ptr = std::unique_ptr<task, task::deleter>;

void schedule(task_ptr t);
void schedule_urgent(task_ptr t);

template <typename Func>
class lambda_task final : public task {
    Func _func;
public:
    lambda_task(scheduling_group sg, const Func& func) : task(sg), _func(func) {}
    lambda_task(scheduling_group sg, Func&& func) : task(sg), _func(std::move(func)) {}
    virtual void run_and_dispose() noexcept override {
        _func();
        delete this;
    }
};

template<typename T, typename... Args>
std::unique_ptr<T, task::deleter> make_task_ptr(Args&&... args) {
    static_assert(std::is_base_of<task, T>::value, "");
    return std::unique_ptr<T, task::deleter>(new T(std::forward<Args>(args)...));
}

template <typename Func>
inline
task_ptr
make_task(Func&& func) {
    return make_task_ptr<lambda_task<Func>>(current_scheduling_group(), std::forward<Func>(func));
}

template <typename Func>
inline
task_ptr
make_task(scheduling_group sg, Func&& func) {
    return make_task_ptr<lambda_task<Func>>(sg, std::forward<Func>(func));
}

}
