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

#include <seastar/core/future.hh>

#if !SEASTAR_COROUTINES_TS
#error Coroutines TS support disabled.
#endif

#if __has_include(<experimental/coroutine>)
#include <experimental/coroutine>
#else

// We are not exactly allowed to defined anything in the std namespace, but this
// makes coroutines work with libstdc++. All of this is experimental anyway.

namespace std::experimental {

template<typename Promise>
class coroutine_handle {
    void* _pointer = nullptr;
public:
    coroutine_handle() = default;

    coroutine_handle &operator=(std::nullptr_t) noexcept {
        _pointer = nullptr;
        return *this;
    }

    explicit operator bool() const noexcept { return _pointer; }

    static coroutine_handle from_address(void* ptr) noexcept {
        coroutine_handle hndl;
        hndl._pointer =ptr;
        return hndl;
    }
    void* address() const noexcept { return _pointer; }

    static coroutine_handle from_promise(Promise& promise) noexcept {
        coroutine_handle hndl;
        hndl._pointer = __builtin_coro_promise(&promise, alignof(Promise), true);
        return hndl;
    }
    Promise& promise() const noexcept {
        return *reinterpret_cast<Promise*>(__builtin_coro_promise(_pointer, alignof(Promise), false));
    }

    void operator()() noexcept { resume(); }

    void resume() const noexcept { __builtin_coro_resume(_pointer); }
    void destroy() const noexcept { __builtin_coro_destroy(_pointer); }
    bool done() const noexcept { return __builtin_coro_done(_pointer); }
};

struct suspend_never {
    constexpr bool await_ready() const noexcept { return true; }
    template<typename T>
    constexpr void await_suspend(coroutine_handle<T>) noexcept { }
    constexpr void await_resume() noexcept { }
};

struct suspend_always {
    constexpr bool await_ready() const noexcept { return false; }
    template<typename T>
    constexpr void await_suspend(coroutine_handle<T>) noexcept { }
    constexpr void await_resume() noexcept { }
};

template<typename T, typename... Args>
class coroutine_traits { };

}

#endif

namespace std::experimental {

template<typename... T, typename... Args>
class coroutine_traits<seastar::future<T...>, Args...> {
public:
    class promise_type final : public seastar::task {
        seastar::promise<T...> _promise;
    public:
        promise_type() = default;
        promise_type(promise_type&&) = delete;
        promise_type(const promise_type&) = delete;

        template<typename... U>
        void return_value(U&&... value) {
            _promise.set_value(std::forward<U>(value)...);
        }
        void unhandled_exception() noexcept {
            _promise.set_exception(std::current_exception());
        }

        seastar::future<T...> get_return_object() noexcept {
            return _promise.get_future();
        }

        suspend_never initial_suspend() noexcept { return { }; }
        suspend_never final_suspend() noexcept { return { }; }

        virtual void run_and_dispose() noexcept override {
            auto handle = std::experimental::coroutine_handle<promise_type>::from_promise(*this);
            handle.resume();
        }
        virtual void dispose() noexcept override {
            auto handle = std::experimental::coroutine_handle<promise_type>::from_promise(*this);
            handle.destroy();
        }
    };
};

template<typename... Args>
class coroutine_traits<seastar::future<>, Args...> {
public:
   class promise_type final : public seastar::task {
        seastar::promise<> _promise;
    public:
        promise_type() = default;
        promise_type(promise_type&&) = delete;
        promise_type(const promise_type&) = delete;

        void return_void() noexcept {
            _promise.set_value();
        }
        void unhandled_exception() noexcept {
            _promise.set_exception(std::current_exception());
        }

        seastar::future<> get_return_object() noexcept {
            return _promise.get_future();
        }

        suspend_never initial_suspend() noexcept { return { }; }
        suspend_never final_suspend() noexcept { return { }; }

        virtual void run_and_dispose() noexcept override {
            auto handle = std::experimental::coroutine_handle<promise_type>::from_promise(*this);
            handle.resume();
        }
        virtual void dispose() noexcept override {
            auto handle = std::experimental::coroutine_handle<promise_type>::from_promise(*this);
            handle.destroy();
        }
    };
};

}

namespace seastar {

namespace internal {

template<typename... T>
struct awaiter {
    seastar::future<T...> _future;
public:
    explicit awaiter(seastar::future<T...>&& f) noexcept : _future(std::move(f)) { }

    awaiter(const awaiter&) = delete;
    awaiter(awaiter&&) = delete;

    bool await_ready() const noexcept {
        return _future.available();
    }

    template<typename U>
    void await_suspend(std::experimental::coroutine_handle<U> hndl) noexcept {
        auto& p = hndl.promise();
        _future.set_coroutine(task_ptr(&p));
    }

    std::tuple<T...> await_resume() { return _future.get(); }
};

template<typename T>
struct awaiter<T> {
    seastar::future<T> _future;
public:
    explicit awaiter(seastar::future<T>&& f) noexcept : _future(std::move(f)) { }

    awaiter(const awaiter&) = delete;
    awaiter(awaiter&&) = delete;

    bool await_ready() const noexcept {
        return _future.available();
    }

    template<typename U>
    void await_suspend(std::experimental::coroutine_handle<U> hndl) noexcept {
        auto& p = hndl.promise();
        _future.set_coroutine(task_ptr(&p));
    }

    T await_resume() { return _future.get0(); }
};

template<>
struct awaiter<> {
    seastar::future<> _future;
public:
    explicit awaiter(seastar::future<>&& f) noexcept : _future(std::move(f)) { }

    awaiter(const awaiter&) = delete;
    awaiter(awaiter&&) = delete;

    bool await_ready() const noexcept {
        return _future.available();
    }

    template<typename U>
    void await_suspend(std::experimental::coroutine_handle<U> hndl) noexcept {
        auto& p = hndl.promise();
        _future.set_coroutine(task_ptr(&p));
    }

    void await_resume() { _future.get(); }
};

}

template<typename... T>
auto operator co_await(future<T...> f) noexcept {
    return internal::awaiter<T...>(std::move(f));
}

}
