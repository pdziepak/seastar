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
 * Copyright (C) 2018 ScyllaDB Ltd.
 */

#include "perf_tests.hh"

#include <regex>

#include <boost/range.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>

#include <fmt/ostream.h>

#include "core/app-template.hh"
#include "core/thread.hh"

namespace perf_tests {
namespace internal {

namespace {

// We need to use signal-based timer instead of seastar ones so that
// tests that do not suspend can be interrupted.
// This causes no overhead though since the timer is used only in a dry run.
class signal_timer {
    std::function<void()> _fn;
    timer_t _timer;
public:
    explicit signal_timer(std::function<void()> fn) : _fn(fn) {
        sigevent se{};
        se.sigev_notify = SIGEV_SIGNAL;
        se.sigev_signo = SIGALRM;
        se.sigev_value.sival_ptr = this;
        auto ret = timer_create(CLOCK_MONOTONIC, &se, &_timer);
        if (ret) {
            throw std::system_error(ret, std::system_category());
        }
    }

    ~signal_timer() {
        timer_delete(_timer);
    }

    void arm(std::chrono::steady_clock::duration dt) {
        time_t sec = std::chrono::duration_cast<std::chrono::seconds>(dt).count();
        auto nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(dt).count();
        nsec -= std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::seconds(sec)).count();

        itimerspec ts{};
        ts.it_value.tv_sec = sec;
        ts.it_value.tv_nsec = nsec;
        auto ret = timer_settime(_timer, 0, &ts, nullptr);
        if (ret) {
            throw std::system_error(ret, std::system_category());
        }
    }

    void cancel() {
        itimerspec ts{};
        auto ret = timer_settime(_timer, 0, &ts, nullptr);
        if (ret) {
            throw std::system_error(ret, std::system_category());
        }
    }
public:
    static void init() {
        struct sigaction sa{};
        sa.sa_sigaction = &signal_timer::signal_handler;
        sa.sa_flags = SA_SIGINFO;
        auto ret = sigaction(SIGALRM, &sa, nullptr);
        if (ret) {
            throw std::system_error(ret, std::system_category());
        }
    }
private:
    static void signal_handler(int, siginfo_t* si, void*) {
        auto t = static_cast<signal_timer*>(si->si_value.sival_ptr);
        t->_fn();
    }
};

}

time_measurement measure_time;

struct config {
    uint64_t single_run_iterations;
    std::chrono::nanoseconds single_run_duration;
    unsigned number_of_runs;
};

struct result {
    sstring test_name;

    uint64_t total_iterations;
    unsigned runs;

    double median;
    double mad;
    double min;
    double max;
};

class result_printer {
protected:
    virtual void do_print_configuration(const config&) = 0;
    virtual void do_print_result(const result&) = 0;

public:
    virtual ~result_printer() = default;

private:
    static std::vector<std::unique_ptr<result_printer>> _printers;

public:
    static void add_result_printer(std::unique_ptr<result_printer> rp) {
        _printers.emplace_back(std::move(rp));
    }

    static void print_configuration(const config& c) {
        for (auto& rp : _printers) {
            rp->do_print_configuration(c);
        }
    }

    static void print_result(const result& r) {
        for (auto& rp : _printers) {
            rp->do_print_result(r);
        }
    }
};

std::vector<std::unique_ptr<result_printer>> result_printer::_printers;

namespace {

struct duration {
    double value;
};

static inline std::ostream& operator<<(std::ostream& os, duration d)
{
    auto value = d.value;
    if (value < 1'000) {
        os << fmt::format("{:.3f}ns", value);
    } else if (value < 1'000'000) {
        // fmt hasn't discovered unicode yet so we are stuck with uicroseconds
        // See: https://github.com/fmtlib/fmt/issues/628
        os << fmt::format("{:.3f}us", value / 1'000);
    } else if (value < 1'000'000'000) {
        os << fmt::format("{:.3f}ms", value / 1'000'000);
    } else {
        os << fmt::format("{:.3f}s", value / 1'000'000'000);
    }
    return os;
}

}

static constexpr auto format_string = "{:<40} {:>11} {:>11} {:>11} {:>11} {:>11}\n";

struct stdout_printer final : result_printer {
  virtual void do_print_configuration(const config& c) override {
    fmt::print("{:<25} {}\n{:<25} {}\n{:<25} {}\n\n",
               "single run iterations:", c.single_run_iterations,
               "single run duration:", duration { double(c.single_run_duration.count()) },
               "number of runs:", c.number_of_runs);
    fmt::print(format_string, "test", "iterations", "median", "mad", "min", "max");
  }

  virtual void do_print_result(const result& r) override {
    fmt::print(format_string, r.test_name, r.total_iterations / r.runs, duration { r.median },
               duration { r.mad }, duration { r.min }, duration { r.max });
  }
};

void performance_test::do_run(const config& conf)
{
    _max_single_run_iterations = conf.single_run_iterations;
    if (!_max_single_run_iterations) {
        _max_single_run_iterations = std::numeric_limits<uint64_t>::max();
    }

    signal_timer tmr([this] {
        _max_single_run_iterations.store(0, std::memory_order_relaxed);
    });

    // dry run, estimate the number of iterations
    if (conf.single_run_duration.count()) {
        // switch out of seastar thread
        later().then([&] {
            tmr.arm(conf.single_run_duration);
            return do_single_run().finally([&] {
                tmr.cancel();
                _max_single_run_iterations = _single_run_iterations;
            });
        }).get();
    }

    auto results = std::vector<double>(conf.number_of_runs);
    uint64_t total_iterations = 0;
    for (auto i = 0u; i < conf.number_of_runs; i++) {
        // switch out of seastar thread
        later().then([&] {
            _single_run_iterations = 0;
            return do_single_run().then([&] (clock_type::duration dt) {
                double ns = std::chrono::duration_cast<std::chrono::nanoseconds>(dt).count();
                results[i] = ns / _single_run_iterations;

                total_iterations += _single_run_iterations;
            });
        }).get();
    }

    result r{};
    r.test_name = name();
    r.total_iterations = total_iterations;
    r.runs = conf.number_of_runs;

    auto mid = conf.number_of_runs / 2;

    boost::range::sort(results);
    r.median = results[mid];

    auto diffs = boost::copy_range<std::vector<double>>(
        results | boost::adaptors::transformed([&] (double x) { return fabs(x - r.median); })
    );
    boost::range::sort(diffs);
    r.mad = diffs[mid];

    r.min = results[0];
    r.max = results[results.size() - 1];

    result_printer::print_result(r);
}

void performance_test::run(const config& conf)
{
    set_up();
    try {
        do_run(conf);
    } catch (...) {
        tear_down();
        throw;
    }
    tear_down();
}

std::vector<std::unique_ptr<performance_test>>& all_tests()
{
    static std::vector<std::unique_ptr<performance_test>> tests;
    return tests;
}

void performance_test::register_test(std::unique_ptr<performance_test> test)
{
    all_tests().emplace_back(std::move(test));
}

void run_all(const std::vector<std::string>& tests, const config& conf)
{
    auto can_run = [tests = boost::copy_range<std::vector<std::regex>>(tests)] (auto&& test) {
        auto it = boost::range::find_if(tests, [&test] (const std::regex& regex) {
            return std::regex_match(test->name(), regex);
        });
        return tests.empty() || it != tests.end();
    };

    result_printer::print_configuration(conf);
    for (auto&& test : all_tests() | boost::adaptors::filtered(std::move(can_run))) {
        test->run(conf);
    }
}

}
}

int main(int ac, char** av)
{
    using namespace perf_tests::internal;
    namespace bpo = boost::program_options;

    app_template app;
    app.add_options()
        ("iterations,i", bpo::value<size_t>()->default_value(0),
            "number of iterations in a single run")
        ("duration,d", bpo::value<double>()->default_value(1),
            "duration of a single run in seconds")
        ("runs,r", bpo::value<size_t>()->default_value(5), "number of runs")
        ("test,t", bpo::value<std::vector<std::string>>(), "tests to execute")
        ("list", "list available tests")
        ;

    return app.run(ac, av, [&] {
        return async([&] {
            signal_timer::init();

            config conf;
            conf.single_run_iterations = app.configuration()["iterations"].as<size_t>();
            auto dur = std::chrono::duration<double>(app.configuration()["duration"].as<double>());
            conf.single_run_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(dur);
            conf.number_of_runs = app.configuration()["runs"].as<size_t>();

            std::vector<std::string> tests_to_run;
            if (app.configuration().count("test")) {
                tests_to_run = app.configuration()["test"].as<std::vector<std::string>>();
            }

            if (app.configuration().count("list")) {
                fmt::print("available tests:\n");
                for (auto&& t : all_tests()) {
                    fmt::print("\t{}\n", t->name());
                }
                return;
            }

            result_printer::add_result_printer(std::make_unique<stdout_printer>());

            run_all(tests_to_run, conf);
        });
    });
}
