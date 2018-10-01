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
 * Copyright (C) 2018 Scylladb, Ltd.
 */

#include "lz4_fragmented_compressor.hh"
#include "core/byteorder.hh"

namespace seastar {
namespace rpc {

const sstring lz4_fragmented_compressor::factory::_name = "LZ4_FRAGMENTED";

// Compressed message format:
// The message consists of one or more data chunks each preceeded by a 4 byte header.
// The value of the header detrmines the size of the chunk:
// - most significant bit is cleared: intermediate chunk, 31 least significant bits
//   contain the compressed size of the chunk (i.e. how it appears on wire), the
//   decompressed size is 128 kB.
// - most significant bit is set: last chunk, 31 least significant bits contain the
//   decompressed size of the chunk, the decompressed size is the remaning part of
//   the message.
// Compression and decompression is done using LZ4 streaming interface. Each chunk
// depends on the one that precedes it.

static constexpr uint32_t last_chunk_flag = uint32_t(1) << 31;
static constexpr size_t chunk_header_size = sizeof(uint32_t);
static constexpr size_t chunk_size = 128 * 1024;

namespace {

struct compression_stream_deleter {
    void operator()(LZ4_stream_t* stream) const noexcept {
        LZ4_freeStream(stream);
    }
};

struct decompression_stream_deleter {
    void operator()(LZ4_streamDecode_t* stream) const noexcept {
        LZ4_freeStreamDecode(stream);
    }
};

}

snd_buf lz4_fragmented_compressor::compress(size_t head_space, snd_buf data) {
    static thread_local auto stream = std::unique_ptr<LZ4_stream_t, compression_stream_deleter>(LZ4_createStream());
    static_assert(chunk_size == snd_buf::chunk_size, "Chunk size mismatch.");

    LZ4_resetStream(stream.get());

    auto size_left = data.size;
    auto src = compat::get_if<temporary_buffer<char>>(&data.bufs);
    if (!src) {
        src = compat::get<std::vector<temporary_buffer<char>>>(data.bufs).data();
    }

    auto single_chunk_size = LZ4_COMPRESSBOUND(size_left) + head_space + chunk_header_size;
    if (single_chunk_size <= chunk_size && size_left <= chunk_size) {
        // faster path for small messages
        assert(src->size() == size_left);
        auto dst = temporary_buffer<char>(single_chunk_size);
        auto header = dst.get_write() + head_space;
        auto compressed_data = header + chunk_header_size;
        auto compressed_size = LZ4_compress_fast_continue(stream.get(), src->get(), compressed_data, size_left, LZ4_COMPRESSBOUND(size_left), 0);
        write_le(header, last_chunk_flag | size_left);
        dst.trim(head_space + chunk_header_size + compressed_size);
        return snd_buf(std::move(dst));
    }

    static constexpr size_t dst_capacity = LZ4_COMPRESSBOUND(chunk_size);
    static thread_local char chunk_data[chunk_header_size + dst_capacity];
    auto header = chunk_data;
    auto dst = header + chunk_header_size;
    std::vector<temporary_buffer<char>> dst_buffers;
    size_t dst_offset = head_space;

    dst_buffers.emplace_back(std::max<size_t>(head_space, chunk_size));

    auto write_chunk_to_dst = [&] (uint32_t header_value, size_t n) {
        write_le(header, header_value);
        n += chunk_header_size;
        auto src = chunk_data;
        while (n) {
            if (dst_offset == dst_buffers.back().size()) {
                dst_buffers.emplace_back(chunk_size);
                dst_offset = 0;
            }
            auto& dst = dst_buffers.back();
            auto this_length = std::min(dst.size() - dst_offset, n);
            std::copy_n(src, this_length, dst.get_write() + dst_offset);
            n -= this_length;
            dst_offset += this_length;
            src += this_length;
        }
    };

    // Intermediate chunks
    size_t total_compressed_size = 0;
    auto src_left = data.size;
    while (src_left > chunk_size) {
        assert(src->size() == chunk_size);
        auto compressed_size = LZ4_compress_fast_continue(stream.get(), src->get(), dst, chunk_size, dst_capacity, 0);
        src_left -= chunk_size;
        total_compressed_size += compressed_size + chunk_header_size;
        ++src;
        write_chunk_to_dst(compressed_size, compressed_size);
    }

    // Last chunk
    auto compressed_size = LZ4_compress_fast_continue(stream.get(), src->get(), dst, src_left, dst_capacity, 0);
    write_chunk_to_dst(last_chunk_flag | src_left, compressed_size);
    total_compressed_size += compressed_size + chunk_header_size + head_space;

    auto& last = dst_buffers.back();
    last.trim(dst_offset);

    if (dst_buffers.size() == 1) {
        return snd_buf(std::move(dst_buffers.front()));
    }
    return snd_buf(std::move(dst_buffers), total_compressed_size);
}

rcv_buf lz4_fragmented_compressor::decompress(rcv_buf data) {
    if (data.size < 4) {
        return rcv_buf();
    }

    static thread_local auto stream = std::unique_ptr<LZ4_streamDecode_t, decompression_stream_deleter>(LZ4_createStreamDecode());

    if (!LZ4_setStreamDecode(stream.get(), nullptr, 0)) {
        throw std::runtime_error("RPC frame LZ4 decompression failed to reset state");
    }

    auto src = compat::get_if<temporary_buffer<char>>(&data.bufs);
    size_t src_left = data.size;
    size_t src_offset = 0;

    auto copy_src = [&] (char* dst, size_t n) {
        src_left -= n;
        while (n) {
            if (src_offset == src->size()) {
                ++src;
                src_offset = 0;
            }
            auto this_size = std::min(n, src->size() - src_offset);
            std::copy_n(src->get() + src_offset, this_size, dst);
            n -= this_size;
            dst += this_size;
            src_offset += this_size;
        }
    };

    auto read_header = [&] {
        uint32_t header_value;
        copy_src(reinterpret_cast<char*>(&header_value), chunk_header_size);
        return le_to_cpu(header_value);
    };

    if (src) {
        uint32_t header;
        std::copy_n(src->get(), chunk_header_size, reinterpret_cast<char*>(&header));
        header = le_to_cpu(header);
        if (header & last_chunk_flag) {
            // faster path for small messages: single chunk in a single buffer
            header &= ~last_chunk_flag;
            src_offset += chunk_header_size;
            src_left -= chunk_header_size;
            auto dst = temporary_buffer<char>(header);
            if (LZ4_decompress_safe_continue(stream.get(), src->get() + src_offset, dst.get_write(), src_left, header) < 0) {
                throw std::runtime_error("RPC frame LZ4 decompression failure");
            }
            return rcv_buf(std::move(dst));
        }
        // not egligible for fast path: multiple chunks in a single buffer
    } else {
        // not egligible for fast path: multiple buffers
        src = compat::get<std::vector<temporary_buffer<char>>>(data.bufs).data();
    }

    // Let's be a bit paranoid and not assume that the remote has the same
    // LZ4_COMPRESSBOUND as we do and allow any compressed chunk size.
    static thread_local auto chunk_buffer = temporary_buffer<char>(LZ4_COMPRESSBOUND(chunk_size));

    std::vector<temporary_buffer<char>> dst_buffers;
    size_t total_size = 0;

    // Intermediate chunks
    uint32_t header_value = read_header();
    while (!(header_value & last_chunk_flag)) {
        total_size += chunk_size;
        dst_buffers.emplace_back(chunk_size);
        if (chunk_buffer.size() < header_value) {
            chunk_buffer = temporary_buffer<char>(header_value);
        }
        copy_src(chunk_buffer.get_write(), header_value);
        if (LZ4_decompress_safe_continue(stream.get(), chunk_buffer.get(), dst_buffers.back().get_write(), header_value, chunk_size) < 0) {
            throw std::runtime_error("RPC frame LZ4 decompression failure");
        }
        header_value = read_header();
    }

    // Last chunk
    header_value &= ~last_chunk_flag;
    total_size += header_value;
    dst_buffers.emplace_back(header_value);
    if (chunk_buffer.size() < src_left) {
        chunk_buffer = temporary_buffer<char>(src_left);
    }
    auto last_chunk_compressed_size = src_left;
    copy_src(chunk_buffer.get_write(), src_left);
    if (LZ4_decompress_safe_continue(stream.get(), chunk_buffer.get(), dst_buffers.back().get_write(), last_chunk_compressed_size, header_value) < 0) {
        throw std::runtime_error("RPC frame LZ4 decompression failure");
    }

    if (dst_buffers.size() == 1) {
        return rcv_buf(std::move(dst_buffers.front()));
    }
    return rcv_buf(std::move(dst_buffers), total_size);
}

}
}
