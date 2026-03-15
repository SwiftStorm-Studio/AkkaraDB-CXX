/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 */

#include "engine/server/ApiFraming.hpp"
#include "core/CRC32C.hpp"
#include <cstring>

namespace akkaradb::server {

    void encode_response(ApiStatus                status,
                         uint32_t                 request_id,
                         std::span<const uint8_t> val,
                         std::vector<uint8_t>&    buf) {
        ApiResponseHeader hdr{};
        std::memcpy(hdr.magic, RESPONSE_MAGIC, 4);
        hdr.status     = status;
        hdr.request_id = request_id;
        hdr.val_len    = static_cast<uint32_t>(val.size());

        const uint32_t crc = core::CRC32C::compute(val.data(), val.size());

        buf.resize(sizeof(ApiResponseHeader) + val.size() + sizeof(uint32_t));
        uint8_t* p = buf.data();
        std::memcpy(p, &hdr, sizeof(hdr));           p += sizeof(hdr);
        std::memcpy(p, val.data(), val.size());       p += val.size();
        std::memcpy(p, &crc, sizeof(crc));
    }

    void encode_error(uint32_t request_id, std::vector<uint8_t>& buf) {
        encode_response(ApiStatus::Error, request_id, {}, buf);
    }

} // namespace akkaradb::server
