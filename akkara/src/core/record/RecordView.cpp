// RecordView.cpp
#include "akkara/core/record/RecordView.hpp"
#include <stdexcept>

namespace akkaradb::core
{
    RecordView RecordView::parse(BufferView buf, size_t& offset)
    {
        // Read header (32 bytes)
        if (offset + AKHdr32::SIZE > buf.size())
        {
            throw std::out_of_range("RecordView::parse: insufficient space for header");
        }

        AKHdr32 hdr = AKHdr32::read_from(buf, offset);
        offset += AKHdr32::SIZE;

        // Validate lengths
        if (hdr.k_len == 0)
        {
            throw std::invalid_argument("RecordView::parse: key length is zero");
        }

        if (const size_t required = static_cast<size_t>(hdr.k_len) + hdr.v_len; offset + required > buf.size())
        {
            throw std::out_of_range("RecordView::parse: insufficient space for key/value");
        }

        // Extract key
        std::string_view key = buf.as_string_view(offset, hdr.k_len);
        offset += hdr.k_len;

        // Extract value
        std::string_view value = buf.as_string_view(offset, hdr.v_len);
        offset += hdr.v_len;

        return RecordView{hdr, key, value};
    }
} // namespace akkaradb::core