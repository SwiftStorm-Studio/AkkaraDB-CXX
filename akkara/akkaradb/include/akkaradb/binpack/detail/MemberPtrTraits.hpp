/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable.
 */

#pragma once

#include <string_view>
#include <type_traits>

namespace akkaradb::binpack::detail {
    template <typename T>
    struct MemberPtrTraits;

    template <typename Class, typename Member>
    struct MemberPtrTraits<Member Class::*> {
        using ClassType = Class;
        using MemberType = std::remove_cv_t<Member>;
    };

    template <auto MPtr>
    using class_of = typename MemberPtrTraits<decltype(MPtr)>::ClassType;

    template <auto MPtr>
    using member_of = typename MemberPtrTraits<decltype(MPtr)>::MemberType;

    template <auto MPtr>
    [[nodiscard]] inline std::string_view member_name() noexcept {
        #if defined(_MSC_VER)
        const std::string_view sig = __FUNCSIG__; const auto amp_lt = sig.rfind("<&"); if (amp_lt == std::string_view::npos) { return {}; } const auto gt = sig.
            find('>', amp_lt); if (gt == std::string_view::npos) { return {}; } const auto cc = sig.rfind("::", gt); if (cc == std::string_view::npos || cc <
            amp_lt) { return {}; } return sig.substr(cc + 2, gt - cc - 2);
        #elif defined(__clang__)
        const std::string_view sig = __PRETTY_FUNCTION__; const auto rb = sig.rfind(']'); if (rb == std::string_view::npos) { return {}; } const auto cc = sig.
            rfind("::", rb); if (cc == std::string_view::npos) { return {}; } return sig.substr(cc + 2, rb - cc - 2);
        #else
        const std::string_view sig = __PRETTY_FUNCTION__;
        const auto rb = sig.rfind(']');
        if (rb == std::string_view::npos) { return {}; }
        const auto rp = sig.rfind(')', rb);
        if (rp == std::string_view::npos) { return {}; }
        const auto cc = sig.rfind("::", rp);
        if (cc == std::string_view::npos) { return {}; }
        return sig.substr(cc + 2, rp - cc - 2);
        #endif
    }
} // namespace akkaradb::binpack::detail

