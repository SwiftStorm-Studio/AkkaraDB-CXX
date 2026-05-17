/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable.
 */

#pragma once

#if defined(_WIN32) && !defined(AKKARADB_STATIC)
#  if defined(AKKARADB_BUILD_SHARED)
#    define AKDB_API __declspec(dllexport)
#  else
#    define AKDB_API __declspec(dllimport)
#  endif
#else
#  define AKDB_API
#endif

