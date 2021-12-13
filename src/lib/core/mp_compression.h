#pragma once
/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include <stdint.h>
#include <stdio.h>
#include "tt_compression.h"

#if defined(__cplusplus)
extern "C" {
#endif

/**
 * Return the maximum size that the data from @a ttc data array
 * will occupy after compression.
 */
uint32_t
mp_sizeof_compression_max(const struct tt_compression *ttc);

/**
 * Decode @a ttc structure from compressed msgpack field @a data
 * with already decoded ext header.
 */
int
compression_unpack(const char **data, uint32_t len,
                   struct tt_compression *ttc);

/**
 * Encode @a ttc structure from @a data. Save compressed data in
 * data array in @a ttc structure.
 */
char *
mp_encode_compression(char *data, const struct tt_compression *ttc);

/**
 * Decode @a ttc structure from compressed msgpack field @a data. Save
 * decompressed data in data array in @a ttc structure. Return 0 if
 * success, otherwise return -1.
 */
int
mp_decode_compression(const char **data, struct tt_compression *ttc);

/**
 * Print compressed data string representation into a given buffer.
 */
int
mp_snprint_compression(char *buf, int size, const char **data, uint32_t len);

/**
 * Print compressed data string representation into a stream.
 */
int
mp_fprint_compression(FILE *file, const char **data, uint32_t len);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */
