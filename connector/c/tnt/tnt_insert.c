
/*
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include <connector/c/include/tarantool/tnt_proto.h>
#include <connector/c/include/tarantool/tnt_tuple.h>
#include <connector/c/include/tarantool/tnt_request.h>
#include <connector/c/include/tarantool/tnt_reply.h>
#include <connector/c/include/tarantool/tnt_stream.h>
#include <connector/c/include/tarantool/tnt_insert.h>

/*
 * tnt_insert()
 *
 * write insert request to stream;
 *
 * s     - stream pointer
 * ns    - space
 * flags - request flags
 * kv    - tuple key-value
 * 
 * returns number of bytes written, or -1 on error.
*/
ssize_t
tnt_insert(struct tnt_stream *s, uint32_t ns, uint32_t flags,
	   struct tnt_tuple *kv)
{
	/* filling major header */
	struct tnt_header hdr;
	hdr.type  = TNT_OP_INSERT;
	hdr.len = sizeof(struct tnt_header_insert) + kv->size;
	hdr.reqid = s->reqid;
	/* filling insert header */
	struct tnt_header_insert hdr_insert;
	hdr_insert.ns = ns;
	hdr_insert.flags = flags;
	/* writing data to stream */
	struct iovec v[3];
	v[0].iov_base = (void *)&hdr;
	v[0].iov_len  = sizeof(struct tnt_header);
	v[1].iov_base = (void *)&hdr_insert;
	v[1].iov_len  = sizeof(struct tnt_header_insert);
	v[2].iov_base = kv->data;
	v[2].iov_len  = kv->size;
	return s->writev(s, v, 3);
}
