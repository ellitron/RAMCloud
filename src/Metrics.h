/* Copyright (c) 2011 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for
 * any purpose with or without fee is hereby granted, provided that
 * the above copyright notice and this permission notice appear in all
 * copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL
 * WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL
 * AUTHORS BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR
 * CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS
 * OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
 * NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef RAMCLOUD_METRICS_H
#define RAMCLOUD_METRICS_H

#if !DISABLE_METRICS
#include <cstdatomic>
namespace RAMCloud {
typedef std::atomic_ulong Metric;
} // namespace RAMCloud
#else
#include "NoOp.h"
namespace RAMCloud {
typedef RAMCloud::NoOp<uint64_t> Metric;
} // namespace RAMCloud
#endif

// this file is automatically generated from scripts/metrics.py
#include "Metrics.in.h"

namespace RAMCloud {
extern Metrics* metrics;
void dump(const Metrics* metrics);
void reset(Metrics* metrics, uint64_t serverId, uint64_t serverRole);

// These metrics are used to track activity on the server
// during BenchMarks. They are triggered and populated by magic keyid
// RC::MasterService::TOTAL_READ_REQUESTS_OBJID. Also see Bench.cc
struct ServerStats {
  uint64_t totalReadRequests;
  uint64_t totalReadNanos;
  uint64_t totalWriteRequests;
  uint64_t totalWriteNanos;
  uint64_t totalBackupSyncs;
  uint64_t totalBackupSyncNanos;
  uint64_t serverWaitNanos;
  uint64_t infrcSendReplyNanos;
  uint64_t infrcGetTxBufferNanos;
  uint64_t infrcGetTxCount;
  uint64_t gtbPollCount;
  uint64_t gtbPollNanos;
  uint64_t gtbPollZeroNanos;
  uint64_t gtbPollNonZeroNanos;
  uint64_t gtbPollZeroNCount;
  uint64_t gtbPollNonZeroNAvg;
};

extern ServerStats serverStats;
} // namespace RAMCloud

#endif
