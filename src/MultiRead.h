/* Copyright (c) 2012 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef RAMCLOUD_MULTIREAD_H
#define RAMCLOUD_MULTIREAD_H

#include "RamCloud.h"
#include "RpcWrapper.h"
#include "Transport.h"
#include "WireFormat.h"

namespace RAMCloud {

/**
 * This class implements the client side of multiRead operations. It
 * manages multiple concurrent RPCs, each requesting one or more objects
 * from a single server.  The behavior of this class is similar to an
 * RpcWrapper, but it isn't an RpcWrapper subclass because it doesn't
 * correspond to a single RPC.
 */
class MultiRead {
  public:
    MultiRead(RamCloud& ramcloud, MultiReadObject* requests[],
            uint32_t numRequests);
    ~MultiRead() {}
    void cancel();
    bool isReady();
    void wait();

  PRIVATE:
    bool startRpcs();

    /// A special Status value indicating than an RPC is underway but
    /// we haven't yet seen the response.
    static const Status UNDERWAY = Status(STATUS_MAX_VALUE+1);

    /// Encapsulates the state of a single RPC sent to a single server.
    class PartRpc : public RpcWrapper {
        friend class MultiRead;
      public:
        PartRpc(RamCloud& ramcloud, Transport::SessionRef session);
        ~PartRpc() {}
        void finish();
        bool handleTransportError();
        void send();

        /// Overall client state information.
        RamCloud& ramcloud;

        /// Session that will be used to transmit the RPC.
        Transport::SessionRef session;

        /// Information about all of the objects that are being requested
        /// in this RPC.
#ifdef TESTING
        static const uint32_t MAX_OBJECTS_PER_RPC = 3;
#else
        static const uint32_t MAX_OBJECTS_PER_RPC = 100;
#endif
        MultiReadObject* requests[MAX_OBJECTS_PER_RPC];

        /// Header for the RPC (used to update count as objects are added).
        WireFormat::MultiRead::Request& reqHdr;

        DISALLOW_COPY_AND_ASSIGN(PartRpc);
    };

    /// Overall client state information.
    RamCloud& ramcloud;

    /// Copy of constructor argument containing information about
    /// desired objects.
    MultiReadObject** requests;

    /// Copy constructor argument giving size of \c requests.
    uint32_t numRequests;

    /// An array holding the constituent RPCs that we are managing.
#ifdef TESTING
    static const uint32_t MAX_RPCS = 2;
#else
    static const uint32_t MAX_RPCS = 10;
#endif
    Tub<PartRpc> rpcs[MAX_RPCS];

    /// Set by \c cancel.
    bool canceled;

    DISALLOW_COPY_AND_ASSIGN(MultiRead);
};

} // end RAMCloud

#endif  // RAMCLOUD_MULTIREAD_H
