/* Copyright (c) 2015 Stanford University
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

#include "ClientTransactionManager.h"
#include "ClientTransactionTask.h"
#include "ClientException.h"
#include "Transaction.h"
#include "Cycles.h"

namespace RAMCloud {

/**
 * Constructor for a transaction.
 *
 * \param ramcloud
 *      Overall information about the calling client.
 */
Transaction::Transaction(RamCloud* ramcloud)
    : ramcloud(ramcloud)
    , taskPtr(new ClientTransactionTask(ramcloud))
    , commitStarted(false)
    , nextReadBatchPtr()
{
}

/**
 * Commits the transaction defined by the operations performed on this
 * transaction (read, remove, write).  This method blocks until a decision is
 * reached and sent to all participant servers but does not wait of the
 * participant servers to acknowledge the decision (e.g. does not wait to sync).
 *
 * \return
 *      True if the transaction was able to commit.  False otherwise.
 */
bool
Transaction::commit()
{
    ClientTransactionTask* task = taskPtr.get();

    if (!commitStarted) {
        commitStarted = true;
        ramcloud->transactionManager->startTransactionTask(taskPtr);
    }

    while (!task->allDecisionsSent()) {
        ramcloud->transactionManager->poll();
        ramcloud->poll();
    }

    if (expect_false(task->getDecision() ==
            WireFormat::TxDecision::UNDECIDED)) {
        ClientException::throwException(HERE, STATUS_INTERNAL_ERROR);
    }

    return (task->getDecision() == WireFormat::TxDecision::COMMIT);
}

/**
 * Block until the decision of this transaction commit is accepted by all
 * participant servers.  If the commit has not yet occurred and a decision is
 * not yet reached, this method will also start the commit.
 *
 * This method is used mostly for testing and benchmarking.
 */
void
Transaction::sync()
{
    ClientTransactionTask* task = taskPtr.get();

    if (!commitStarted) {
        commitStarted = true;
        ramcloud->transactionManager->startTransactionTask(taskPtr);
    }

    while (!task->isReady()) {
        ramcloud->transactionManager->poll();
        ramcloud->poll();
    }
}

/**
 * Commits the transaction defined by the operations performed on this
 * transaction (read, remove, write).  This method blocks until a participant
 * servers have accepted the decision.
 *
 * \return
 *      True if the transaction was able to commit.  False otherwise.
 */
bool
Transaction::commitAndSync()
{
    sync();
    return commit();
}

/**
 * Read the current contents of an object as part of this transaction.
 *
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated.
 * \param keyLength
 *      Size in bytes of the key.
 * \param[out] value
 *      After a successful return, this Buffer will hold the
 *      contents of the desired object - only the value portion of the object.
 * \param[out] objectExists
 *      If non-NULL, the ObjectDoesntExistException is not thrown and a flag
 *      indicating the existence of the object is returned here.
 */
void
Transaction::read(uint64_t tableId, const void* key, uint16_t keyLength,
        Buffer* value, bool* objectExists)
{
    ReadOp readOp(this, tableId, key, keyLength, value);
    readOp.wait(objectExists);
}

/**
 * Delete an object from a table as part of this transaction. If the object does
 * not currently exist then the operation succeeds without doing anything.
 *
 * \param tableId
 *      The table containing the object to be deleted (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated.
 * \param keyLength
 *      Size in bytes of the key.
 */
void
Transaction::remove(uint64_t tableId, const void* key, uint16_t keyLength)
{
    if (expect_false(commitStarted)) {
        throw TxOpAfterCommit(HERE);
    }

    ClientTransactionTask* task = taskPtr.get();
    task->readOnly = false;

    Key keyObj(tableId, key, keyLength);
    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(keyObj);

    if (entry == NULL) {
        entry = task->insertCacheEntry(keyObj, NULL, 0);
    } else {
        entry->objectBuf.reset();
        Object::appendKeysAndValueToBuffer(
                keyObj, NULL, 0, &entry->objectBuf, true);
    }

    entry->type = ClientTransactionTask::CacheEntry::REMOVE;
}

/**
 * Replace the value of a given object, or create a new object if none
 * previously existed as part of this transaction.
 *
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated.
 * \param keyLength
 *      Size in bytes of the key.
 * \param buf
 *      Address of the first byte of the new contents for the object;
 *      must contain at least length bytes.
 * \param length
 *      Size in bytes of the new contents for the object.
 */
void
Transaction::write(uint64_t tableId, const void* key, uint16_t keyLength,
        const void* buf, uint32_t length)
{
    if (expect_false(commitStarted)) {
        throw TxOpAfterCommit(HERE);
    }

    if (length > 1048576) { // RAMCloud doesn't support data > 1MB.
        throw RequestTooLargeException(HERE);
    }

    ClientTransactionTask* task = taskPtr.get();
    task->readOnly = false;

    Key keyObj(tableId, key, keyLength);
    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(keyObj);

    if (entry == NULL) {
        entry = task->insertCacheEntry(keyObj, buf, length);
    } else {
        entry->objectBuf.reset();
        Object::appendKeysAndValueToBuffer(
                keyObj, buf, length, &entry->objectBuf, true);
    }

    entry->type = ClientTransactionTask::CacheEntry::WRITE;
}

/**
 * Constructor for Transaction::ReadOp: initiates a read just like
 * #Transaction::read, but returns once the operation has been initiated,
 * without waiting for it to complete.  The operation is not consider part of
 * the transaction until it is waited on.
 *
 * \param transaction
 *      The Transaction object of which this operation is a part.
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated.
 * \param keyLength
 *      Size in bytes of the key.
 * \param[out] value
 *      After a successful return, this Buffer will hold the
 *      contents of the desired object - only the value portion of the object.
 * \param batch
 *      True if this operation can be batched trading latency for throughput.
 *      Defaults to false.
 */
Transaction::ReadOp::ReadOp(Transaction* transaction, uint64_t tableId,
        const void* key, uint16_t keyLength, Buffer* value, bool batch)
    : transaction(transaction)
    , tableId(tableId)
    , keyBuf()
    , keyLength(keyLength)
    , value(value)
    , buf()
    , requestBatched(batch)
    , singleRequest()
    , batchedRequest()
{
    keyBuf.appendCopy(key, keyLength);

    ClientTransactionTask* task = transaction->taskPtr.get();

    Key keyObj(tableId, key, keyLength);
    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(keyObj);

    if (!requestBatched) {
        singleRequest.construct();
    } else {
        batchedRequest.construct();
    }

    // If no cache entry exists an rpc should be issued.
    if (entry == NULL) {
        if (!requestBatched) {
            assert(singleRequest);
            buf.construct();
            singleRequest->readRpc.construct(
                    transaction->ramcloud, tableId, key, keyLength, buf.get());
        } else {
            assert(batchedRequest);

            if (!transaction->nextReadBatchPtr) {
                transaction->nextReadBatchPtr = std::make_shared<ReadBatch>();
            }
            assert(transaction->nextReadBatchPtr);

            batchedRequest->readBatchPtr = transaction->nextReadBatchPtr;
            assert(!batchedRequest->readBatchPtr->rpc);

            batchedRequest->request =
                    {tableId, keyBuf.getRange(0, keyLength), keyLength, &buf};

            batchedRequest->readBatchPtr->requests.push_back(
                    &batchedRequest->request);
        }
    }
    // Otherwise we will just return it from cache when wait is called.
}

/**
 * Indicates whether a response has been received for this ReadOp and thus
 * whether #wait will not block.  Used for asynchronous processing of RPCs.
 * Checking that an ReadOp isReady does not include the operation in the
 * transaction (see #wait).
 *
 * For a batched ReadOp, calling isReady will also trigger the execution of
 * the batch.
 *
 * \return
 *      True if ReadOp #wait will not block; false otherwise.
 */
bool
Transaction::ReadOp::isReady()
{
    if (!requestBatched) {
        assert(singleRequest);
        return (!singleRequest->readRpc || singleRequest->readRpc->isReady());
    } else {
        assert(batchedRequest);

        // Send out the batch request if it has not already been sent.
        if (batchedRequest->readBatchPtr
            && !batchedRequest->readBatchPtr->rpc) {
            assert(batchedRequest->readBatchPtr
                    == transaction->nextReadBatchPtr);
            // Reset the batch pointer so next Op starts a new batch.
            transaction->nextReadBatchPtr.reset();

            batchedRequest->readBatchPtr->rpc.construct(
                    transaction->ramcloud,
                    &batchedRequest->readBatchPtr->requests[0],
                    downCast<uint32_t>(
                            batchedRequest->readBatchPtr->requests.size()));
        }

        return (!batchedRequest->readBatchPtr
                || batchedRequest->readBatchPtr->rpc->isReady());
    }
}

/**
 * Wait for the operation to complete.  The operation is not part of the
 * transaction until wait is called (e.g. if commit is called before wait,
 * this operation will not be included).  Behavior when calling wait more than
 * once is undefined.
 *
 * \param[out] objectExists
 *      If non-NULL, the ObjectDoesntExistException is not thrown and a flag
 *      indicating the existence of the object is returned here.
 */
void
Transaction::ReadOp::wait(bool* objectExists)
{
    if (expect_false(transaction->commitStarted)) {
        throw TxOpAfterCommit(HERE);
    }

    // Determine what type of object we are reading.
    const char* objtypestr;
    char edgeLabel[64];
    edgeLabel[0] = '\0';
    char neighborLabel[64];
    neighborLabel[0] = '\0';
    if (keyLength == 16) {
      objtypestr = "properties";
    } else {
      objtypestr = "edgelist";
      short eLabelLen = *(keyBuf.getOffset<short>(16));
      memcpy(edgeLabel, keyBuf.getOffset<char>(16 + 2), eLabelLen);
      edgeLabel[eLabelLen] = '\0'; 

      short vLabelLen = *(keyBuf.getOffset<short>(16 + 2 + eLabelLen + 1));
      memcpy(neighborLabel, keyBuf.getOffset<char>(16 + 2 + eLabelLen + 1 + 2), vLabelLen);
      neighborLabel[vLabelLen] = '\0'; 
    }

    // When we find out later, store the type of op this was.
    const char *optypestr;

    uint64_t startTime = Cycles::rdtsc();

    ClientTransactionTask* task = transaction->taskPtr.get();

    Key keyObj(tableId, keyBuf, 0, keyLength);
    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(keyObj);

    // Assume we found the object exists unless we find out otherwise.
    bool objectFound = true;

    if (entry == NULL) {
        uint64_t version;
        uint32_t dataLength = 0;
        const void* data = NULL;

        if (!requestBatched) {
            optypestr = "txrd";
            assert(singleRequest);
            // If no entry exists in cache an rpc must have been issued.
            assert(singleRequest->readRpc);

            singleRequest->readRpc->wait(&version, &objectFound);
            if (objectFound)
                data = buf->getValue(&dataLength);
        } else {
            optypestr = "txbatchrd";
            assert(batchedRequest);
            // If no entry exists in cache a batch must have been assigned.
            assert(batchedRequest->readBatchPtr);

            // Trigger the batch start if it has not been already.
            isReady();

            batchedRequest->readBatchPtr->rpc->wait();

            switch (batchedRequest->request.status) {
                case STATUS_OK:
                    version = batchedRequest->request.version;
                    data = buf->getValue(&dataLength);
                    break;
                case STATUS_OBJECT_DOESNT_EXIST:
                    objectFound = false;
                    break;
                default:
                    RAMCLOUD_LOG(ERROR,
                                 "Unexpected status '%s' while processing"
                                 "batched Transaction::ReadOp.",
                                 statusToString(
                                        batchedRequest->request.status));
                    ClientException::throwException(
                            HERE, batchedRequest->request.status);
            }
        }

        entry = task->insertCacheEntry(keyObj, data, dataLength);
        entry->type = ClientTransactionTask::CacheEntry::READ;
        if (objectFound) {
            entry->rejectRules.doesntExist = true;
            entry->rejectRules.givenVersion = version;
            entry->rejectRules.versionNeGiven = true;
        } else {
            // Object did not exists at the time of the read so remember to
            // reject (abort) the transaction if it does exist.
            entry->rejectRules.exists = true;
            objectFound = false;
        }

    } else {
        if (entry->type == ClientTransactionTask::CacheEntry::REMOVE) {
            // Read after remove; object would no longer exist.
            objectFound = false;

            if (!requestBatched) {
                optypestr = "txrdhitdne";
            } else {
                optypestr = "txbatchrdhitdne";
            }
        } else if (entry->type == ClientTransactionTask::CacheEntry::READ
                && entry->rejectRules.exists) {
            // Read after read resulting in object DNE; object still DNE.
            objectFound = false;

            if (!requestBatched) {
                optypestr = "txrdhitdne";
            } else {
                optypestr = "txbatchrdhitdne";
            }
        } else {
            if (!requestBatched) {
                optypestr = "txrdhit";
            } else {
                optypestr = "txbatchrdhit";
            }
        }
    }

    if (objectExists == NULL) {
        if (!objectFound) {
            uint64_t endTime = Cycles::rdtsc();

            uint32_t dataLength = 0;

            NANO_LOG(NOTICE, "{\"type\": \"%s_%s\", \"startTime\": %lu, \"endTime\": %lu, \"elapsedTime\": %lu, \"edgeLabel\": \"%s\", \"neighborLabel\": \"%s\", \"keyLen\": %d, \"valLen\": %d, \"totalLen\": %d}", optypestr, objtypestr, Cycles::toNanoseconds(startTime), Cycles::toNanoseconds(endTime), Cycles::toNanoseconds(endTime - startTime), edgeLabel, neighborLabel, keyLength, dataLength, keyLength + dataLength);

            throw ObjectDoesntExistException(HERE);
        }
    } else {
        *objectExists = objectFound;
        if (!objectFound) {
            uint64_t endTime = Cycles::rdtsc();

            uint32_t dataLength = 0;

            NANO_LOG(NOTICE, "{\"type\": \"%s_%s\", \"startTime\": %lu, \"endTime\": %lu, \"elapsedTime\": %lu, \"edgeLabel\": \"%s\", \"neighborLabel\": \"%s\", \"keyLen\": %d, \"valLen\": %d, \"totalLen\": %d}", optypestr, objtypestr, Cycles::toNanoseconds(startTime), Cycles::toNanoseconds(endTime), Cycles::toNanoseconds(endTime - startTime), edgeLabel, neighborLabel, keyLength, dataLength, keyLength + dataLength);

            return;
        }
    }

    uint32_t dataLength;
    const void* data = entry->objectBuf.getValue(&dataLength);
    value->reset();
    value->appendCopy(data, dataLength);

    uint64_t endTime = Cycles::rdtsc();

    NANO_LOG(NOTICE, "{\"type\": \"%s_%s\", \"startTime\": %lu, \"endTime\": %lu, \"elapsedTime\": %lu, \"edgeLabel\": \"%s\", \"neighborLabel\": \"%s\", \"keyLen\": %d, \"valLen\": %d, \"totalLen\": %d}", optypestr, objtypestr, Cycles::toNanoseconds(startTime), Cycles::toNanoseconds(endTime), Cycles::toNanoseconds(endTime - startTime), edgeLabel, neighborLabel, keyLength, dataLength, keyLength + dataLength);
}

} // namespace RAMCloud
