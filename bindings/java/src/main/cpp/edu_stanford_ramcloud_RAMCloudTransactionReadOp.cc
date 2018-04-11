/* Copyright (c) 2014 Stanford University
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

#include <RamCloud.h>

#include <Util.h>
#include "edu_stanford_ramcloud_RAMCloudTransactionReadOp.h"
#include "JavaCommon.h"
#include "Transaction.h"

using namespace RAMCloud;

/**
 * Constructs a RAMCloudTransactionReadOp object.
 *
 * \param env
 *      The current JNI environment.
 * \param javaRAMCloudTransactionReadOpObject
 *      The calling object.
 * \param byteBufferPointer
 *      A pointer to the ByteBuffer through which Java and C++ communicate.
 */
JNIEXPORT void 
JNICALL Java_edu_stanford_ramcloud_RAMCloudTransactionReadOp_cppConstructor(
    JNIEnv *env, 
    jobject javaRAMCloudTransactionReadOpObject, 
    jlong byteBufferPointer) {
    ByteBuffer buffer(byteBufferPointer);

    Transaction* transaction = buffer.readPointer<Transaction>();
    uint64_t tableId = buffer.read<uint64_t>();
    uint32_t keyLength = buffer.read<uint32_t>();
    void* key = buffer.getVoidPointer(keyLength);
    uint32_t batchInt = buffer.read<uint32_t>();

    bool batch;
    if (batchInt == 1) {
      batch = true;
    } else {
      batch = false;
    }

    buffer.rewind();

    Buffer* value = NULL;
    Transaction::ReadOp* readOp = NULL;
    try {
        value = new Buffer();
        readOp = new Transaction::ReadOp(transaction, tableId, key, 
            keyLength, value, batch);
    } EXCEPTION_CATCHER(buffer);

    buffer.write(reinterpret_cast<uint64_t>(readOp));
    buffer.write(reinterpret_cast<uint64_t>(value));
}

/**
 * Deconstruct a RAMCloudTransaction object.
 *
 * \param env
 *      The current JNI environment.
 * \param javaRAMCloudTransactionReadOpObject
 *      The calling object.
 * \param byteBufferPointer
 *      A pointer to the ByteBuffer through which Java and C++ communicate.
 */
JNIEXPORT void 
JNICALL Java_edu_stanford_ramcloud_RAMCloudTransactionReadOp_cppDeconstructor(
    JNIEnv *env, 
    jobject javaRAMCloudTransactionReadOpObject, 
    jlong byteBufferPointer) {
    ByteBuffer buffer(byteBufferPointer);

    Transaction::ReadOp* readOp = 
        buffer.readPointer<Transaction::ReadOp>();
    Buffer* value = buffer.readPointer<Buffer>();

    buffer.rewind();

    try {
        delete readOp;
        delete value;
    } EXCEPTION_CATCHER(buffer); 
}

/**
 * Calls ReadOp.isReady()
 *
 * \param env
 *      The current JNI environment.
 * \param javaRAMCloudTransactionReadOpObject
 *      The calling object.
 * \param byteBufferPointer
 *      A pointer to the ByteBuffer through which Java and C++ communicate.
 */
JNIEXPORT void 
JNICALL Java_edu_stanford_ramcloud_RAMCloudTransactionReadOp_cppIsReady(
    JNIEnv *env, 
    jobject javaRAMCloudTransactionReadOpObject, 
    jlong byteBufferPointer) {
    ByteBuffer buffer(byteBufferPointer);

    Transaction::ReadOp* readOp = 
        buffer.readPointer<Transaction::ReadOp>();

    buffer.rewind();

    bool ready;
    try {
        ready = readOp->isReady();
    } EXCEPTION_CATCHER(buffer);

    if(ready == true)
        buffer.write<uint32_t>(1);
    else
        buffer.write<uint32_t>(0);
}

/**
 * Calls ReadOp.wait(), and returns the read value.
 *
 * \param env
 *      The current JNI environment.
 * \param javaRAMCloudTransactionReadOpObject
 *      The calling object.
 * \param byteBufferPointer
 *      A pointer to the ByteBuffer through which Java and C++ communicate.
 */
JNIEXPORT void 
JNICALL Java_edu_stanford_ramcloud_RAMCloudTransactionReadOp_cppWait(
    JNIEnv *env, 
    jobject javaRAMCloudTransactionReadOpObject, 
    jlong byteBufferPointer) {
    ByteBuffer buffer(byteBufferPointer);

    Transaction::ReadOp* readOp = 
        buffer.readPointer<Transaction::ReadOp>();
    Buffer* value = buffer.readPointer<Buffer>();

    buffer.rewind();

    bool exists;
    try {
        readOp->wait(&exists);
    } EXCEPTION_CATCHER(buffer);

    if(exists == true)
        buffer.write<uint32_t>(1);
    else
        buffer.write<uint32_t>(0);

    buffer.write(value->size());
    value->copy(0, value->size(), buffer.getVoidPointer());
}
