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
#include "edu_stanford_ramcloud_RAMCloudTransaction.h"
#include "JavaCommon.h"
#include "Transaction.h"

using namespace RAMCloud;

/**
 * Constructs a RAMCloudTransaction object.
 *
 * \param env
 *      The current JNI environment.
 * \param transaction
 *      The calling object.
 * \param byteBufferPointer
 *      A pointer to the ByteBuffer through which Java and C++ communicate.
 *      The format for the input buffer is:
 *          8 bytes for a pointer to a C++ RamCloud object
 *      The format for the output buffer is:
 *          4 bytes for the status code of the transaction constructor
 */
JNIEXPORT void 
JNICALL Java_edu_stanford_ramcloud_RAMCloudTransaction_cppConstructor(
        JNIEnv *env, 
        jobject javaRAMCloudTransactionObject, 
        jlong byteBufferPointer) {
    ByteBuffer buffer(byteBufferPointer);
    RamCloud* ramcloud = buffer.readPointer<RamCloud>();
    buffer.rewind();
    Transaction* transaction = NULL;
    try {
        transaction = new Transaction(ramcloud);
    } EXCEPTION_CATCHER(buffer);
    buffer.write(reinterpret_cast<uint64_t>(transaction));
}

/**
 * Deconstruct a RAMCloudTransaction object.
 *
 * \param env
 *      The current JNI environment.
 * \param transaction
 *      The calling object.
 * \param byteBufferPointer
 *      A pointer to the ByteBuffer through which Java and C++ communicate.
 *      The format for the input buffer is:
 *          8 bytes for a pointer to a C++ Transaction object
 *      The format for the output buffer is:
 *          4 bytes for the status code of the commit operation
 */
JNIEXPORT void 
JNICALL Java_edu_stanford_ramcloud_RAMCloudTransaction_cppDeconstructor(
        JNIEnv *env, 
        jobject javaRAMCloudTransactionObject, 
        jlong byteBufferPointer) {
    ByteBuffer buffer(byteBufferPointer);
    Transaction* transaction = buffer.readPointer<Transaction>();
    buffer.rewind();
    try {
        delete transaction;
    } EXCEPTION_CATCHER(buffer); 
}

/**
 * Commits the transaction defined by the operations performed on this
 * transaction (read, remove, write).  This method blocks until a decision is
 * reached and sent to all participant servers but does not wait on the
 * participant servers to acknowledge the decision (e.g. does not wait to sync).
 *
 * \param env
 *      The current JNI environment.
 * \param transaction
 *      The calling object.
 * \param byteBufferPointer
 *      A pointer to the ByteBuffer through which Java and C++ communicate.
 *      The format for the input buffer is:
 *          8 bytes for a pointer to a C++ Transaction object
 *      The format for the output buffer is:
 *          4 bytes for the status code of the commit operation
 *          4 bytes for a boolean for whether or not the commit succeeded
 */
JNIEXPORT void 
JNICALL Java_edu_stanford_ramcloud_RAMCloudTransaction_cppCommit(
        JNIEnv *env, 
        jobject javaRAMCloudTransactionObject, 
        jlong byteBufferPointer) {
    ByteBuffer buffer(byteBufferPointer);
    Transaction* transaction = buffer.readPointer<Transaction>();
    buffer.rewind();
    bool result;
    try {
        result = transaction->commit();
    } EXCEPTION_CATCHER(buffer);
    
    if(result == true)
        buffer.write<uint32_t>(1);
    else
        buffer.write<uint32_t>(0);
}

/**
 * Block until the decision of this transaction commit is accepted by all
 * participant servers.  If the commit has not yet occurred and a decision is
 * not yet reached, this method will also start the commit.
 *
 * \param env
 *      The current JNI environment.
 * \param transaction
 *      The calling object.
 * \param byteBufferPointer
 *      A pointer to the ByteBuffer through which Java and C++ communicate.
 *      The format for the input buffer is:
 *          8 bytes for a pointer to a C++ Transaction object
 *      The format for the output buffer is:
 *          4 bytes for the status code of the sync operation
 */
JNIEXPORT void 
JNICALL Java_edu_stanford_ramcloud_RAMCloudTransaction_cppSync(
        JNIEnv *env, 
        jobject javaRAMCloudTransactionObject, 
        jlong byteBufferPointer) {
    ByteBuffer buffer(byteBufferPointer);
    Transaction* transaction = buffer.readPointer<Transaction>();
    buffer.rewind();
    try {
        transaction->sync();
    } EXCEPTION_CATCHER(buffer);
}

/**
 * Commits the transaction defined by the operations performed on this
 * transaction (read, remove, write).  This method blocks until a participant
 * servers have accepted the decision.
 *
 * \param env
 *      The current JNI environment.
 * \param transaction
 *      The calling object.
 * \param byteBufferPointer
 *      A pointer to the ByteBuffer through which Java and C++ communicate.
 *      The format for the input buffer is:
 *          8 bytes for a pointer to a C++ Transaction object
 *      The format for the output buffer is:
 *          4 bytes for the status code of the CommitAndSync operation
 *          4 bytes for a boolean for whether or not the commit succeeded
 */
JNIEXPORT void 
JNICALL Java_edu_stanford_ramcloud_RAMCloudTransaction_cppCommitAndSync(
        JNIEnv *env, 
        jobject javaRAMCloudTransactionObject, 
        jlong byteBufferPointer) {
    ByteBuffer buffer(byteBufferPointer);
    Transaction* transaction = buffer.readPointer<Transaction>();
    buffer.rewind();
    bool result;
    try {
        result = transaction->commitAndSync();
    } EXCEPTION_CATCHER(buffer);
    
    if(result == true)
        buffer.write<uint32_t>(1);
    else
        buffer.write<uint32_t>(0);
}

/**
 * Read the current contents of an object as part of this transaction.
 *
 * \param env
 *      The current JNI environment.
 * \param transaction
 *      The calling object.
 * \param byteBufferPointer
 *      A pointer to the ByteBuffer through which Java and C++ communicate.
 *      The format for the input buffer is:
 *          8 bytes for a pointer to a C++ Transaction object
 *          8 bytes for the ID of the table to read from
 *          4 bytes for the length of the key to find
 *          byte array for the key to find
 *      The format for the output buffer is:
 *          4 bytes for the status code of the read operation
 *          4 bytes for the size of the read value
 *          byte array for the read value
 */
JNIEXPORT void 
JNICALL Java_edu_stanford_ramcloud_RAMCloudTransaction_cppRead(
        JNIEnv *env, 
        jobject javaRAMCloudTransactionObject, 
        jlong byteBufferPointer) {
    ByteBuffer buffer(byteBufferPointer);
    Transaction* transaction = buffer.readPointer<Transaction>();
    uint64_t tableId = buffer.read<uint64_t>();
    uint32_t keyLength = buffer.read<uint32_t>();
    void* key = buffer.getVoidPointer(keyLength);
    Buffer value;
    buffer.rewind();

    bool exists;
    try {
        transaction->read(tableId, key, keyLength, &value, &exists);
    } EXCEPTION_CATCHER(buffer);

    if(exists == true)
        buffer.write<uint32_t>(1);
    else
        buffer.write<uint32_t>(0);

    buffer.write(value.size());
    value.copy(0, value.size(), buffer.getVoidPointer());
}

/**
 * Delete an object from a table as part of this transaction. If the object does
 * not currently exist then the operation succeeds without doing anything.
 *
 * \param env
 *      The current JNI environment.
 * \param transaction
 *      The calling object.
 * \param byteBufferPointer
 *      A pointer to the ByteBuffer through which Java and C++ communicate.
 *      The format for the input buffer is:
 *          8 bytes for a pointer to a C++ Transaction object
 *          8 bytes for the ID of the table to delete from
 *          4 bytes for the length of the key to remove
 *          byte array for the key to remove
 *      The format for the output buffer is:
 *          4 bytes for the status code of the remove operation
 */
JNIEXPORT void 
JNICALL Java_edu_stanford_ramcloud_RAMCloudTransaction_cppRemove(
        JNIEnv *env, 
        jobject javaRAMCloudTransactionObject, 
        jlong byteBufferPointer) {
    ByteBuffer buffer(byteBufferPointer);
    Transaction* transaction = buffer.readPointer<Transaction>();
    uint64_t tableId = buffer.read<uint64_t>();
    uint32_t keyLength = buffer.read<uint32_t>();
    void* key = buffer.getVoidPointer(keyLength);
    buffer.rewind();
    try {
        transaction->remove(tableId, key, keyLength);
    } EXCEPTION_CATCHER(buffer);
}

/**
 * Replace the value of a given object, or create a new object if none
 * previously existed as part of this transaction.
 *
 * \param env
 *      The current JNI environment.
 * \param transaction
 *      The calling object.
 * \param byteBufferPointer
 *      A pointer to the ByteBuffer through which Java and C++ communicate.
 *      The format for the input buffer is:
 *          8 bytes for a pointer to a C++ Transaction object
 *          8 bytes for the ID of the table to write to
 *          4 bytes for the length of the key to write
 *          byte array for the key to write
 *          4 bytes for the length of the value to write
 *          byte array for the value to write
 *      The format for the output buffer is:
 *          4 bytes for the status code of the write operation
 */
JNIEXPORT void 
JNICALL Java_edu_stanford_ramcloud_RAMCloudTransaction_cppWrite(
        JNIEnv *env, 
        jobject javaRAMCloudTransactionObject, 
        jlong byteBufferPointer) {
    ByteBuffer buffer(byteBufferPointer);
    Transaction* transaction = buffer.readPointer<Transaction>();
    uint64_t tableId = buffer.read<uint64_t>();
    uint32_t keyLength = buffer.read<uint32_t>();
    void* key = buffer.getVoidPointer(keyLength);
    uint32_t valueLength = buffer.read<uint32_t>();
    void* value = buffer.getVoidPointer(valueLength);
    buffer.rewind();
    try {
        transaction->write(tableId, key, keyLength, value, valueLength);
    } EXCEPTION_CATCHER(buffer);
}

