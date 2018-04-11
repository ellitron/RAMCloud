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

package edu.stanford.ramcloud;

import edu.stanford.ramcloud.*;
import static edu.stanford.ramcloud.ClientException.checkStatus;
import static edu.stanford.ramcloud.RAMCloud.getRejectRulesBytes;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.log4j.Logger;

/**
 * This class provides the Java bindings for the RAMCloud C++ Transaction class.
 *
 * Note: This class is not thread safe (neither is the C++ implementation)
 */
public class RAMCloudTransaction {
    static {
        Util.loadLibrary("ramcloud_java");
    } 

    private static final Logger logger = 
        Logger.getLogger(RAMCloudTransaction.class);

    /**
     * Target cluster of this transaction.
     */
    private RAMCloud ramcloud;
    
    /**
     * Pointer to the underlying C++ RAMCloud object on which this transaction
     * operates.
     */
    private long cppRamcloudObjectPointer;

    /**
     * A native ByteBuffer that acts as a shared memory region between Java and
     * C++. This enables fast passing of arguments and return values for native
     * calls.
     */
    private ByteBuffer byteBuffer;

    /**
     * C++ pointer to the shared memory location that byteBuffer wraps.
     */
    private long cppByteBufferPointer;
    
    /**
     * Pointer to the underlying C++ Transaction object associated with this
     * java object.
     */
    private long cppTransactionObjectPointer;
    
    /**
     * Constructor for a transaction. Also constructs the underlying C++ 
     * Transaction object, which we keep a pointer to for future JNI calls.
     * 
     * @param ramcloud 
     *            RAMCloud cluster on which to perform the transaction.
     */
    public RAMCloudTransaction(RAMCloud ramcloud) {
        this.ramcloud = ramcloud;
        cppRamcloudObjectPointer = ramcloud.getRamCloudClusterHandle();
        byteBuffer = ramcloud.getByteBuffer();
        cppByteBufferPointer = ramcloud.getByteBufferPointer();
        byteBuffer.rewind();
        byteBuffer.putLong(cppRamcloudObjectPointer);
        cppConstructor(cppByteBufferPointer);
        byteBuffer.rewind();
        ClientException.checkStatus(byteBuffer.getInt());
        cppTransactionObjectPointer = byteBuffer.getLong();
    }

    /**
     * Accessor method for getting a pointer to the underlying C++ RAMCloud
     * Transaction object. Used by TransactionReadOp objects which reference a
     * RAMCloud Transaction object in their C++ implementation.
     * 
     * @return Address of this RAMCloud Transaction object in memory.
     */
    public long getCppTransactionObjectPointer() {
        return cppTransactionObjectPointer;
    }

    /**
     * Accessor method for byteBuffer. Used by the TransactionReadOp class to
     * reuse RAMCloud's buffer for communicating arguments and return values
     * to/from C++.
     * 
     * @return ByteBuffer of this object.
     * 
     * @note A more elegant approach might be to create a "context" object that 
     * contains global variables for a single RAMCloud object and any objects 
     * that reference it. 
     */
    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }
    
    /**
     * Accessor method for cppByteBufferPointer. Used by the TransactionReadOp
     * class to avoid the work of figuring out the byteBuffer's address in
     * memory.
     * 
     * @return Pointer referring to the byteBuffer in memory.
     * 
     * @note A more elegant approach might be to create a "context" object that 
     * contains global variables for a single RAMCloud object and any objects 
     * that reference it. 
     */
    public long getByteBufferPointer() {
        return cppByteBufferPointer;
    }

    public void clear() {
        byteBuffer.rewind();
        byteBuffer.putLong(cppTransactionObjectPointer);
        cppDeconstructor(cppByteBufferPointer);
        byteBuffer.rewind();
        ClientException.checkStatus(byteBuffer.getInt());
        
        byteBuffer.rewind();
        byteBuffer.putLong(cppRamcloudObjectPointer);
        cppConstructor(cppByteBufferPointer);
        byteBuffer.rewind();
        ClientException.checkStatus(byteBuffer.getInt());
        cppTransactionObjectPointer = byteBuffer.getLong();
    }
    
    /**
     * Permanently closes this transaction. This method deconstructs the C++
     * transaction object, and therefore should *always* be called as the last 
     * operation for this transaction object. The transaction object cannot be 
     * used after a call to this method.
     */
    public void close() {
        if (cppTransactionObjectPointer != 0) {
            byteBuffer.rewind();
            byteBuffer.putLong(cppTransactionObjectPointer);
            cppDeconstructor(cppByteBufferPointer);
            byteBuffer.rewind();
            cppTransactionObjectPointer = 0;   
            ClientException.checkStatus(byteBuffer.getInt());
        }
    }

    /**
     * This method is called by the garbage collector before destroying the
     * object. The user really should have called close, but in case they
     * did not, be sure to clean up after them.
     */
    @Override
    public void finalize() {
        close();
    }
    
    /**
     * Commits the transaction defined by the operations performed on this
     * transaction (read, remove, write). This method blocks until a decision is
     * reached and sent to all participant servers but does not wait for the
     * participant servers to acknowledge the decision (e.g. does not wait to
     * sync).
     * 
     * @return True if the transaction was able to commit.  False otherwise.
     */
    public boolean commit() {
        byteBuffer.rewind();
        byteBuffer.putLong(cppTransactionObjectPointer);
        cppCommit(cppByteBufferPointer);
        byteBuffer.rewind();
        ClientException.checkStatus(byteBuffer.getInt());
        return (byteBuffer.getInt() == 1);
    }
    
    /**
     * Block until the decision of this transaction commit is accepted by all
     * participant servers. If the commit has not yet occurred and a decision is
     * not yet reached, this method will also start the commit.
     *
     * This method is used mostly for testing and benchmarking.
     */
    public void sync() {
        byteBuffer.rewind();
        byteBuffer.putLong(cppTransactionObjectPointer);
        cppSync(cppByteBufferPointer);
        byteBuffer.rewind();
        ClientException.checkStatus(byteBuffer.getInt());
    }
    
    /**
     * Commits the transaction defined by the operations performed on this
     * transaction (read, remove, write). This method blocks until all
     * participant servers have accepted the decision.
     *
     * @return True if the transaction was able to commit. False otherwise.
     */
    public boolean commitAndSync() {
        byteBuffer.rewind();
        byteBuffer.putLong(cppTransactionObjectPointer);
        cppCommitAndSync(cppByteBufferPointer);
        byteBuffer.rewind();
        ClientException.checkStatus(byteBuffer.getInt());
        return (byteBuffer.getInt() == 1);
    }
    
    /**
     * Read the current contents of an object as a part of this transaction.
     *
     * @see #read(long, byte[]) 
     */
    public RAMCloudObject read(long tableId, String key) {
        return read(tableId, key.getBytes());
    }
    
    /**
     * Read the current contents of an object as a part of this transaction.
     *
     * @param tableId
     *            The table containing the desired object (return value from a
     *            previous call to RAMCloud.getTableId).
     * @param key
     *            Variable length key that uniquely identifies the object within
     *            tableId. It does not necessarily have to be null terminated.
     *            The caller must ensure that the storage for this key is
     *            unchanged through the life of the RPC.
     * @return A RAMCloudObject holding the key and value of the read object 
     *          (no version information, defaults to 0), or null if the object
     *          does not exist.
     */
    public RAMCloudObject read(long tableId, byte[] key) {
        byteBuffer.rewind();
        byteBuffer.putLong(cppTransactionObjectPointer)
                .putLong(tableId)
                .putInt(key.length)
                .put(key);
        
        cppRead(cppByteBufferPointer);

        byteBuffer.rewind();
        ClientException.checkStatus(byteBuffer.getInt());

        boolean exists = (byteBuffer.getInt() == 1);

        if (!exists) {
          return null;
        }

        int valueLength = byteBuffer.getInt();
        byte[] value = new byte[valueLength];
        byteBuffer.get(value);

        return new RAMCloudObject(key, value, 0);
    }
    
    /**
     * Delete an object from a table as part of this transaction. If the object
     * does not currently exist then the operation succeeds without doing
     * anything.
     *
     * @see #remove(long, byte[]) 
     */
    public void remove(long tableId, String key) {
        remove(tableId, key.getBytes());
    }
    
    /**
     * Delete an object from a table as part of this transaction. If the object
     * does not currently exist then the operation succeeds without doing
     * anything.
     *
     * @param tableId
     *            The table containing the object to be deleted (return value
     *            from a previous call to RAMCloud.getTableId).
     * @param key
     *            Variable length key that uniquely identifies the object within
     *            tableId.
     */
    public void remove(long tableId, byte[] key) {
        byteBuffer.rewind();
        byteBuffer.putLong(cppTransactionObjectPointer)
                .putLong(tableId)
                .putInt(key.length)
                .put(key);
        cppRemove(cppByteBufferPointer);
        byteBuffer.rewind();
        ClientException.checkStatus(byteBuffer.getInt());
    }
    
    /**
     * Replace the value of a given object, or create a new object if none
     * previously existed as part of this transaction.
     *
     * @see #write(long, byte[], byte[])
     */
    public void write(long tableId, String key, String value) {
        write(tableId, key.getBytes(), value.getBytes());
    }
    
    /**
     * Replace the value of a given object, or create a new object if none
     * previously existed as part of this transaction.
     *
     * @see #write(long, byte[], byte[])
     */
    public void write(long tableId, String key, byte[] value) {
        write(tableId, key.getBytes(), value);
    }
    
    /**
     * Replace the value of a given object, or create a new object if none
     * previously existed as part of this transaction.
     *
     * @param tableId
     *            The table containing the desired object (return value from a
     *            previous call to RAMCloud.getTableId).
     * @param key
     *            Variable length key that uniquely identifies the object within
     *            tableId.
     * @param value
     *            String providing the new value for the object.
     */
    public void write(long tableId, byte[] key, byte[] value) {
        byteBuffer.rewind();
        byteBuffer.putLong(cppTransactionObjectPointer)
                .putLong(tableId)
                .putInt(key.length)
                .put(key)
                .putInt(value.length)
                .put(value);

        cppWrite(cppByteBufferPointer);

        byteBuffer.rewind();
        ClientException.checkStatus(byteBuffer.getInt());
    }
    
    // Documentation for native methods located in C++ files
    protected native void cppConstructor(long cppByteBufferPointer);
    protected native void cppDeconstructor(long cppByteBufferPointer);
    protected native void cppCommit(long cppByteBufferPointer);
    protected native void cppSync(long cppByteBufferPointer);
    protected native void cppCommitAndSync(long cppByteBufferPointer);
    protected native void cppRead(long cppByteBufferPointer);
    protected native void cppRemove(long cppByteBufferPointer);
    protected native void cppWrite(long cppByteBufferPointer);
}
