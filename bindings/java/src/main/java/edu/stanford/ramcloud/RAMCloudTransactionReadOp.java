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
 * This class provides the Java bindings for the RAMCloud C++
 * Transaction::ReadOp class.
 *
 * Note: This class is not thread safe (neither is the C++ implementation)
 */
public class RAMCloudTransactionReadOp {
    static {
        Util.loadLibrary("ramcloud_java");
    } 

    private static final Logger logger = 
        Logger.getLogger(RAMCloudTransactionReadOp.class);

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
     * Pointer to the underlying C++ Transaction::ReadOp object associated with
     * this java object.
     */
    private long cppTransactionReadOpObjectPointer;
    
    /**
     * Pointer to the underlying C++ Buffer object that holds the return value
     * of the ReadOp.
     */
    private long cppReturnBufferObjectPointer;
    
    /**
     * Key that this ReadOp is reading.
     */
    private byte[] key;

    /**
     * Constructor for a transaction ReadOp. Also constructs the underlying C++
     * Transaction::ReadOp object.
     * 
     * @param ramcloudTx RAMCloud Transaction in which to perform the ReadOp.
     * @param tableId RAMCloud tableId.
     * @param key RAMCloud key to read in table.
     * @param batch Whether or not to batch the ReadOp.
     */
    public RAMCloudTransactionReadOp(RAMCloudTransaction ramcloudTx, 
        long tableId, byte[] key, boolean batch) {
        this.key = key;
        byteBuffer = ramcloudTx.getByteBuffer();
        cppByteBufferPointer = ramcloudTx.getByteBufferPointer();

        long cppTransactionObjectPointer = 
            ramcloudTx.getCppTransactionObjectPointer();

        int batchInt;
        if (batch) {
          batchInt = 1;
        } else {
          batchInt = 0;
        }

        byteBuffer.rewind();
        byteBuffer.putLong(cppTransactionObjectPointer)
            .putLong(tableId)
            .putInt(key.length)
            .put(key)
            .putInt(batchInt);

        cppConstructor(cppByteBufferPointer);

        byteBuffer.rewind();
        ClientException.checkStatus(byteBuffer.getInt());

        cppTransactionReadOpObjectPointer = byteBuffer.getLong();
        cppReturnBufferObjectPointer = byteBuffer.getLong();
    }

    public RAMCloudTransactionReadOp(RAMCloudTransaction ramcloudTx, 
        long tableId, String key, boolean batch) {
      this(ramcloudTx, tableId, key.getBytes(), batch);
    }

    /**
     * This method is called by the garbage collector before destroying the
     * object. 
     */
    @Override
    public void finalize() {
        if (cppTransactionReadOpObjectPointer != 0) {
            byteBuffer.rewind();
            byteBuffer.putLong(cppTransactionReadOpObjectPointer);
            byteBuffer.putLong(cppReturnBufferObjectPointer);

            cppDeconstructor(cppByteBufferPointer);

            byteBuffer.rewind();

            cppTransactionReadOpObjectPointer = 0;   
            
            ClientException.checkStatus(byteBuffer.getInt());
        }
    }

    /**
     * Non-blocking call to check if the ReadOp is finished. If this method
     * returns true, then getValue() will return immediately. Otherwise
     * getValue() may block. 
     *
     * @return True if getValue() is guaranteed to return immediately, false
     * otherwise. 
     */
    public boolean isReady() {
        byteBuffer.rewind();
        byteBuffer.putLong(cppTransactionReadOpObjectPointer);

        cppIsReady(cppByteBufferPointer);
        
        byteBuffer.rewind();
        ClientException.checkStatus(byteBuffer.getInt());
        return (byteBuffer.getInt() == 1);
    }

    /**
     * Blocking call to return the RAMCloud object read by this ReadOp.
     *
     * @return RAMCloud object returned by this ReadOp, or null if the object
     *          does not exist.
     */
    public RAMCloudObject getValue() {
        byteBuffer.rewind();
        byteBuffer.putLong(cppTransactionReadOpObjectPointer);
        byteBuffer.putLong(cppReturnBufferObjectPointer);
        
        cppWait(cppByteBufferPointer);

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
    
    // Documentation for native methods located in C++ files
    protected native void cppConstructor(long cppByteBufferPointer);
    protected native void cppDeconstructor(long cppByteBufferPointer);
    protected native void cppIsReady(long cppByteBufferPointer);
    protected native void cppWait(long cppByteBufferPointer);
}
