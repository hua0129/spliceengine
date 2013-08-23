package com.splicemachine.hbase.batch;

import com.splicemachine.si.api.RollForwardQueue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * @author Scott Fines
 * Created on: 4/30/13
 */
public interface WriteContextFactory<T> {

    WriteContext create(String txnId,T key) throws IOException, InterruptedException;

    WriteContext create(String txnId,T key,RollForwardQueue<byte[],ByteBuffer> queue) throws IOException, InterruptedException;

    /**
     * Creates a context that only updates side effects.
     * @param key
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    WriteContext createPassThrough(String txnid,T key) throws IOException,InterruptedException;

    void dropIndex(long indexConglomId);

    void addIndex(long indexConglomId, BitSet indexedColumns, int[] mainColToIndexPosMap, boolean unique,BitSet descColumns);
}
