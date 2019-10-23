package com.splicemachine.hbase;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.configuration.ConfigurationSource;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.si.data.hbase.coprocessor.HBaseSIEnvironment;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.timestamp.api.TimestampSource;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * Created by jyuan on 9/27/19.
 */
public class ReplicationUtils {

    private static final Logger LOG = Logger.getLogger(ReplicationUtils.class);

    /**
     * Bump up timestamp if the provided timestamp value is larger than current timetamp
     * @param timestamp
     * @throws IOException
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void setTimestamp(long timestamp) throws IOException, KeeperException, InterruptedException {
        TimestampSource timestampSource = SIDriver.driver().getTimestampSource();
        long currentTimestamp = timestampSource.currentTimestamp();
        if (currentTimestamp < timestamp) {
            RecoverableZooKeeper rzk = ZkUtils.getRecoverableZooKeeper();
            HBaseSIEnvironment env = HBaseSIEnvironment.loadEnvironment(new SystemClock(), rzk);
            ConfigurationSource configurationSource = env.configuration().getConfigSource();
            String rootNode = configurationSource.getString(HConfiguration.SPLICE_ROOT_PATH, HConfiguration.DEFAULT_ROOT_PATH);
            String node = rootNode + HConfiguration.MAX_RESERVED_TIMESTAMP_PATH;
            //if (LOG.isDebugEnabled()) {
            SpliceLogUtils.info(LOG, "bump up timestamp to %d", timestamp);
            //}
            byte[] data = Bytes.toBytes(timestamp);
            rzk.setData(node, data, -1 /* version */);
            timestampSource.refresh();
        }
        else {
            //if (LOG.isDebugEnabled()) {
            SpliceLogUtils.info(LOG, "current timestamp = %d >  %d",
                    currentTimestamp, timestamp);
            //}
        }
    }
}
