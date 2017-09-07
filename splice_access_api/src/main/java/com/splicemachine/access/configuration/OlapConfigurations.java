/*
 * Copyright (c) 2012 - 2018 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 */

package com.splicemachine.access.configuration;


/**
 * Repository for holding configuration keys for Olap client/server.
 * <p/>
 * Each specific architecture configuration should provide
 * a default value for each of these keys.
 */
public class OlapConfigurations implements ConfigurationDefault {

    /**
     * The number of milliseconds the OLAP client should wait for a result.
     * Defaults to Integer.MAX_VALUE (wait forever)
     */
    public static final String OLAP_CLIENT_WAIT_TIME = "splice.olap_server.clientWaitTime";
    private static final int DEFAULT_OLAP_CLIENT_WAIT_TIME = Integer.MAX_VALUE;

    /**
     * The number of milliseconds the OLAP client should wait for performing a status check.
     * Defaults to 1000 (1 s)
     */
    public static final String OLAP_CLIENT_TICK_TIME = "splice.olap_server.clientTickTime";
    private static final int DEFAULT_OLAP_CLIENT_TICK_TIME = 1000;

    /**
     * The Port to bind the OLAP Server connection to
     * Defaults to 60014
     */
    public static final String OLAP_SERVER_BIND_PORT = "splice.olap_server.port";
    private static final int DEFAULT_OLAP_SERVER_BIND_PORT = 60014;

    /**
     * The directory to use for staging files sent to the Olap Server
     * Defaults to NULL
     */
    public static final String OLAP_SERVER_STAGING_DIR = "splice.olap_server.stagingDirectory";
    private static final String DEFAULT_OLAP_SERVER_STAGING_DIR = null;

    /**
     * Run OlapServer externally on YARN
     * Defaults to true
     */
    public static final String OLAP_SERVER_EXTERNAL = "splice.olap_server.external";
    private static final boolean DEFAULT_OLAP_SERVER_EXTERNAL = true;

    /**
     * Number of threads used by the Olap server, determines the maximum number of concurrent
     * Olap jobs
     *
     * Defaults to 16
     */
    public static final String OLAP_SERVER_THREADS = "splice.olap_server.threads";
    private static final int DEFAULT_OLAP_SERVER_THREADS = 16;

    public static final String OLAP_SERVER_TICK_LIMIT = "splice.olap_server.tickLimit";
    private static final int DEFAULT_OLAP_SERVER_TICK_LIMIT = 120;

    public static final String OLAP_SERVER_SUBMIT_ATTEMPTS = "splice.olap_server.submitAttempts";
    private static final int DEFAULT_OLAP_SERVER_SUBMIT_ATTEMPTS = 50;

    public static final String OLAP_SERVER_MEMORY = "splice.olap_server.memory";
    private static final int DEFAULT_OLAP_SERVER_MEMORY = 1024;

    public static final String OLAP_SERVER_MEMORY_OVERHEAD = "splice.olap_server.memoryOverhead";
    private static final int DEFAULT_OLAP_SERVER_MEMORY_OVERHEAD = 512;

    public static final String OLAP_SERVER_VIRTUAL_CORES = "splice.olap_server.virtualCores";
    private static final int DEFAULT_OLAP_SERVER_VIRTUAL_CORES = 1;

    public static final String OLAP_CLIENT_RETRIES = "splice.olap_client.retries";
    private static final int DEFAULT_OLAP_CLIENT_RETRIES = 10;

    public static final String ACTIVE_TRANSACTION_CACHE_SIZE="splice.txn.activeCacheSize";
    private static final int DEFAULT_ACTIVE_TRANSACTION_CACHE_SIZE = 1<<12;

    @Override
    public void setDefaults(ConfigurationBuilder builder, ConfigurationSource configurationSource) {
        builder.activeTransactionCacheSize  = configurationSource.getInt(ACTIVE_TRANSACTION_CACHE_SIZE, DEFAULT_ACTIVE_TRANSACTION_CACHE_SIZE);
        builder.olapServerBindPort  = configurationSource.getInt(OLAP_SERVER_BIND_PORT, DEFAULT_OLAP_SERVER_BIND_PORT);
        builder.olapServerStagingDir = configurationSource.getString(OLAP_SERVER_STAGING_DIR, DEFAULT_OLAP_SERVER_STAGING_DIR);
        builder.olapServerExternal  = configurationSource.getBoolean(OLAP_SERVER_EXTERNAL, DEFAULT_OLAP_SERVER_EXTERNAL);
        builder.olapClientWaitTime  = configurationSource.getInt(OLAP_CLIENT_WAIT_TIME, DEFAULT_OLAP_CLIENT_WAIT_TIME);
        builder.olapClientTickTime  = configurationSource.getInt(OLAP_CLIENT_TICK_TIME, DEFAULT_OLAP_CLIENT_TICK_TIME);
        builder.olapServerThreads = configurationSource.getInt(OLAP_SERVER_THREADS, DEFAULT_OLAP_SERVER_THREADS);
        builder.olapServerTickLimit = configurationSource.getInt(OLAP_SERVER_TICK_LIMIT,DEFAULT_OLAP_SERVER_TICK_LIMIT);
        builder.olapClientRetries = configurationSource.getInt(OLAP_CLIENT_RETRIES,DEFAULT_OLAP_CLIENT_RETRIES);
        builder.olapServerSubmitAttempts = configurationSource.getInt(OLAP_SERVER_SUBMIT_ATTEMPTS, DEFAULT_OLAP_SERVER_SUBMIT_ATTEMPTS);
        builder.olapServerMemory = configurationSource.getInt(OLAP_SERVER_MEMORY, DEFAULT_OLAP_SERVER_MEMORY);
        builder.olapServerMemoryOverhead = configurationSource.getInt(OLAP_SERVER_MEMORY_OVERHEAD, DEFAULT_OLAP_SERVER_MEMORY_OVERHEAD);
        builder.olapServerVirtualCores = configurationSource.getInt(OLAP_SERVER_VIRTUAL_CORES, DEFAULT_OLAP_SERVER_VIRTUAL_CORES);
    }
}
