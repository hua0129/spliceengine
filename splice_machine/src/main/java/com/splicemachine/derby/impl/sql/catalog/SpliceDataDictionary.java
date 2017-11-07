/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
 */

package com.splicemachine.derby.impl.sql.catalog;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.splicemachine.client.SpliceClient;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.catalog.*;
import com.splicemachine.derby.lifecycle.EngineLifecycleService;
import com.splicemachine.management.Manager;
import org.apache.log4j.Logger;
import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ScanQualifier;
import com.splicemachine.db.iapi.store.access.AccessFactory;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.impl.sql.execute.IndexColumnOrder;
import com.splicemachine.derby.ddl.DDLDriver;
import com.splicemachine.derby.ddl.DDLWatcher;
import com.splicemachine.derby.impl.sql.catalog.upgrade.SpliceCatalogUpgradeScripts;
import com.splicemachine.derby.impl.sql.depend.SpliceDependencyManager;
import com.splicemachine.derby.impl.sql.execute.sequence.SequenceKey;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.impl.store.access.*;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.tools.version.ManifestReader;
import com.splicemachine.utils.SpliceLogUtils;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public class SpliceDataDictionary extends DataDictionaryImpl{

    protected static final Logger LOG=Logger.getLogger(SpliceDataDictionary.class);
    private volatile TabInfoImpl pkTable=null;
    private volatile TabInfoImpl backupTable=null;
    private volatile TabInfoImpl backupItemsTable=null;
    private volatile TabInfoImpl backupStatesTable=null;
    private volatile TabInfoImpl backupJobsTable=null;
    private volatile TabInfoImpl tableStatsTable=null;
    private volatile TabInfoImpl columnStatsTable=null;
    private volatile TabInfoImpl physicalStatsTable=null;
    private volatile TabInfoImpl snapshotTable = null;
    private Splice_DD_Version spliceSoftwareVersion;

    public static final String SPLICE_DATA_DICTIONARY_VERSION="SpliceDataDictionaryVersion";
    private ConcurrentLinkedHashMap<String, byte[]> sequenceRowLocationBytesMap=null;
    private ConcurrentLinkedHashMap<String, SequenceDescriptor[]> sequenceDescriptorMap=null;

    @Override
    public SystemProcedureGenerator getSystemProcedures(){
        return new SpliceSystemProcedures(this);
    }

    @Override
    public SubKeyConstraintDescriptor getSubKeyConstraint(UUID constraintId,
                                                          int type) throws StandardException{
        if(type==DataDictionary.PRIMARYKEY_CONSTRAINT){
            DataValueDescriptor constraintIDOrderable=getIDValueAsCHAR(constraintId);

            TabInfoImpl ti=getPkTable();
            faultInTabInfo(ti);

            ScanQualifier[][] scanQualifiers=exFactory.getScanQualifier(1);
            scanQualifiers[0][0].setQualifier(
                    SYSPRIMARYKEYSRowFactory.SYSPRIMARYKEYS_CONSTRAINTID-1,
                    constraintIDOrderable,
                    Orderable.ORDER_OP_EQUALS,
                    false,false,false);
            return (SubKeyConstraintDescriptor)getDescriptorViaHeap(
                    null,scanQualifiers,ti,null,null);
        }
        /*If it's a foreign key or unique constraint, then just do the derby default*/
        return super.getSubKeyConstraint(constraintId,type);
    }


    @Override
    protected void addSubKeyConstraint(KeyConstraintDescriptor descriptor,
                                       TransactionController tc) throws StandardException{
        ExecRow row;
        TabInfoImpl ti;

        /*
         * Foreign keys get a row in SYSFOREIGNKEYS, and all others get a row in SYSKEYS.
         */
        if(descriptor.getConstraintType()==DataDictionary.FOREIGNKEY_CONSTRAINT){
            if(SanityManager.DEBUG){
                if(!(descriptor instanceof ForeignKeyConstraintDescriptor)){
                    SanityManager.THROWASSERT("descriptor not an fk descriptor, is "+descriptor.getClass().getName());
                }
            }
            @SuppressWarnings("ConstantConditions")
            ForeignKeyConstraintDescriptor fkDescriptor=(ForeignKeyConstraintDescriptor)descriptor;

            ti=getNonCoreTI(SYSFOREIGNKEYS_CATALOG_NUM);
            SYSFOREIGNKEYSRowFactory fkkeysRF=(SYSFOREIGNKEYSRowFactory)ti.getCatalogRowFactory();

            row=fkkeysRF.makeRow(fkDescriptor,null);

            /*
             * Now we need to bump the reference count of the constraint that this FK references
             */
            ReferencedKeyConstraintDescriptor refDescriptor=fkDescriptor.getReferencedConstraint();
            refDescriptor.incrementReferenceCount();
            int[] colsToSet=new int[1];
            colsToSet[0]=SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_REFERENCECOUNT;

            /* Have to update the reference count in a nested transaction here because the SYSCONSTRAINTS row we are
             * updating (a primary key constraint or unique index constraint) may have been created in the same
             * statement as the FK (create table for self referencing FK, for example). In that case the KeyValue for
             * that constraint row will have the same rowKey AND timestamp. Updating here with the same ts would REPLACE
             * the entire row with just the updated reference count column, corrupting the row (DB-3345). */
            TransactionController transactionController=tc.startNestedUserTransaction(false,true);
            try{
                updateConstraintDescriptor(refDescriptor,refDescriptor.getUUID(),colsToSet,transactionController);
            }finally{
                transactionController.commit();
                transactionController.destroy();
            }

        }else if(descriptor.getConstraintType()==DataDictionary.PRIMARYKEY_CONSTRAINT){
            ti=getNonCoreTI(SYSPRIMARYKEYS_CATALOG_NUM);
            SYSPRIMARYKEYSRowFactory pkRF=(SYSPRIMARYKEYSRowFactory)ti.getCatalogRowFactory();

            row=pkRF.makeRow(descriptor,null);
        }else{
            ti=getNonCoreTI(SYSKEYS_CATALOG_NUM);
            SYSKEYSRowFactory keysRF=(SYSKEYSRowFactory)ti.getCatalogRowFactory();

            // build the row to be stuffed into SYSKEYS
            row=keysRF.makeRow(descriptor,null);
        }

        // insert row into catalog and all its indices
        ti.insertRow(row,tc);
    }

    public void createSnapshotTable(TransactionController tc) throws StandardException {
        SchemaDescriptor systemSchema=getSystemSchemaDescriptor();
        TabInfoImpl snapshotTableInfo=getSnapshotTable();
        addTableIfAbsent(tc,systemSchema,snapshotTableInfo,null);
    }

    private TabInfoImpl getSnapshotTable() throws StandardException{
        if(snapshotTable==null){
            snapshotTable=new TabInfoImpl(new SYSSNAPSHOTSRowFactory(uuidFactory,exFactory,dvf));
        }
        initSystemIndexVariables(snapshotTable);
        return snapshotTable;
    }

    private void dropDictionaryTable(TransactionController tc, TabInfoImpl tableInfo, String viewName) throws StandardException {
        SchemaDescriptor systemSchema=getSystemSchemaDescriptor();
        DataDictionaryCache cache = getDataDictionaryCache();
        if (viewName != null) {
            TableDescriptor view = getTableDescriptor(viewName, systemSchema, tc);
            if (view != null) {
                dropAllColumnDescriptors(view.getUUID(), tc);
                dropViewDescriptor(getViewDescriptor(view), tc);
                dropTableDescriptor(view, systemSchema, tc);
            }
        }
        TableDescriptor td = getTableDescriptor(tableInfo.getTableName(), systemSchema, tc);
        dropAllColumnDescriptors(td.getUUID(), tc);
        dropTableDescriptor(td, systemSchema, tc);
        dropAllConglomerateDescriptors(td, tc);

        TableKey tableKey = new TableKey(td.getSchemaDescriptor().getUUID(), td.getName());
        cache.nameTdCacheRemove(tableKey);
        cache.oidTdCacheRemove(td.getUUID());

        // Remove Conglomerate Level and Statistics Caching..
        for (ConglomerateDescriptor cd: td.getConglomerateDescriptorList()) {
            cache.partitionStatisticsCacheRemove(cd.getConglomerateNumber());
            cache.conglomerateCacheRemove(cd.getConglomerateNumber());
        }
    }

    public void dropStatisticsTables(TransactionController tc) throws StandardException{
        //sys_table_statistics
        dropDictionaryTable(tc, getTableStatisticsTable(), "SYSTABLESTATISTICS");

        //sys_column_statistics
        dropDictionaryTable(tc, getColumnStatisticsTable(), "SYSCOLUMNSTATISTICS");

        //sys_physical_statistics
        dropDictionaryTable(tc, getPhysicalStatisticsTable(), null);
    }

    public void createStatisticsTables(TransactionController tc) throws StandardException{
        SchemaDescriptor systemSchema=getSystemSchemaDescriptor();

        //sys_table_statistics
        TabInfoImpl tableStatsInfo=getTableStatisticsTable();
        addTableIfAbsent(tc,systemSchema,tableStatsInfo,null);

        createSysTableStatsView(tc);

        //sys_column_statistics
        TabInfoImpl columnStatsInfo=getColumnStatisticsTable();
        addTableIfAbsent(tc,systemSchema,columnStatsInfo,null);

        createSysColumnStatsView(tc);

        //sys_physical_statistics
        TabInfoImpl physicalStatsInfo=getPhysicalStatisticsTable();
        addTableIfAbsent(tc,systemSchema,physicalStatsInfo,null);
    }

    public void createSchemasPermsTables(TransactionController tc) throws StandardException{
        SchemaDescriptor systemSchema=getSystemSchemaDescriptor();
        TabInfoImpl schemaPerms=new TabInfoImpl(new SYSSCHEMAPERMSRowFactory(uuidFactory,exFactory,dvf));
        addTableIfAbsent(tc,systemSchema,schemaPerms,null);
    }

    private TabInfoImpl getBackupTable() throws StandardException{
        if(backupTable==null){
            backupTable=new TabInfoImpl(new SYSBACKUPRowFactory(uuidFactory,exFactory,dvf));
        }
        initSystemIndexVariables(backupTable);
        return backupTable;
    }

    private TabInfoImpl getBackupItemsTable() throws StandardException{
        if(backupItemsTable==null){
            backupItemsTable=new TabInfoImpl(new SYSBACKUPITEMSRowFactory(uuidFactory,exFactory,dvf));
        }
        initSystemIndexVariables(backupItemsTable);
        return backupItemsTable;
    }

    private TabInfoImpl getBackupStatesTable() throws StandardException{
        if(backupStatesTable==null){
            backupStatesTable=new TabInfoImpl(new SYSBACKUPFILESETRowFactory(uuidFactory,exFactory,dvf));
        }
        initSystemIndexVariables(backupStatesTable);
        return backupStatesTable;
    }

    private TabInfoImpl getBackupJobsTable() throws StandardException{
        if(backupJobsTable==null){
            backupJobsTable=new TabInfoImpl(new SYSBACKUPJOBSRowFactory(uuidFactory,exFactory,dvf));
        }
        initSystemIndexVariables(backupJobsTable);
        return backupJobsTable;
    }

    public void createLassenTables(TransactionController tc) throws StandardException{
        SchemaDescriptor systemSchemaDescriptor=getSystemSchemaDescriptor();

        // Create BACKUP table
        TabInfoImpl backupTabInfo=getBackupTable();
        if(getTableDescriptor(backupTabInfo.getTableName(),systemSchemaDescriptor,tc)==null){
            if(LOG.isTraceEnabled()){
                LOG.trace(String.format("Creating system table %s.%s",
                        systemSchemaDescriptor.getSchemaName(),backupTabInfo.getTableName()));
            }
            makeCatalog(backupTabInfo,systemSchemaDescriptor,tc);
        }else{
            if(LOG.isTraceEnabled()){
                LOG.trace(String.format("Skipping table creation since system table %s.%s already exists.",
                        systemSchemaDescriptor.getSchemaName(),backupTabInfo.getTableName()));
            }
        }

        // Create BACKUPITEMS
        TabInfoImpl backupItemsTabInfo=getBackupItemsTable();
        if(getTableDescriptor(backupItemsTabInfo.getTableName(),systemSchemaDescriptor,tc)==null){
            if(LOG.isTraceEnabled()){
                LOG.trace(String.format("Creating system table %s.%s",systemSchemaDescriptor.getSchemaName(),
                        backupItemsTabInfo.getTableName()));
            }
            makeCatalog(backupItemsTabInfo,systemSchemaDescriptor,tc);
        }else{
            if(LOG.isTraceEnabled()){
                LOG.trace(String.format("Skipping table creation since system table %s.%s already exists.",
                        systemSchemaDescriptor.getSchemaName(),backupItemsTabInfo.getTableName()));
            }
        }

        // Create BACKUPFILESET
        TabInfoImpl backupStatesTabInfo=getBackupStatesTable();
        if(getTableDescriptor(backupStatesTabInfo.getTableName(),systemSchemaDescriptor,tc)==null){
            if(LOG.isTraceEnabled()){
                LOG.trace(String.format("Creating system table %s.%s",systemSchemaDescriptor.getSchemaName(),
                        backupStatesTabInfo.getTableName()));
            }
            makeCatalog(backupStatesTabInfo,systemSchemaDescriptor,tc);
        }else{
            if(LOG.isTraceEnabled()){
                LOG.trace(String.format("Skipping table creation since system table %s.%s already exists.",
                        systemSchemaDescriptor.getSchemaName(),backupStatesTabInfo.getTableName()));
            }
        }

        // Create BACKUPJOBS
        TabInfoImpl backupJobsTabInfo=getBackupJobsTable();
        if(getTableDescriptor(backupJobsTabInfo.getTableName(),systemSchemaDescriptor,tc)==null){
            if(LOG.isTraceEnabled()){
                LOG.trace(String.format("Creating system table %s.%s",systemSchemaDescriptor.getSchemaName(),
                        backupJobsTabInfo.getTableName()));
            }
            makeCatalog(backupJobsTabInfo,systemSchemaDescriptor,tc);
        }else{
            if(LOG.isTraceEnabled()){
                LOG.trace(String.format("Skipping table creation since system table %s.%s already exists.",
                        systemSchemaDescriptor.getSchemaName(),backupJobsTabInfo.getTableName()));
            }
        }
    }

    @Override
    protected void createDictionaryTables(Properties params,
                                          TransactionController tc,
                                          DataDescriptorGenerator ddg) throws StandardException{
        //create the base dictionary tables
        super.createDictionaryTables(params,tc,ddg);

        createLassenTables(tc);

        //create the Statistics tables
        createStatisticsTables(tc);

        // TODO - this needs to be included into an upgrade script (JY)
        createSnapshotTable(tc);

    }

    @Override
    public SystemAggregateGenerator getSystemAggregateGenerator(){
        return new SpliceSystemAggregatorGenerator(this);
    }

    @Override
    protected void loadDictionaryTables(TransactionController tc,
                                        Properties startParams) throws StandardException{
        super.loadDictionaryTables(tc,startParams);

        // Check splice data dictionary version to decide if upgrade is necessary
        upgradeIfNecessary(tc);
    }

    /**
     * Overridden so that SQL functions implemented as system procedures
     * will be found if in the SYSFUN schema. Otherwise, the default
     * behavior would be to ignore these and only consider functions
     * implicitly defined in {@link BaseDataDictionary#SYSFUN_FUNCTIONS},
     * which are not actually in the system catalog.
     */
    @SuppressWarnings("unchecked")
    public List getRoutineList(String schemaID,String routineName,char nameSpace) throws StandardException{

        List list=super.getRoutineList(schemaID,routineName,nameSpace);
        if(list.isEmpty()){
            if(schemaID.equals(SchemaDescriptor.SYSFUN_SCHEMA_UUID) &&
                    (nameSpace==AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR ||
                            nameSpace==AliasInfo.ALIAS_NAME_SPACE_AGGREGATE_AS_CHAR)){
                AliasDescriptor ad=getAliasDescriptor(schemaID,routineName,nameSpace);
                return ad==null?
                        Collections.EMPTY_LIST:
                        Collections.singletonList(ad);
            }
        }
        return list;
    }

    @Override
    protected void setDependencyManager(){
        SpliceLogUtils.trace(LOG,"Initializing the Splice Dependency Manager");
        this.dmgr=new SpliceDependencyManager(this);
    }

    @Override
    public void boot(boolean create,Properties startParams) throws StandardException{
        SpliceLogUtils.trace(LOG,"boot with create=%s,startParams=%s",create,startParams);
        DatabaseVersion databaseVersion=(new ManifestReader()).createVersion();
        if(!databaseVersion.isUnknown()){
            spliceSoftwareVersion=new Splice_DD_Version(this,databaseVersion.getMajorVersionNumber(),
                    databaseVersion.getMinorVersionNumber(),databaseVersion.getPatchVersionNumber(),
                    databaseVersion.getSprintVersionNumber());
        }
        if(create){
            SpliceAccessManager af=(SpliceAccessManager)Monitor.findServiceModule(this,AccessFactory.MODULE);
            SpliceTransactionManager txnManager=(SpliceTransactionManager)af.getTransaction(ContextService.getFactory().getCurrentContextManager());
            ((SpliceTransaction)txnManager.getRawTransaction()).elevate(Bytes.toBytes("boot"));
            if(spliceSoftwareVersion!=null){
                txnManager.setProperty(SPLICE_DATA_DICTIONARY_VERSION,spliceSoftwareVersion,true);
            }
        }

        super.boot(create,startParams);
    }

    @Override
    public boolean canSupport(Properties startParams){
        SpliceLogUtils.trace(LOG,"canSupport startParam=%s",startParams);
        return super.canSupport(startParams);
    }

    @Override
    public void startWriting(LanguageConnectionContext lcc) throws StandardException{
        startWriting(lcc,true);
    }

    @Override
    public void startWriting(LanguageConnectionContext lcc,boolean setDDMode) throws StandardException{
        elevateTxnForDictionaryOperations(lcc);
    }

    @Override
    public void getCurrentValueAndAdvance(String sequenceUUIDstring,NumberDataValue returnValue, boolean useBatch)
            throws StandardException{
        SpliceSequence sequence=getSpliceSequence(sequenceUUIDstring, useBatch);
        returnValue.setValue(sequence.getNext());
    }

    @Override
    public Long peekAtSequence(String schemaName,String sequenceName) throws StandardException {
        String sequenceUUIDstring=getSequenceID(schemaName, sequenceName);
        if(sequenceUUIDstring==null)
            throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND_DURING_EXECUTION,"SEQUENCE",(schemaName+"."+sequenceName));

        SpliceSequence sequence=getSpliceSequence(sequenceUUIDstring, true);
        return sequence.peekAtCurrentValue();
    }

    private SpliceSequence getSpliceSequence(String sequenceUUIDstring, boolean useBatch)
        throws StandardException {
        try{
            if(sequenceRowLocationBytesMap==null){
                sequenceRowLocationBytesMap=new ConcurrentLinkedHashMap.Builder<String, byte[]>()
                        .maximumWeightedCapacity(512)
                        .concurrencyLevel(64)
                        .build();
            }

            if(sequenceDescriptorMap==null){
                sequenceDescriptorMap=new ConcurrentLinkedHashMap.Builder<String, SequenceDescriptor[]>()
                        .maximumWeightedCapacity(512)
                        .concurrencyLevel(64)
                        .build();
            }
            byte[] sequenceRowLocationBytes=sequenceRowLocationBytesMap.get(sequenceUUIDstring);
            SequenceDescriptor[] sequenceDescriptor=sequenceDescriptorMap.get(sequenceUUIDstring);
            if(sequenceRowLocationBytes==null || sequenceDescriptor==null){
                RowLocation[] rowLocation=new RowLocation[1];
                sequenceDescriptor=new SequenceDescriptor[1];

                LanguageConnectionContext llc=(LanguageConnectionContext)
                        ContextService.getContextOrNull(LanguageConnectionContext.CONTEXT_ID);

                TransactionController tc=llc.getTransactionExecute();
                computeSequenceRowLocation(tc,sequenceUUIDstring,rowLocation,sequenceDescriptor);
                sequenceRowLocationBytes=rowLocation[0].getBytes();
                sequenceRowLocationBytesMap.put(sequenceUUIDstring,sequenceRowLocationBytes);
                sequenceDescriptorMap.put(sequenceUUIDstring,sequenceDescriptor);
            }

            long start=sequenceDescriptor[0].getStartValue();
            long increment=sequenceDescriptor[0].getIncrement();

            SIDriver siDriver =SIDriver.driver();
            PartitionFactory partFactory = siDriver.getTableFactory();
            TxnOperationFactory txnOpFactory = siDriver.getOperationFactory();
            return EngineDriver.driver().sequencePool().
                    get(new SequenceKey(sequenceRowLocationBytes,useBatch?SIDriver.driver().getConfiguration().getSequenceBlockSize():1l,start,increment,partFactory,txnOpFactory));
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }
    public void createOrUpdateAllSystemProcedures(TransactionController tc) throws StandardException{
        tc.elevate("dictionary");
        super.createOrUpdateAllSystemProcedures(tc);
    }

    /*Table fetchers for Statistics tables*/
    private TabInfoImpl getPhysicalStatisticsTable() throws StandardException{
        if(physicalStatsTable==null){
            physicalStatsTable=new TabInfoImpl(new SYSPHYSICALSTATISTICSRowFactory(uuidFactory,exFactory,dvf));
        }
        initSystemIndexVariables(physicalStatsTable);
        return physicalStatsTable;
    }

    private TabInfoImpl getColumnStatisticsTable() throws StandardException{
        if(columnStatsTable==null){
            columnStatsTable=new TabInfoImpl(new SYSCOLUMNSTATISTICSRowFactory(uuidFactory,exFactory,dvf));
        }
        initSystemIndexVariables(columnStatsTable);
        return columnStatsTable;
    }

    private TabInfoImpl getTableStatisticsTable() throws StandardException{
        if(tableStatsTable==null){
            tableStatsTable=new TabInfoImpl(new SYSTABLESTATISTICSRowFactory(uuidFactory,exFactory,dvf));
        }
        initSystemIndexVariables(tableStatsTable);
        return tableStatsTable;
    }

    protected TabInfoImpl getPkTable() throws StandardException{
        if(pkTable==null){
            pkTable=new TabInfoImpl(new SYSPRIMARYKEYSRowFactory(uuidFactory,exFactory,dvf));
        }
        initSystemIndexVariables(pkTable);
        return pkTable;
    }

    private void upgradeIfNecessary(TransactionController tc) throws StandardException{
        boolean toUpgrade = Boolean.TRUE.equals(EngineLifecycleService.toUpgrade.get());
        // Only master can upgrade
        if (!toUpgrade) {
            return;
        }

        Splice_DD_Version catalogVersion=(Splice_DD_Version)tc.getProperty(SPLICE_DATA_DICTIONARY_VERSION);
        if(needToUpgrade(catalogVersion)){
            tc.elevate("dictionary");
            SpliceCatalogUpgradeScripts scripts=new SpliceCatalogUpgradeScripts(this,catalogVersion,tc);
            scripts.run();
            tc.setProperty(SPLICE_DATA_DICTIONARY_VERSION,spliceSoftwareVersion,true);
            tc.commit();
        }
        // TODO - include the following in an upgrade script
        if (spliceSoftwareVersion.toLong() >= 2005000) {
            upgradeSystables(tc);
        }
    }

    private void upgradeSystables(TransactionController tc) throws StandardException {
        addNewColumToSystables(tc);
        SchemaDescriptor sd = getSystemSchemaDescriptor();
        TableDescriptor td = getTableDescriptor(SYSSNAPSHOTSRowFactory.TABLENAME_STRING, sd, tc);
        if (td == null)
        {
            tc.elevate("dictionary");
            createSnapshotTable(tc);
            SpliceLogUtils.info(LOG, "Catalog upgraded: added SYS.SYSSNAPSHOTS table.");
        }

        upgradeSysStatsTable(tc);

        createOrUpdateAllSystemProcedures(tc);

        updateSysTableStatsView1(tc);
    }

    private void addNewColumToSystables(TransactionController tc) throws StandardException {
        SchemaDescriptor sd = getSystemSchemaDescriptor();
        TableDescriptor td = getTableDescriptor(SYSTABLESRowFactory.TABLENAME_STRING, sd, tc);

        ColumnDescriptor cd = td.getColumnDescriptor(SYSTABLESRowFactory.DELIMITED);
        if (cd == null)
        {
            tc.elevate("dictionary");
            dropTableDescriptor(td, sd, tc);
            td.setColumnSequence(td.getColumnSequence() + 1);
            // add the table descriptor with new name
            addDescriptor(td, sd, DataDictionary.SYSTABLES_CATALOG_NUM, false, tc);


            SystemColumn[] columns = new SystemColumn[] {
                    SystemColumnImpl.getColumn("DELIMITED", Types.VARCHAR,  true),
                    SystemColumnImpl.getColumn("ESCAPED", Types.VARCHAR, true),
                    SystemColumnImpl.getColumn("LINES", Types.VARCHAR, true),
                    SystemColumnImpl.getColumn("STORED", Types.VARCHAR, true),
                    SystemColumnImpl.getColumn("LOCATION", Types.VARCHAR, true),
                    SystemColumnImpl.getColumn("COMPRESSION", Types.VARCHAR, true),
                    SystemColumnImpl.getColumn("IS_PINNED", Types.BOOLEAN, true)}; // allow it to be null, it's not used

            for (SystemColumn sc : columns) {
                DataValueDescriptor storableDV = sc.getType().getJDBCTypeId() == Types.VARCHAR ? getDataValueFactory().getNullVarchar(null) : getDataValueFactory().getNullBoolean(new SQLBoolean(false));
                int colNumber = td.getNumberOfColumns() + 1;
                DataTypeDescriptor dtd = sc.getType();
                tc.addColumnToConglomerate(td.getHeapConglomerateId(), colNumber, storableDV, dtd.getCollationType());
                UUID uuid = getUUIDFactory().createUUID();
                ColumnDescriptor columnDescriptor = new ColumnDescriptor(
                        sc.getName(),
                        colNumber,
                        colNumber,
                        dtd,
                        sc.getType().getJDBCTypeId() == Types.VARCHAR ? null : new SQLBoolean(false),
                        null,
                        td,
                        uuid,
                        0,
                        0,
                        td.getColumnSequence());

                addDescriptor(columnDescriptor, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);

                // now add the column to the tables column descriptor list.
                td.getColumnDescriptorList().add(columnDescriptor);

                updateSYSCOLPERMSforAddColumnToUserTable(td.getUUID(), tc);
                SpliceLogUtils.info(LOG, "SYS.SYSTABLES upgraded: added a new column %s.", sc.getName());
            }
        }
        cd = td.getColumnDescriptor(SYSTABLESRowFactory.PURGE_DELETED_ROWS);
        if (cd == null)
        {
            tc.elevate("dictionary");
            dropTableDescriptor(td, sd, tc);
            td.setColumnSequence(td.getColumnSequence() + 1);
            // add the table descriptor with new name
            addDescriptor(td, sd, DataDictionary.SYSTABLES_CATALOG_NUM, false, tc);

            DataValueDescriptor storableDV = getDataValueFactory().getNullBoolean(null);
            int colNumber = td.getNumberOfColumns() + 1;
            DataTypeDescriptor dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor((Types.BOOLEAN));
            tc.addColumnToConglomerate(td.getHeapConglomerateId(), colNumber, storableDV, dtd.getCollationType());
            UUID uuid = getUUIDFactory().createUUID();
            ColumnDescriptor columnDescriptor = new ColumnDescriptor(
                    SYSTABLESRowFactory.PURGE_DELETED_ROWS,
                    colNumber,
                    colNumber,
                    dtd,
                    new SQLBoolean(false),
                    null,
                    td,
                    uuid,
                    0,
                    0,
                    td.getColumnSequence());

            addDescriptor(columnDescriptor, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);

            // now add the column to the tables column descriptor list.
            td.getColumnDescriptorList().add(columnDescriptor);

            updateSYSCOLPERMSforAddColumnToUserTable(td.getUUID(), tc);
            SpliceLogUtils.info(LOG, "SYS.SYSTABLES upgraded: added a new column %s.", SYSTABLESRowFactory.PURGE_DELETED_ROWS);
        }
    }

    private void upgradeSysStatsTable(TransactionController tc) throws StandardException {
        SchemaDescriptor sd = getSystemSchemaDescriptor();
        TableDescriptor td = getTableDescriptor(SYSTABLESTATISTICSRowFactory.TABLENAME_STRING, sd, tc);
        ColumnDescriptor cd = td.getColumnDescriptor("SAMPLEFRACTION");
        if (cd == null) {
            tc.elevate("dictionary");
            dropTableDescriptor(td, sd, tc);
            td.setColumnSequence(td.getColumnSequence()+1);
            // add the table descriptor with new name
            addDescriptor(td,sd,DataDictionary.SYSTABLES_CATALOG_NUM,false,tc);

            DataValueDescriptor storableDV;
            int colNumber;
            DataTypeDescriptor dtd;
            ColumnDescriptor columnDescriptor;
            UUID uuid = getUUIDFactory().createUUID();

            /**
             *  Add the column NUMPARTITIONS
             */
            if (td.getColumnDescriptor("NUMPARTITIONS") == null) {
                storableDV = getDataValueFactory().getNullLong(null);
                colNumber = SYSTABLESTATISTICSRowFactory.NUMBEROFPARTITIONS;
                dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor((Types.BIGINT));
                tc.addColumnToConglomerate(td.getHeapConglomerateId(), colNumber, storableDV, dtd.getCollationType());

                columnDescriptor = new ColumnDescriptor("NUMPARTITIONS",9,9,dtd,new SQLLongint(1),null,td,uuid,0,0,8);

                addDescriptor(columnDescriptor, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);
                // now add the column to the tables column descriptor list.
                td.getColumnDescriptorList().add(columnDescriptor);
                updateSYSCOLPERMSforAddColumnToUserTable(td.getUUID(), tc);
            }
            /**
             * Add the column STATSTYPE
             */
            storableDV = getDataValueFactory().getNullInteger(null);
            colNumber = SYSTABLESTATISTICSRowFactory.STATSTYPE;
            dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor((Types.INTEGER));
            tc.addColumnToConglomerate(td.getHeapConglomerateId(), colNumber, storableDV, dtd.getCollationType());

            columnDescriptor =  new ColumnDescriptor("STATSTYPE",10,10,dtd,new SQLInteger(0),null,td,uuid,0,0, 9);

            addDescriptor(columnDescriptor, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);
            td.getColumnDescriptorList().add(columnDescriptor);
            updateSYSCOLPERMSforAddColumnToUserTable(td.getUUID(), tc);

            /**
             * Add the column SAMPLEFRACTION
             */
            storableDV = getDataValueFactory().getNullDouble(null);
            colNumber = SYSTABLESTATISTICSRowFactory.SAMPLEFRACTION;
            dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor((Types.DOUBLE));
            tc.addColumnToConglomerate(td.getHeapConglomerateId(), colNumber, storableDV, dtd.getCollationType());

            columnDescriptor =  new ColumnDescriptor("SAMPLEFRACTION",11,11,dtd,new SQLDouble(0),null,td,uuid,0,0,10);
            addDescriptor(columnDescriptor, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);
            td.getColumnDescriptorList().add(columnDescriptor);

            updateSYSCOLPERMSforAddColumnToUserTable(td.getUUID(), tc);
            SpliceLogUtils.info(LOG, "SYS.SYSTABLESTATS upgraded: added columns: NUMPARTITIONS, STATSTYPE, SAMPLEFRACTION.");

            updateSysTableStatsView(tc);
        }
    }

    private void updateSysTableStatsView(TransactionController tc) throws StandardException{
        //drop table descriptor corresponding to the tablestats view and add
        SchemaDescriptor sd=getSystemSchemaDescriptor();
        TableDescriptor td = getTableDescriptor("SYSTABLESTATISTICS", sd, tc);
        dropTableDescriptor(td, sd, tc);
        td.setColumnSequence(td.getColumnSequence()+1);
        // add the table descriptor with new name
        addDescriptor(td,sd,DataDictionary.SYSTABLES_CATALOG_NUM,false,tc);

        // add the two newly added columns statType and sampleFraction
        ColumnDescriptor columnDescriptor = new ColumnDescriptor("STATS_TYPE",10,10,DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER),null,null,td,td.getUUID(),0,0,9);
        addDescriptor(columnDescriptor, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);
        td.getColumnDescriptorList().add(columnDescriptor);

        columnDescriptor = new ColumnDescriptor("SAMPLE_FRACTION",11,11,DataTypeDescriptor.getBuiltInDataTypeDescriptor((Types.DOUBLE)),null,null,td,td.getUUID(),0,0,10);
        addDescriptor(columnDescriptor, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);
        td.getColumnDescriptorList().add(columnDescriptor);

        ViewDescriptor vd=getViewDescriptor(td);
        dropViewDescriptor(vd, tc);
        DataDescriptorGenerator ddg=getDataDescriptorGenerator();
        vd=ddg.newViewDescriptor(td.getUUID(),"SYSTABLESTATISTICS",
                SYSTABLESTATISTICSRowFactory.STATS_VIEW_SQL,0,sd.getUUID());
        addDescriptor(vd,sd,DataDictionary.SYSVIEWS_CATALOG_NUM,true,tc);
        SpliceLogUtils.info(LOG, "SYS.SYSVIEWS upgraded: updated view SYSTABLESTATISTICS with two more columns: STATSTYPE, SAMPLEFRACTION.");
    }

    private boolean needToUpgrade(Splice_DD_Version catalogVersion){

        LOG.info(String.format("Splice Software Version = %s",(spliceSoftwareVersion==null?"null":spliceSoftwareVersion.toString())));
        LOG.info(String.format("Splice Catalog Version = %s",(catalogVersion==null?"null":catalogVersion.toString())));

        // Check if there is a manual override that is forcing an upgrade.
        // This flag should only be true for the master server.  If the upgrade runs on the region server,
        // it would probably be bad (at least if it ran concurrently with another upgrade).
        SConfiguration configuration=SIDriver.driver().getConfiguration();
        if(configuration.upgradeForced()) {
            LOG.info(String.format("Upgrade has been manually forced from version %s",
                    configuration.getUpgradeForcedFrom()));
        }

        // Not sure about the current version, do not upgrade
        if(spliceSoftwareVersion==null){
            return false;
        }

        // This is a pre-Fuji catalog, upgrade it.
        if(catalogVersion==null){
            LOG.info("Upgrade needed since catalog version is null");
            return true;
        }

        // Compare software version and catalog version
        if(catalogVersion.toLong()<spliceSoftwareVersion.toLong()){
            LOG.info("Upgrade needed since catalog version < software version");
            return true;
        }
        return false;
    }


    private void updateSysTableStatsView1(TransactionController tc) throws StandardException{
        //drop table descriptor corresponding to the tablestats view and add
        SchemaDescriptor sd=getSystemSchemaDescriptor();
        TableDescriptor td = getTableDescriptor("SYSTABLESTATISTICS", sd, tc);

        ViewDescriptor vd=getViewDescriptor(td);
        // check if the view definition is the same or not
        if (Objects.equals(vd.getViewText(), SYSTABLESTATISTICSRowFactory.STATS_VIEW_SQL))
            return;
        dropViewDescriptor(vd, tc);
        DataDescriptorGenerator ddg=getDataDescriptorGenerator();
        vd=ddg.newViewDescriptor(td.getUUID(),"SYSTABLESTATISTICS",
                SYSTABLESTATISTICSRowFactory.STATS_VIEW_SQL,0,sd.getUUID());
        addDescriptor(vd,sd,DataDictionary.SYSVIEWS_CATALOG_NUM,true,tc);
        SpliceLogUtils.info(LOG, "SYS.SYSVIEWS upgraded: updated view SYSTABLESTATISTICS's definition");
    }

    private void addTableIfAbsent(TransactionController tc,SchemaDescriptor systemSchema,TabInfoImpl sysTableToAdd,
                                  ColumnOrdering[] columnOrder) throws StandardException{
        if(getTableDescriptor(sysTableToAdd.getTableName(),systemSchema,tc)==null){
            SpliceLogUtils.trace(LOG,String.format("Creating system table %s.%s",systemSchema.getSchemaName(),sysTableToAdd.getTableName()));
            makeCatalog(sysTableToAdd,systemSchema,tc,columnOrder);
        }else{
            SpliceLogUtils.trace(LOG,String.format("Skipping table creation since system table %s.%s already exists",systemSchema.getSchemaName(),sysTableToAdd.getTableName()));
        }
    }

    private void createSysTableStatsView(TransactionController tc) throws StandardException{
        //create statistics views
        SchemaDescriptor sysSchema=getSystemSchemaDescriptor();

        DataDescriptorGenerator ddg=getDataDescriptorGenerator();
        TableDescriptor view=ddg.newTableDescriptor("SYSTABLESTATISTICS",
                sysSchema,TableDescriptor.VIEW_TYPE,TableDescriptor.ROW_LOCK_GRANULARITY,-1,null,null,null,null,null,null,false,false);
        addDescriptor(view,sysSchema,DataDictionary.SYSTABLES_CATALOG_NUM,false,tc);
        UUID viewId=view.getUUID();
        ColumnDescriptor[] tableViewCds=SYSTABLESTATISTICSRowFactory.getViewColumns(view,viewId);
        addDescriptorArray(tableViewCds,view,DataDictionary.SYSCOLUMNS_CATALOG_NUM,false,tc);

        ColumnDescriptorList viewDl=view.getColumnDescriptorList();
        Collections.addAll(viewDl,tableViewCds);


        ViewDescriptor vd=ddg.newViewDescriptor(viewId,"SYSTABLESTATISTICS",
                SYSTABLESTATISTICSRowFactory.STATS_VIEW_SQL,0,sysSchema.getUUID());
        addDescriptor(vd,sysSchema,DataDictionary.SYSVIEWS_CATALOG_NUM,true,tc);
    }

    private void createSysColumnStatsView(TransactionController tc) throws StandardException{
        //create statistics views
        SchemaDescriptor sysSchema=getSystemSchemaDescriptor();

        DataDescriptorGenerator ddg=getDataDescriptorGenerator();
        TableDescriptor view=ddg.newTableDescriptor("SYSCOLUMNSTATISTICS",
                sysSchema,TableDescriptor.VIEW_TYPE,TableDescriptor.ROW_LOCK_GRANULARITY,-1,null,null,null,null,null,null,false,false);
        addDescriptor(view,sysSchema,DataDictionary.SYSTABLES_CATALOG_NUM,false,tc);
        UUID viewId=view.getUUID();
        ColumnDescriptor[] tableViewCds=SYSCOLUMNSTATISTICSRowFactory.getViewColumns(view,viewId);
        addDescriptorArray(tableViewCds,view,DataDictionary.SYSCOLUMNS_CATALOG_NUM,false,tc);

        ColumnDescriptorList viewDl=view.getColumnDescriptorList();
        Collections.addAll(viewDl,tableViewCds);

        ViewDescriptor vd=ddg.newViewDescriptor(viewId,"SYSCOLUMNSTATISTICS",
                SYSCOLUMNSTATISTICSRowFactory.STATS_VIEW_SQL,0,sysSchema.getUUID());
        addDescriptor(vd,sysSchema,DataDictionary.SYSVIEWS_CATALOG_NUM,true,tc);
    }

    private void elevateTxnForDictionaryOperations(LanguageConnectionContext lcc) throws StandardException{
        BaseSpliceTransaction rawTransaction=((SpliceTransactionManager)lcc.getTransactionExecute()).getRawTransaction();
        if (rawTransaction instanceof SpliceTransactionView) // Already serde
            return;
        assert rawTransaction instanceof SpliceTransaction:
                "Programmer Error: Cannot perform a data dictionary write with a non-SpliceTransaction";
        // No subtransactions from here on, since we are modifying the data dictionary we rely on persisted
        // transactions for coordination
        ((SpliceTransaction) rawTransaction).getActiveStateTxn().forbidSubtransactions();
        /*
         * This is a bit of an awkward hack--at this stage, we need to ensure that the transaction
         * allows writes, but we don't really know where it's going, except to the data dictionary (and
         * therefore to system tables only)
         *
         * Thankfully, we only use the write-table transaction field to determine whether or not to
         * pause DDL operations, which can only occur against non-system tables. Since we are indicating
         * that this transaction will be writing to system tables, we don't have to worry about it.
         *
         * HOWEVER, it's possible that a transaction could modify both dictionary and non-dictionary tables.
         * In that situation, we don't want to confuse people with which table is being modified. So to do this,
         * we just only elevate the transaction if we absolutely have to.
         */
        if(rawTransaction.allowsWrites())
            return;
        SpliceTransaction txn=(SpliceTransaction)rawTransaction;
        txn.elevate(Bytes.toBytes("dictionary"));
    }

    @Override
    public boolean canUseCache(TransactionController xactMgr) throws StandardException {
        // Only enable dictionary cache for region server.
        // TODO - enable it for master and spark executor
        if (!SpliceClient.isRegionServer) {
            SpliceLogUtils.debug(LOG, "Cannot use dictionary cache.");
            return false;
        }
        DDLDriver driver=DDLDriver.driver();
        if(driver==null) return false;
        DDLWatcher ddlWatcher=driver.ddlWatcher();
        if(xactMgr==null)
            xactMgr = getTransactionCompile();
        return ddlWatcher.canUseCache((TransactionManager)xactMgr);
    }

    @Override
    public boolean canUseSPSCache() throws StandardException {
        DDLDriver driver=DDLDriver.driver();
        if(driver==null) return false;
        DDLWatcher ddlWatcher=driver.ddlWatcher();
        return ddlWatcher.canUseSPSCache((TransactionManager)getTransactionCompile());
    }

    @Override
    public ColPermsDescriptor getColumnPermissions(UUID colPermsUUID) throws StandardException {
        Manager manager = EngineDriver.driver().manager();
            return manager.isEnabled()?manager.getColPermsManager().getColumnPermissions(this,colPermsUUID):null;
    }

    /**
     * Get one user's column privileges for a table.
     *
     * @param tableUUID       the uuid of the table of interest
     * @param privType        (as int) Authorizer.SELECT_PRIV, Authorizer.UPDATE_PRIV, or Authorizer.REFERENCES_PRIV
     * @param forGrant        whether or not we are looking for grant priviledges
     * @param authorizationId The user name
     * @return a ColPermsDescriptor or null if the user has no separate column
     * permissions of the specified type on the table. Note that the user may have been granted
     * permission on all the columns of the table (no column list), in which case this routine
     * will return null. You must also call getTablePermissions to see if the user has permission
     * on a set of columns.
     * @throws StandardException
     */
    @Override
    public ColPermsDescriptor getColumnPermissions(UUID tableUUID,
                                                   int privType,
                                                   boolean forGrant,
                                                   String authorizationId) throws StandardException{
        Manager manager = EngineDriver.driver().manager();
        return manager.isEnabled()?manager.getColPermsManager().getColumnPermissions(this,tableUUID,privType,forGrant,authorizationId):null;
    } // end of getColumnPermissions


}
