package com.splicemachine.derby.ddl;

import org.apache.derby.iapi.error.StandardException;

public interface DDLController {
    public void notifyMetadataChange(String transactionId) throws StandardException;
}
