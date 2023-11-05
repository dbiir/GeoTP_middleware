package org.apache.shardingsphere.transaction.xa.harp.manager;

import com.atomikos.datasource.xa.jdbc.JdbcTransactionalResource;
import org.apache.shardingsphere.transaction.xa.spi.SingleXAResource;

import javax.sql.XADataSource;
import javax.transaction.xa.XAResource;

public class HarpXARecoverableResource extends JdbcTransactionalResource {
    private final String resourceName;

    HarpXARecoverableResource(final String serverName, final XADataSource xaDataSource) {
        super(serverName, xaDataSource);
        resourceName = serverName;
    }

    @Override
    public boolean usesXAResource(final XAResource xaResource) {
        return resourceName.equals(((SingleXAResource) xaResource).getResourceName());
    }
}
