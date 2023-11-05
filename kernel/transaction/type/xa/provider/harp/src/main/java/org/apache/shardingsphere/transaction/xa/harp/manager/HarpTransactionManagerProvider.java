package org.apache.shardingsphere.transaction.xa.harp.manager;

import com.atomikos.icatch.config.UserTransactionService;
import com.atomikos.icatch.config.UserTransactionServiceImp;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.shardingsphere.transaction.xa.spi.SingleXAResource;
import org.apache.shardingsphere.transaction.xa.spi.XATransactionManagerProvider;

import javax.sql.XADataSource;
import javax.transaction.SystemException;
import javax.transaction.RollbackException;

public class HarpTransactionManagerProvider implements XATransactionManagerProvider {
    @Getter
    HarpTransactionManager transactionManager;
    private UserTransactionService userTransactionService;

    @Override
    public void init() {
        transactionManager = new HarpTransactionManager();
        userTransactionService = new UserTransactionServiceImp();
        userTransactionService.init();
    }

    @Override
    public void registerRecoveryResource(String dataSourceName, XADataSource xaDataSource) {
        userTransactionService.registerResource(new HarpXARecoverableResource(dataSourceName, xaDataSource));
    }

    @Override
    public void removeRecoveryResource(String dataSourceName, XADataSource xaDataSource) {
        userTransactionService.removeResource(new HarpXARecoverableResource(dataSourceName, xaDataSource));
    }

    @SneakyThrows({SystemException.class, RollbackException.class})
    @Override
    public void enlistResource(SingleXAResource singleXAResource) {
        transactionManager.getTransaction().enlistResource(singleXAResource);
    }

    @Override
    public void close() throws Exception {
        userTransactionService.shutdown(true);
    }

    @Override
    public String getType() {
        return "Harp";
    }

    @Override
    public boolean isDefault() {
        return true;
    }
}
