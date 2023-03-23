package org.infinispan.client.hotrod.query;

import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.client.hotrod.transaction.lookup.RemoteTransactionManagerLookup;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

/**
 * @since 9.4
 */
@Test(testName = "client.hotrod.query.TransactionalIndexingTest", groups = "functional")
public class TransactionalIndexingTest extends MultiHotRodServerQueryTest {

   @Override
   protected boolean useTransactions() {
      return true;
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(
           String host, int serverPort) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = super
              .createHotRodClientConfigurationBuilder(host, serverPort);
      clientBuilder.forceReturnValues(false);
      clientBuilder.remoteCache("").transactionManagerLookup(RemoteTransactionManagerLookup.getInstance())
              .transactionMode(TransactionMode.FULL_XA);

      return clientBuilder;
   }

   public void testConflictWithTxRemoveByIdRemoveByQuery() throws Exception {
      final TransactionManager tm = remoteCache1.getTransactionManager();

      QueryFactory qf = Search.getQueryFactory(remoteCache1);
      Query<Object[]> readQuery = qf.create("FROM sample_bank_account.User WHERE name = 'Tom'");
      Query<Object[]> deleteQuery = qf.create("DELETE FROM sample_bank_account.User WHERE name = 'Tom'");

      tm.begin();

      // read by a query OK
      //readQuery.execute().list().get(0);

      // read by id is required to get rollback
      remoteCache1.get(1);

      // remove by id
      remoteCache1.remove(1);

      // remove by query that targets the same entity
      deleteQuery.executeStatement();

      // rollback with the commit
      tm.commit();
   }
}
