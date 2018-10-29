package kz.greetgo.sandbox.register.impl.jdbc.migration;

import kz.greetgo.sandbox.register.impl.jdbc.migration.model.FrsAccount;
import kz.greetgo.sandbox.register.impl.jdbc.migration.model.FrsTransaction;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;

@SuppressWarnings("WeakerAccess, SqlResolve")
public class FrsMigrationImpl extends MigrationAbstract {

  final static Logger logger = Logger.getLogger("kz.greetgo.sandbox.register.impl.jdbc.migration.FrsMigrationImpl");

  public PreparedStatement accountInsertPS;
  public PreparedStatement transactionInsertPS;

  public final int MAX_BATCH_SIZE = 50000;

  public int accountBatchCount = 0;
  public int transactionBatchCount = 0;

  public FrsMigrationImpl(Connection connection) {
    super(connection);
  }

  public FrsMigrationImpl(Connection connection, FTPClient ftp, String filePath) {
    super(connection, ftp, filePath);
  }

  @Override
  public void createTempTables() throws Exception {

    Instant startTime = Instant.now();

    if (logger.isInfoEnabled()) {
      logger.info("Started creating temp tables!");
    }

    if (logger.isInfoEnabled()) {
      logger.info("Creating temp table - ClientAccountTempTable!");
    }

    final String clientAccountTableCreate =
      "create table client_account_temp (" +
        " client varchar(100), " +
        " account_number varchar(100), " +
        " registered_at varchar(100), " +
        " status int default 1, " +
        " migration_order int" +
        ")";

    try (PreparedStatement ps = connection.prepareStatement(clientAccountTableCreate)) {
      ps.executeUpdate();
    }

    if (logger.isInfoEnabled()) {
      logger.info("Creating temp table - ClientAccountTransactionTempTable!");
    }

    final String clientAccountTransactionTableCreate =
      "create table client_account_transaction_temp (" +
        " transaction_type varchar(100), " +
        " account_number varchar(100), " +
        " finished_at varchar(100), " +
        " money varchar(100), " +
        " status int default 1" +
        ")";

    try (PreparedStatement ps = connection.prepareStatement(clientAccountTransactionTableCreate)) {
      ps.executeUpdate();
    }

    Instant endTime = Instant.now();
    Duration timeSpent = Duration.between(startTime, endTime);


    if (logger.isInfoEnabled()) {
      logger.info(String.format("Temporary tables were created! Time taken: %s milliseconds!", timeSpent.toMillis()));
    }
  }

  @Override
  public void parseAndFillData() throws Exception {

    this.connection.setAutoCommit(false);

    Instant startTime = Instant.now();
    int accountCount = 0;
    int transactionCount = 0;

    if (logger.isInfoEnabled()) {
      logger.info(String.format("Started parsing file %s, and inserting to temp tables!", filePath));
    }

    initPreparedStatements();

    InputStream stream = ftp.retrieveFileStream(filePath);
    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));

    String rowString;

    while ((rowString = bufferedReader.readLine()) != null) {
      JSONObject rowJson = new JSONObject(rowString);

      if (rowJson.get("type").equals("transaction")) {
        FrsTransaction frsTransaction = new FrsTransaction();
        frsTransaction.money = ((String) rowJson.get("money")).replace("_", "");
        frsTransaction.finishedAt = (String) rowJson.get("finished_at");
        frsTransaction.transactionType = (String) rowJson.get("transaction_type");
        frsTransaction.accountNumber = (String) rowJson.get("account_number");

        transactionInsertPS.setObject(1, frsTransaction.accountNumber);
        transactionInsertPS.setObject(2, frsTransaction.transactionType);
        transactionInsertPS.setObject(3, frsTransaction.money);
        transactionInsertPS.setObject(4, frsTransaction.finishedAt);

        transactionInsertPS.addBatch();

        if (transactionBatchCount == MAX_BATCH_SIZE) {
          transactionInsertPS.executeBatch();
          connection.commit();
          transactionBatchCount = 0;
        }

        transactionBatchCount++;
        transactionCount++;

      } else if (rowJson.get("type").equals("new_account")) {
        FrsAccount frsAccount = new FrsAccount();
        frsAccount.client = (String) rowJson.get("client_id");
        frsAccount.accountNumber = (String) rowJson.get("account_number");
        frsAccount.registeredAt = (String) rowJson.get("registered_at");

        accountInsertPS.setObject(1, frsAccount.client);
        accountInsertPS.setObject(2, frsAccount.accountNumber);
        accountInsertPS.setObject(3, frsAccount.registeredAt);

        accountInsertPS.addBatch();

        if (accountBatchCount == MAX_BATCH_SIZE) {
          accountInsertPS.executeBatch();
          connection.commit();
          accountBatchCount = 0;
        }

        accountBatchCount++;
        accountCount++;
      }
    }

    executeLeftBatches();

    connection.setAutoCommit(true);

    bufferedReader.close();
    stream.close();
    ftp.completePendingCommand();
    ftp.rename(filePath, filePath + ".txt");
    ftp.disconnect();

    Instant endTime = Instant.now();
    Duration timeSpent = Duration.between(startTime, endTime);

    if (logger.isInfoEnabled()) {
      logger.info(String.format("Ended parsing file %s, and in total inserted %d accounts and %d transactions! Time taken: %s milliseconds!", filePath, accountCount, transactionCount, timeSpent.toMillis()));
    }
  }

  /*
   Function for checking records for validness, if there some error than changes it`s status to 2
 */
  @Override
  public void checkForValidness() throws Exception {

    Instant startTime = Instant.now();

    if (logger.isInfoEnabled()) {
      logger.info("Started checking temp tables for validness, and if there error sets status = 2!");
    }

    if (logger.isInfoEnabled()) {
      logger.info("Checking temp table - ClientAccountTempTable!");
    }

    String clientAccountTempTableUpdateError =
      "update client_account_temp set status = 2 " +
        " where client = '' or client = 'null' " +
        "    or account_number = '' or account_number = 'null' " +
        "    or registered_at = '' or registered_at = 'null'";

    try (PreparedStatement ps = connection.prepareStatement(clientAccountTempTableUpdateError)) {
      ps.executeUpdate();
    }

    if (logger.isInfoEnabled()) {
      logger.info("Checking temp table - ClientAccountTransactionTempTable!");
    }

    String clientAccountTransactionTempTableUpdateError =
      "update client_account_transaction_temp set status = 2 " +
        " where transaction_type = '' or transaction_type = 'null' " +
        "    or account_number = '' or account_number = 'null' " +
        "    or finished_at = '' or finished_at = 'null' " +
        "    or money = '' or money = 'null'";

    try (PreparedStatement ps = connection.prepareStatement(clientAccountTransactionTempTableUpdateError)) {
      ps.executeUpdate();
    }

    Instant endTime = Instant.now();
    Duration timeSpent = Duration.between(startTime, endTime);


    if (logger.isInfoEnabled()) {
      logger.info(String.format("Ended checking temp tables for validness! Time taken: %s milliseconds!", timeSpent.toMillis()));
    }
  }

  @Override
  public void validateAndMigrateData() throws Exception {

    Instant startTime = Instant.now();

    if (logger.isInfoEnabled()) {
      logger.info("Started validating and migrating data!");
    }

    if (logger.isInfoEnabled()) {
      logger.info("Inserting new transaction types!");
    }

    Instant startTransactionTypeInsertTime = Instant.now();

    // Adding new transaction types

    String clientAccountTransactionTypeInsert =
      "insert into transaction_type (name, id, code) " +
        " select " +
        "   distinct transaction_type as name, " +
        "   nextval('id') as id, " +
        "   nextval('code') as code " +
        " from client_account_transaction_temp " +
        " where transaction_type notnull and status = 1" +
        " group by name " +
        "on conflict (name) do nothing";

    try (PreparedStatement ps = connection.prepareStatement(clientAccountTransactionTypeInsert)) {
      ps.executeUpdate();
    }

    Instant endTransactionTypeInsertTime = Instant.now();
    Duration timeSpentTransactionTypeInsert = Duration.between(startTransactionTypeInsertTime, endTransactionTypeInsertTime);

    if (logger.isInfoEnabled()) {
      logger.info(String.format("Ended inserting new Transaction Types! Time taken: %s milliseconds!", timeSpentTransactionTypeInsert.toMillis()));
    }

    if (logger.isInfoEnabled()) {
      logger.info("Validating and migrating Account!");
    }

    Instant startAccountValidateTime = Instant.now();

    // Migrate valid accounts and transactions with actual 1

    String clientAccountTableUpdateMigrate =
      "insert into client_account (id, client, number, registered_at, migration_client) " +
        "  select " +
        "  distinct on (account_number)" +
        "  nextval('id') as id, " +
        "  cl.id as client, " +
        "  ac_temp.account_number as number, " +
        "  to_timestamp(ac_temp.registered_at, 'YYYY-MM-DD hh24:mi:ss') as registered_at, " +
        "  ac_temp.client as migration_client" +
        " from client_account_temp ac_temp " +
        "   left join client cl " +
        "     on cl.migration_id = ac_temp.client" +
        "   where status = 1 " +
        "   order by account_number, migration_order asc" +
        " on conflict (number) " +
        " do nothing";

    try (PreparedStatement ps = connection.prepareStatement(clientAccountTableUpdateMigrate)) {
      ps.executeUpdate();
    }

    Instant endAccountValidateTime = Instant.now();
    Duration timeSpentAccountValidate = Duration.between(startAccountValidateTime, endAccountValidateTime);

    if (logger.isInfoEnabled()) {
      logger.info(String.format("Ended validating Accounts! Time taken: %s milliseconds!", timeSpentAccountValidate.toMillis()));
    }

    disableAccounts();

    if (logger.isInfoEnabled()) {
      logger.info("Validating and migrating Transaction!");
    }

    Instant startTransactionValidateTime = Instant.now();

    String clientAccountTransactionTableUpdateMigrate =
      "insert into client_account_transaction (id, account, money, finished_at, type, migration_account) " +
        " select nextval('id') as id, " +
        "   ac.id as account, " +
        "   cast(ac_tr_temp.money as double precision) as money, " +
        "   to_timestamp(ac_tr_temp.finished_at, 'YYYY-MM-DD hh24:mi:ss') as finished_at, " +
        "   tt.id as type, " +
        "   account_number as migration_account " +
        " from client_account_transaction_temp ac_tr_temp " +
        "   left join client_account ac " +
        "     on ac.actual = 1 and account_number = ac.number  " +
        "   left join transaction_type tt " +
        "     on transaction_type = tt.name " +
        " where status = 1 " +
        "on conflict (migration_account, money, finished_at) do nothing";

    try (PreparedStatement ps = connection.prepareStatement(clientAccountTransactionTableUpdateMigrate)) {
      ps.executeUpdate();
    }

    Instant endTransactionValidateTime = Instant.now();
    Duration timeSpentTransactionValidate = Duration.between(startTransactionValidateTime, endTransactionValidateTime);

    if (logger.isInfoEnabled()) {
      logger.info(String.format("Ended validating Transactions! Time taken: %s milliseconds!", timeSpentTransactionValidate.toMillis()));
    }

    disableTransactions();

//    if (logger.isInfoEnabled()) {
//      logger.info("Validating and migrating Account (setting money from transactions)!");
//    }

//    String clientAccountTableUpdateMigrateMoney =
//      "update client_account " +
//        "set money = (select sum(money) from client_account_transaction where account = client_account.id and type notnull) " +
//        "where actual = 1 and client notnull";
//
//    try (PreparedStatement ps = connection.prepareStatement(clientAccountTableUpdateMigrateMoney)) {
//      ps.executeUpdate();
//    }

    Instant endTime = Instant.now();
    Duration timeSpent = Duration.between(startTime, endTime);

    if (logger.isInfoEnabled()) {
      logger.info(String.format("Ended validating and migrating Account, Transaction! Time taken: %s milliseconds!", timeSpent.toMillis()));
    }
  }

  private void disableAccounts() throws Exception {
    if (logger.isInfoEnabled()) {
      logger.info("Disabling Client Accounts!");
    }

    String clientAccountTableUpdateDisable =
      "update client_account " +
        "set actual = 0 " +
        "where client isnull and actual = 1";

    try (PreparedStatement ps = connection.prepareStatement(clientAccountTableUpdateDisable)) {
      ps.executeUpdate();
    }
  }

  private void disableTransactions() throws Exception {
    if (logger.isInfoEnabled()) {
      logger.info("Disabling Client Account Transactions!");
    }

    String clientAccountTransactionTableUpdateDisable =
            "update client_account_transaction " +
                    "set actual = 0 " +
                    "where account isnull and actual = 1";

    try (PreparedStatement ps = connection.prepareStatement(clientAccountTransactionTableUpdateDisable)) {
      ps.executeUpdate();
    }
  }

  @Override
  public void dropTemplateTables() throws Exception {

    Instant startTime = Instant.now();

    if (logger.isInfoEnabled()) {
      logger.info("Started dropping temp tables!");
    }

    if (logger.isInfoEnabled()) {
      logger.info("Dropping Client Account Temp Table!");
    }

    final String clientAccountTableDrop = "drop table if exists client_account_temp";

    try (PreparedStatement ps = connection.prepareStatement(clientAccountTableDrop)) {
      ps.executeUpdate();
    }

    if (logger.isInfoEnabled()) {
      logger.info("Dropping Client Account Transaction Temp Table!");
    }

    final String clientAccountTransactionTableDrop = "drop table if exists client_account_transaction_temp";

    try (PreparedStatement ps = connection.prepareStatement(clientAccountTransactionTableDrop)) {
      ps.executeUpdate();
    }

    Instant endTime = Instant.now();
    Duration timeSpent = Duration.between(startTime, endTime);


    if (logger.isInfoEnabled()) {
      logger.info(String.format("Ended dropping temp tables! Time taken: %s milliseconds!", timeSpent.toMillis()));
    }
  }

  /*
    Function for checking new records that needed for disabled data, if there exists than we enable disabled records
  */
  @Override
  public void checkForLateUpdates() throws Exception {

    Instant startTime = Instant.now();

    if (logger.isInfoEnabled()) {
      logger.info("Started checking late updates, ex: new clients for existing accounts or new accounts for transactions!");
    }

    if (logger.isInfoEnabled()) {
      logger.info("Checking Accounts without Clients for new Clients!");
    }

    Instant startCheckLateUpdateAccountTime = Instant.now();

    String clientAccountTableUpdateMigrate =
      "update client_account " +
        "set actual = 1, " +
        "    client = cl.id " +
        "   from client cl " +
        "where client_account.actual = 0 and migration_client notnull and cl.migration_id = migration_client and cl.id notnull";

    try (PreparedStatement ps = connection.prepareStatement(clientAccountTableUpdateMigrate)) {
      ps.executeUpdate();
    }

    Instant endCheckLateUpdateAccountTime = Instant.now();
    Duration timeCheckLateUpdateAccountSpent = Duration.between(startCheckLateUpdateAccountTime, endCheckLateUpdateAccountTime);

    if (logger.isInfoEnabled()) {
      logger.info(String.format("Ended checking late update Accounts! Time taken: %s milliseconds!", timeCheckLateUpdateAccountSpent.toMillis()));
    }

    if (logger.isInfoEnabled()) {
      logger.info("Checking Transactions without Accounts for new Accounts!");
    }

    Instant startCheckLateUpdateTransactionTime = Instant.now();

    String clientAccountTransactionTableUpdateMigrate =
      "update client_account_transaction " +
        "set actual = 1, " +
        "    account = acc.id " +
        "   from client_account acc " +
        "where client_account_transaction.actual = 0 and acc.actual = 1 and migration_account notnull and acc.number = migration_account and acc.id notnull";

    try (PreparedStatement ps = connection.prepareStatement(clientAccountTransactionTableUpdateMigrate)) {
      ps.executeUpdate();
    }

    Instant endCheckLateUpdateTransactionTime = Instant.now();
    Duration timeCheckLateUpdateTransactionSpent = Duration.between(startCheckLateUpdateTransactionTime, endCheckLateUpdateTransactionTime);

    if (logger.isInfoEnabled()) {
      logger.info(String.format("Ended checking late update Transactions! Time taken: %s milliseconds!", timeCheckLateUpdateTransactionSpent.toMillis()));
    }

//    if (logger.isInfoEnabled()) {
//      logger.info("Validating and migrating new Accounts (setting money from transactions)!");
//    }

//    String clientAccountTableUpdateMigrateMoney =
//      "update client_account " +
//        "set money = (select sum(money) from client_account_transaction where account = client_account.id and type notnull) " +
//        "where actual = 1 and client notnull";
//
//    try (PreparedStatement ps = connection.prepareStatement(clientAccountTableUpdateMigrateMoney)) {
//      ps.executeUpdate();
//    }

    Instant endTime = Instant.now();
    Duration timeSpent = Duration.between(startTime, endTime);


    if (logger.isInfoEnabled()) {
      logger.info(String.format("Ended checking late updates! Time taken: %s milliseconds!", timeSpent.toMillis()));
    }
  }


  private void initPreparedStatements() throws Exception {
    String clientAccountTempTableInsert =
      "insert into client_account_temp (client, account_number, registered_at, migration_order) " +
        " values (?, ?, ?, nextval('migration_order'))";

    String clientAccountTransactionTempTableInsert =
      "insert into client_account_transaction_temp (account_number, transaction_type, money, finished_at) " +
        " values (?, ?, ?, ?)";

    transactionInsertPS = connection.prepareStatement(clientAccountTransactionTempTableInsert);
    accountInsertPS = connection.prepareStatement(clientAccountTempTableInsert);
  }

  private void executeLeftBatches() throws Exception {
    accountInsertPS.executeBatch();
    connection.commit();
    transactionInsertPS.executeBatch();
    connection.commit();
  }
}
