package kz.greetgo.sandbox.register.impl.jdbc.migration;

import kz.greetgo.depinject.core.BeanGetter;
import kz.greetgo.sandbox.register.configs.MigrationConfig;
import kz.greetgo.sandbox.register.dao_model.Character;
import kz.greetgo.sandbox.register.dao_model.*;
import kz.greetgo.sandbox.register.dao_model.temp.ClientAccountTemp;
import kz.greetgo.sandbox.register.dao_model.temp.ClientAccountTransactionTemp;
import kz.greetgo.sandbox.register.impl.MigrationRegisterImpl;
import kz.greetgo.sandbox.register.impl.jdbc.migration.model.FrsAccount;
import kz.greetgo.sandbox.register.impl.jdbc.migration.model.FrsTransaction;
import kz.greetgo.sandbox.register.test.dao.CharacterTestDao;
import kz.greetgo.sandbox.register.test.dao.MigrationTestDao;
import kz.greetgo.sandbox.register.test.util.ParentTestNg;
import org.apache.commons.net.ftp.FTPClient;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;

@SuppressWarnings("WeakerAccess")
public class FrsMigrationTest extends ParentTestNg {

  public BeanGetter<MigrationTestDao> migrationTestDao;
  public BeanGetter<CharacterTestDao> characterTestDao;
  public BeanGetter<MigrationRegisterImpl> migrationRegister;
  public BeanGetter<MigrationConfig> migrationConfig;

  public FTPClient ftp;

  private FrsMigrationImpl frsMigration;


  private void prepareTempTables() throws Exception {
    frsMigration = new FrsMigrationImpl(migrationRegister.get().getConnection());

    frsMigration.dropTemplateTables();
    frsMigration.createTempTables();
  }

  private Timestamp getTimestampFromString(String date) throws Exception {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    Date parsedDate = dateFormat.parse(date);

    return new Timestamp(parsedDate.getTime());
  }

  private Character insertCharacter(String name) {
    Character character = new Character();
    character.name = name;
    character.description = "Default description";
    character.energy = 100;

    characterTestDao.get().insertCharacter(character);

    return character;
  }

  private Client insertClient(int id, int characterId, String migrationId) {
    Client client = new Client();
    client.id = id;
    client.surname = "Default surname";
    client.name = "Default name";
    client.patronymic = "";
    client.gender = "MALE";
    client.birthDate = new GregorianCalendar(2000, 10, 10).getTime();
    client.charm = characterId;
    client.migration_id = migrationId;

    migrationTestDao.get().insertClient(client);

    return client;
  }

  @SuppressWarnings("SameParameterValue")
  private ClientAccount insertClientAccount(Integer client, double money, String number, Timestamp registeredAt, int actual, String migrationClient) {
    ClientAccount clientAccount = new ClientAccount();
    clientAccount.client = client;
    clientAccount.money = money;
    clientAccount.number = number;
    clientAccount.registeredAt = registeredAt;
    clientAccount.actual = actual;
    clientAccount.migrationClient = migrationClient;

    migrationTestDao.get().insertClientAccount(clientAccount);

    return clientAccount;
  }

  private ClientAccountTemp insertClientAccountTemp(String client, String accountNumber, String registeredAt) {
    ClientAccountTemp clientAccountTemp = new ClientAccountTemp();
    clientAccountTemp.client = client;
    clientAccountTemp.accountNumber = accountNumber;
    clientAccountTemp.registeredAt = registeredAt;
    clientAccountTemp.status = 1;

    migrationTestDao.get().insertClientAccountTemp(clientAccountTemp);

    return clientAccountTemp;
  }

  @SuppressWarnings("SameParameterValue")
  private ClientAccountTransaction insertClientAccountTransaction(Integer account, double money, Timestamp finishedAt, int type, int actual, String migrationAccount) {
    ClientAccountTransaction clientAccountTransaction = new ClientAccountTransaction();
    clientAccountTransaction.account = account;
    clientAccountTransaction.money = money;
    clientAccountTransaction.finishedAt = finishedAt;
    clientAccountTransaction.type = type;
    clientAccountTransaction.migrationAccount = migrationAccount;
    clientAccountTransaction.actual = actual;

    migrationTestDao.get().insertClientAccountTransaction(clientAccountTransaction);

    return clientAccountTransaction;
  }

  private ClientAccountTransactionTemp insertClientAccountTransactionTemp(String transactionType, String accountNumber, String finishedAt, String money) {
    ClientAccountTransactionTemp clientAccountTransactionTemp = new ClientAccountTransactionTemp();
    clientAccountTransactionTemp.transactionType = transactionType;
    clientAccountTransactionTemp.accountNumber = accountNumber;
    clientAccountTransactionTemp.finishedAt = finishedAt;
    clientAccountTransactionTemp.money = money;
    clientAccountTransactionTemp.status = 1;

    migrationTestDao.get().insertClientAccountTransactionTemp(clientAccountTransactionTemp);

    return clientAccountTransactionTemp;
  }

  @SuppressWarnings("SameParameterValue")
  private TransactionType insertTransactionType(String name) {
    TransactionType transactionType = new TransactionType();
    transactionType.name = name;

    migrationTestDao.get().insertTransactionType(transactionType);

    return transactionType;
  }

  @Test
  public void parseAndFillData_account() throws Exception {

    this.prepareTempTables();

    String fileName = "from_frs_2018-02-21-154543-1-30009.json_row";
    ftp = migrationRegister.get().getFtpConnection();
    String filePath = String.format("%s/%s", migrationConfig.get().ftpTestPath(), fileName);
    InputStream frsStream = ftp.retrieveFileStream(filePath);

    //
    //
    frsMigration = new FrsMigrationImpl(migrationRegister.get().getConnection(), frsStream, filePath);
    frsMigration.parseAndFillData();
    //
    //

    List<FrsAccount> accounts = migrationTestDao.get().getAccounts();

    assertThat(accounts).hasSize(3);

    assertThat(accounts.get(0).client).isEqualTo("1-A69-QA-PJ-G6hRzbEf2W");
    assertThat(accounts.get(0).accountNumber).isEqualTo("19382KZ865-20725-98987-8267359");
    assertThat(accounts.get(0).registeredAt).isEqualTo("2001-02-21T15:51:14.111");

    assertThat(accounts.get(1).client).isEqualTo("");
    assertThat(accounts.get(1).accountNumber).isEqualTo("22382KZ865");
    assertThat(accounts.get(1).registeredAt).isEqualTo("2001-02-21T15:51:14.111");

    assertThat(accounts.get(2).client).isEqualTo("10-A69-QA-PJ-G6hRzbEf2W");
    assertThat(accounts.get(2).accountNumber).isEqualTo("19382KZ865-20725-11111-8267359");
    assertThat(accounts.get(2).registeredAt).isEqualTo("2001-02-21T15:11:11.111");
  }

  @Test
  public void parseAndFillData_account_transaction() throws Exception {

    this.prepareTempTables();

    String fileName = "from_frs_2018-02-21-154543-1-30009.json_row";
    ftp = migrationRegister.get().getFtpConnection();
    String filePath = String.format("%s/%s", migrationConfig.get().ftpTestPath(), fileName);
    InputStream frsStream = ftp.retrieveFileStream(filePath);

    //
    //
    frsMigration = new FrsMigrationImpl(migrationRegister.get().getConnection(), frsStream, filePath);
    frsMigration.parseAndFillData();
    //
    //

    List<FrsTransaction> transactions = migrationTestDao.get().getAccountTransactions();

    assertThat(transactions).hasSize(5);

    assertThat(transactions.get(0).transactionType).isEqualTo("null");
    assertThat(transactions.get(0).accountNumber).isEqualTo("19382KZ865-20725-98987-8267359");
    assertThat(transactions.get(0).finishedAt).isEqualTo("2011-02-21T15:51:18.996");
    assertThat(transactions.get(0).money).isEqualTo("+7000");


    assertThat(transactions.get(1).transactionType).isEqualTo("Перевод в офшоры");
    assertThat(transactions.get(1).accountNumber).isEqualTo("19382KZ865-20725-98987-8267359");
    assertThat(transactions.get(1).finishedAt).isEqualTo("2011-02-21T15:51:16.996");
    assertThat(transactions.get(1).money).isEqualTo("-4321.5");

    assertThat(transactions.get(2).transactionType).isEqualTo("Списывание с регионального бюджета Алматинской области");
    assertThat(transactions.get(2).accountNumber).isNullOrEmpty();
    assertThat(transactions.get(2).finishedAt).isEqualTo("2011-02-21T15:51:14.996");
    assertThat(transactions.get(2).money).isEqualTo("+4321.5");

    assertThat(transactions.get(3).transactionType).isEqualTo("null");
    assertThat(transactions.get(3).accountNumber).isEqualTo("57743KZ372-07560-15462-1553047");
    assertThat(transactions.get(3).finishedAt).isEqualTo("2011-02-21T15:51:26.996");
    assertThat(transactions.get(3).money).isEqualTo("+0");

    assertThat(transactions.get(4).transactionType).isEqualTo("Отмывание на компьютерной технике");
    assertThat(transactions.get(4).accountNumber).isEqualTo("57743KZ372-07560-15462-1553047");
    assertThat(transactions.get(4).finishedAt).isEqualTo("2011-02-21T15:51:25.996");
    assertThat(transactions.get(4).money).isEqualTo("-3732");
  }


  @Test
  public void checkForValidness_account_client() throws Exception {

    this.prepareTempTables();

    insertClientAccountTemp("1-A69-QA-PJ-G6hRzbEf2W", "19382KZ865-20725-98987-8267359", "2001-02-21T15:51:14.111");
    insertClientAccountTemp("", "22382KZ865", "2001-02-21T15:51:14.111");
    insertClientAccountTemp("10-A69-QA-PJ-G6hRzbEf2W", "2001-02-21T15:11:11.111", "2001-02-21T15:51:14.111");

    //
    //
    frsMigration.checkForValidness();
    //
    //

    List<Integer> statuses = migrationTestDao.get().getAccountsWithoutClients();

    assertThat(statuses).hasSize(1);

    for (Integer status : statuses) {
      assertThat(status).isEqualTo(2);
    }
  }

  @Test
  public void checkForValidness_account_transaction_account_number() throws Exception {

    this.prepareTempTables();

    insertClientAccountTransactionTemp("null", "19382KZ865-20725-98987-8267359", "2011-02-21T15:51:18.996", "+7000");
    insertClientAccountTransactionTemp("Перевод в офшоры", "19382KZ865-20725-98987-8267359", "2011-02-21T15:51:16.996", "-4321.5");
    insertClientAccountTransactionTemp("Списывание с регионального бюджета Алматинской области", "", "2011-02-21T15:51:14.996", "+4321.5");
    insertClientAccountTransactionTemp("null", "57743KZ372-07560-15462-1553047", "2011-02-21T15:51:26.996", "+0");
    insertClientAccountTransactionTemp("Отмывание на компьютерной технике", "57743KZ372-07560-15462-1553047", "2011-02-21T15:51:25.996", "-3732");

    //
    //
    frsMigration.checkForValidness();
    //
    //

    List<Integer> statuses = migrationTestDao.get().getTransactionsWithoutAccounts();

    assertThat(statuses).hasSize(1);

    for (Integer status : statuses) {
      assertThat(status).isEqualTo(2);
    }
  }


  @Test
  public void validateAndMigrateData_insert_account() throws Exception {

    this.prepareTempTables();

    Character character = insertCharacter("Character for insert account");
    character.id = migrationTestDao.get().getCharmByName(character.name).id;

    Client client = insertClient(111, character.id, "1-A69-ia-PJ-G6hRzbEf2W");

    ClientAccountTemp clientAccountTemp = insertClientAccountTemp(client.migration_id, "19382KZ865-20725-11111-8267359", "2001-02-21 15:51:14");

    //
    //
    frsMigration.validateAndMigrateData();
    //
    //

    ClientAccount account = migrationTestDao.get().getAccountByNumber(clientAccountTemp.accountNumber);

    Timestamp registeredAt = getTimestampFromString(clientAccountTemp.registeredAt);

    assertThat(account.client).isEqualTo(client.id);
    assertThat(account.number).isEqualTo(clientAccountTemp.accountNumber);
    assertThat(account.registeredAt).isEqualTo(registeredAt);
  }

  @Test
  public void validateAndMigrateData_insert_account_no_client() throws Exception {

    this.prepareTempTables();

    ClientAccountTemp clientAccountTemp = insertClientAccountTemp("", "19382KZ865-20725-33333-8267359", "2001-02-21 15:51:14");

    //
    //
    frsMigration.validateAndMigrateData();
    //
    //

    ClientAccount account = migrationTestDao.get().getAccountByNumber(clientAccountTemp.accountNumber);

    assertThat(account).isNull();
  }

  @Test
  public void validateAndMigrateData_duplicate_account() throws Exception {

    this.prepareTempTables();

    Character character = insertCharacter("Character for duplicate account");
    character.id = migrationTestDao.get().getCharmByName(character.name).id;

    Client client = insertClient(222, character.id, "1-A69-ua-PJ-G6hRzbEf2W");

    ClientAccountTemp clientAccountTemp1 = insertClientAccountTemp(client.migration_id, "19382KZ865-20725-22222-8267359", "2001-02-21 15:51:14");
    ClientAccountTemp clientAccountTemp2 = insertClientAccountTemp(client.migration_id, "19382KZ865-20725-22222-8267359", "2001-02-21 15:51:44");

    //
    //
    frsMigration.validateAndMigrateData();
    //
    //

    ClientAccount account = migrationTestDao.get().getAccountByNumber(clientAccountTemp2.accountNumber);

    Timestamp registeredAt = getTimestampFromString(clientAccountTemp1.registeredAt);

    assertThat(account.client).isEqualTo(client.id);
    assertThat(account.number).isEqualTo(clientAccountTemp1.accountNumber);
    assertThat(account.registeredAt).isEqualTo(registeredAt);
  }


  @Test
  public void validateAndMigrateData_insert_transaction() throws Exception {

    this.prepareTempTables();

    Character character = insertCharacter("Character for insert transaction");
    character.id = migrationTestDao.get().getCharmByName(character.name).id;

    Client client = insertClient(333, character.id, "1-A69-it-PJ-G6hRzbEf2W");

    ClientAccountTemp accountTemp = insertClientAccountTemp(client.migration_id, "19382KZ865-20725-44444-8267359", "2001-02-21 15:51:14");

    ClientAccountTransactionTemp transactionTemp = insertClientAccountTransactionTemp("Переводик в офшоры", accountTemp.accountNumber, "2011-02-21 15:51:16", "-4321.5");

    //
    //
    frsMigration.validateAndMigrateData();
    //
    //

    ClientAccount account = migrationTestDao.get().getAccountByNumber(accountTemp.accountNumber);
    ClientAccountTransaction transaction = migrationTestDao.get().getTransaction(Double.parseDouble(transactionTemp.money), transactionTemp.finishedAt, account.id);
    TransactionType transactionType = migrationTestDao.get().getTransactionTypeById(transaction.type);

    Timestamp finishedAt = getTimestampFromString(transactionTemp.finishedAt);

    assertThat(transaction.money).isEqualTo(Double.parseDouble(transactionTemp.money));
    assertThat(transaction.finishedAt).isEqualTo(finishedAt);
    assertThat(transaction.account).isEqualTo(account.id);
    assertThat(transactionType.name).isEqualTo(transactionTemp.transactionType);
  }

  @Test
  public void validateAndMigrateData_insert_transaction_no_account() throws Exception {

    this.prepareTempTables();

    ClientAccountTransactionTemp transactionTemp = insertClientAccountTransactionTemp("Переводииик в никуда", "19382KZ865-20725-55555-8267359", "2011-02-21 15:51:16", "-4321.5");

    //
    //
    frsMigration.validateAndMigrateData();
    //
    //

    ClientAccount account = migrationTestDao.get().getAccountByNumber(transactionTemp.accountNumber);

    assertThat(account).isNull();
  }

  @Test
  public void validateAndMigrateData_duplicate_transaction() throws Exception {

    this.prepareTempTables();

    Character character = insertCharacter("Character for duplicate transaction");
    character.id = migrationTestDao.get().getCharmByName(character.name).id;

    Client client = insertClient(888, character.id, "1-A69-ut-PJ-G6hRzbEf2W");

    ClientAccountTemp accountTemp = insertClientAccountTemp(client.migration_id, "19382KZ865-20725-66666-8267359", "2001-02-21 15:51:14");

    ClientAccountTransactionTemp transactionTemp1 = insertClientAccountTransactionTemp("Перевод", accountTemp.accountNumber, "2011-02-21 15:51:16", "-4321.5");
    ClientAccountTransactionTemp transactionTemp2 = insertClientAccountTransactionTemp("Перевод", accountTemp.accountNumber, "2011-02-21 15:51:16", "-4321.5");

    //
    //
    frsMigration.validateAndMigrateData();
    //
    //

    ClientAccount account = migrationTestDao.get().getAccountByNumber(accountTemp.accountNumber);
    ClientAccountTransaction transaction = migrationTestDao.get().getTransaction(Double.parseDouble(transactionTemp2.money), transactionTemp2.finishedAt, account.id);
    TransactionType transactionType = migrationTestDao.get().getTransactionTypeById(transaction.type);

    Timestamp finishedAt = getTimestampFromString(transactionTemp1.finishedAt);

    assertThat(transaction.money).isEqualTo(Double.parseDouble(transactionTemp1.money));
    assertThat(transaction.finishedAt).isEqualTo(finishedAt);
    assertThat(transaction.account).isEqualTo(account.id);
    assertThat(transactionType.name).isEqualTo(transactionTemp1.transactionType);
  }


  @Test
  public void checkForLateUpdates_account_no_client() throws Exception {

    this.prepareTempTables();

    Timestamp registeredAt = getTimestampFromString("2001-02-21 10:10:10");
    ClientAccount account = insertClientAccount(null, 0, "19382KZ865-20725-12312-8267359", registeredAt, 0, "1-A69-cua-PJ-G6hRzbEf2W");

    Character character = insertCharacter("Character for late update account");
    character.id = migrationTestDao.get().getCharmByName(character.name).id;

    Client client = insertClient(12312, character.id, account.migrationClient);
    client = migrationTestDao.get().getClientByMigrationId(client.migration_id);

    //
    //
    frsMigration.checkForLateUpdates();
    //
    //

    account = migrationTestDao.get().getAccountByNumber(account.number);

    assertThat(account.client).isEqualTo(client.id);
    assertThat(account.number).isEqualTo(account.number);
    assertThat(account.actual).isEqualTo(1);
  }

  @Test
  public void checkForLateUpdates_transaction_no_account() throws Exception {

    this.prepareTempTables();

    TransactionType transactionType = insertTransactionType("Перевод в никуда");
    transactionType = migrationTestDao.get().getTransactionTypeByName(transactionType.name);

    Timestamp finishedAt = getTimestampFromString("2011-02-21 11:11:11");
    ClientAccountTransaction transaction = insertClientAccountTransaction(null, Double.parseDouble("-4321.5"), finishedAt, transactionType.id, 0, "19382KZ865-20725-32132-8267359");

    Character character = insertCharacter("Character for late update transaction");
    character.id = migrationTestDao.get().getCharmByName(character.name).id;

    Client client = insertClient(32132, character.id, "1-A69-cut-PJ-G6hRzbEf2W");
    client = migrationTestDao.get().getClientByMigrationId(client.migration_id);

    Timestamp registeredAt = getTimestampFromString("2001-02-21 09:09:09");
    ClientAccount account = insertClientAccount(client.id, 0, transaction.migrationAccount, registeredAt, 1, client.migration_id);

    //
    //
    frsMigration.checkForLateUpdates();
    //
    //

    account = migrationTestDao.get().getAccountByNumber(account.number);
    transaction = migrationTestDao.get().getTransaction(transaction.money, "2011-02-21 11:11:11", account.id);

    assertThat(transaction.money).isEqualTo(transaction.money);
    assertThat(transaction.finishedAt).isEqualTo(transaction.finishedAt);
    assertThat(transaction.account).isEqualTo(account.id);
    assertThat(transaction.actual).isEqualTo(1);
  }


  @Test
  public void validateAndMigrateData() throws Exception {

    this.prepareTempTables();

    String fileName = "from_frs_2018-02-21-154543-1-30010.json_row";
    ftp = migrationRegister.get().getFtpConnection();
    String filePath = String.format("%s/%s", migrationConfig.get().ftpTestPath(), fileName);
    InputStream frsStream = ftp.retrieveFileStream(filePath);

    Character character = insertCharacter("Character for full test");
    character.id = migrationTestDao.get().getCharmByName(character.name).id;

    Client client = insertClient(999, character.id, "7-A69-ff-PJ-G6hRzbEf2W");

    //
    //
    frsMigration = new FrsMigrationImpl(migrationRegister.get().getConnection(), frsStream, filePath);
    frsMigration.parseAndFillData();
    frsMigration.checkForValidness();
    frsMigration.validateAndMigrateData();
    frsMigration.checkForLateUpdates();
    //
    //

    String accountNumber1 = "19382KZ865-20725-77777-8267359";
    ClientAccount account1 = migrationTestDao.get().getAccountByNumber(accountNumber1);

    Timestamp registeredAt = getTimestampFromString("2001-02-21 15:51:14");

    assertThat(account1.client).isEqualTo(client.id);
    assertThat(account1.number).isEqualTo(accountNumber1);
    assertThat(account1.registeredAt).isEqualTo(registeredAt);


    String accountNumber2 = "19382KZ865-20725-88888-8267359";
    ClientAccount account2 = migrationTestDao.get().getAccountByNumber(accountNumber2);

    assertThat(account2).isNull();

    ClientAccountTransaction transaction1 = migrationTestDao.get().getTransaction(-4321.5, "2011-02-21T15:51:16", account1.id);
    TransactionType transactionType = migrationTestDao.get().getTransactionTypeById(transaction1.type);

    Timestamp finishedAt = getTimestampFromString("2011-02-21 15:51:16");

    assertThat(transaction1.money).isEqualTo(-4321.5);
    assertThat(transaction1.finishedAt).isEqualTo(finishedAt);
    assertThat(transaction1.account).isEqualTo(account1.id);
    assertThat(transactionType.name).isEqualTo("Перевод в оффтоп");

    ClientAccountTransaction transaction2 = migrationTestDao.get().getTransaction(100100.5, "2011-02-21T15:51:16", account1.id);

    assertThat(transaction2).isNull();
  }
}