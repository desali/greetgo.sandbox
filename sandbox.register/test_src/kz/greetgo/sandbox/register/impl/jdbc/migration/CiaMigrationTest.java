package kz.greetgo.sandbox.register.impl.jdbc.migration;

import kz.greetgo.depinject.core.BeanGetter;
import kz.greetgo.sandbox.register.configs.MigrationConfig;
import kz.greetgo.sandbox.register.dao_model.Client;
import kz.greetgo.sandbox.register.dao_model.ClientAddress;
import kz.greetgo.sandbox.register.dao_model.ClientPhone;
import kz.greetgo.sandbox.register.dao_model.temp.ClientAddressTemp;
import kz.greetgo.sandbox.register.dao_model.temp.ClientPhoneTemp;
import kz.greetgo.sandbox.register.dao_model.temp.ClientTemp;
import kz.greetgo.sandbox.register.impl.MigrationRegisterImpl;
import kz.greetgo.sandbox.register.impl.jdbc.migration.model.CiaAddress;
import kz.greetgo.sandbox.register.impl.jdbc.migration.model.CiaClient;
import kz.greetgo.sandbox.register.impl.jdbc.migration.model.CiaPhone;
import kz.greetgo.sandbox.register.test.dao.MigrationTestDao;
import kz.greetgo.sandbox.register.test.util.ParentTestNg;
import org.apache.commons.net.ftp.FTPClient;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;

@SuppressWarnings("WeakerAccess")
public class CiaMigrationTest extends ParentTestNg {

  public BeanGetter<MigrationTestDao> migrationTestDao;
  public BeanGetter<MigrationRegisterImpl> migrationRegister;
  public BeanGetter<MigrationConfig> migrationConfig;

  public FTPClient ftp;

  private CiaMigrationImpl ciaMigration;


  private void prepareTempTables() throws Exception {
    ciaMigration = new CiaMigrationImpl(migrationRegister.get().getConnection());

    ciaMigration.dropTemplateTables();
    ciaMigration.createTempTables();
  }


  private ClientTemp insertClientTemp(String id, String surname, String name, String patronymic, String gender, String birthDate, String charm) {
    ClientTemp clientTemp = new ClientTemp();
    clientTemp.id = id;
    clientTemp.surname = surname;
    clientTemp.name = name;
    clientTemp.patronymic = patronymic;
    clientTemp.gender = gender;
    clientTemp.birthDate = birthDate;
    clientTemp.charm = charm;
    clientTemp.status = 1;

    migrationTestDao.get().insertClientTemp(clientTemp);

    return clientTemp;
  }

  private ClientAddressTemp insertClientAddressTemp(String client, String type, String street, String house, String flat) {
    ClientAddressTemp clientAddressTemp = new ClientAddressTemp();
    clientAddressTemp.client = client;
    clientAddressTemp.type = type;
    clientAddressTemp.street = street;
    clientAddressTemp.house = house;
    clientAddressTemp.flat = flat;
    clientAddressTemp.status = 1;
    clientAddressTemp.migrationOrder = migrationTestDao.get().getCurrentMigrationOrder();

    migrationTestDao.get().insertClientAddressTemp(clientAddressTemp);

    return clientAddressTemp;
  }

  private ClientPhoneTemp insertClientPhoneTemp(String client, String type, String number) {
    ClientPhoneTemp clientPhoneTemp = new ClientPhoneTemp();
    clientPhoneTemp.client = client;
    clientPhoneTemp.type = type;
    clientPhoneTemp.number = number;
    clientPhoneTemp.status = 1;
    clientPhoneTemp.migrationOrder = migrationTestDao.get().getCurrentMigrationOrder();

    migrationTestDao.get().insertClientPhoneTemp(clientPhoneTemp);

    return clientPhoneTemp;
  }

  @Test
  public void parseAndFillData_client() throws Exception {

    prepareTempTables();

    String fileName = "from_cia_2018-02-21-154532-1-300.xml";
    ftp = migrationRegister.get().getFtpConnection();
    String filePath = String.format("%s/%s", migrationConfig.get().ftpTestPath(), fileName);
    InputStream ciaStream = ftp.retrieveFileStream(filePath);


    //
    //
    ciaMigration = new CiaMigrationImpl(migrationRegister.get().getConnection(), ciaStream, filePath);
    ciaMigration.parseAndFillData();
    //
    //

    List<CiaClient> clients = migrationTestDao.get().getClients();

    assertThat(clients).hasSize(4);

    assertThat(clients.get(0).id).isEqualTo("0-B9N-HT-PU-04wolRBPzj");
    assertThat(clients.get(0).surname).isEqualTo("");
    assertThat(clients.get(0).name).isEqualTo("WCИTБЯ7щАо");
    assertThat(clients.get(0).patronymic.trim()).isEqualTo("");
    assertThat(clients.get(0).gender).isEqualTo("FEMALE");
    assertThat(clients.get(0).birthDate).isEqualTo("1995-07-07");
    assertThat(clients.get(0).charm).isEqualTo("ЩlВOpФpЪИШ");

    assertThat(clients.get(1).id).isEqualTo("0-B9N-HT-PU-04wolRBPzj");
    assertThat(clients.get(1).surname).isEqualTo("ЕDфзEуЧь57");
    assertThat(clients.get(1).name).isEqualTo("NIfТDтуЯkТ");
    assertThat(clients.get(1).patronymic).isEqualTo("УДтЮцКp5ЁЛайl");
    assertThat(clients.get(1).gender).isEqualTo("MALE");
    assertThat(clients.get(1).birthDate).isEqualTo("1966-03-10");
    assertThat(clients.get(1).charm).isEqualTo("БОШХы5UЪМЗ");

    assertThat(clients.get(2).id).isEqualTo("2-MFR-HN-2X-LPT6Ex2SPW");
    assertThat(clients.get(2).surname).isEqualTo("ФAcgsHZЪкp");
    assertThat(clients.get(2).name).isEqualTo("2вцнwкПnУи");
    assertThat(clients.get(2).patronymic).isEqualTo("ЁWЛ2я1КЬОLAФъ");
    assertThat(clients.get(2).gender).isEqualTo("MALE");
    assertThat(clients.get(2).birthDate).isEqualTo("1941-07-23");
    assertThat(clients.get(2).charm).isEqualTo("пlJХёsъСAШ");

    assertThat(clients.get(3).id).isEqualTo("8-3K0-1Q-NP-8EPS3lmuxb");
    assertThat(clients.get(3).surname).isEqualTo("цWбш0мVцdТ");
    assertThat(clients.get(3).name).isEqualTo("");
    assertThat(clients.get(3).patronymic).isEqualTo("Sq88КцЪE3ИЗyу");
    assertThat(clients.get(3).gender).isEqualTo("MALE");
    assertThat(clients.get(3).birthDate).isEqualTo("5359-08-16");
    assertThat(clients.get(3).charm).isEqualTo("ЪЧйLGШmАЙГ");
  }

  @Test
  public void parseAndFillData_client_phone() throws Exception {

    prepareTempTables();

    String fileName = "from_cia_2018-02-21-154532-1-300.xml";
    ftp = migrationRegister.get().getFtpConnection();
    String filePath = String.format("%s/%s", migrationConfig.get().ftpTestPath(), fileName);
    InputStream ciaStream = ftp.retrieveFileStream(filePath);

    //
    //
    ciaMigration = new CiaMigrationImpl(migrationRegister.get().getConnection(), ciaStream, filePath);
    ciaMigration.parseAndFillData();
    //
    //

    List<CiaPhone> phones = migrationTestDao.get().getClientPhones();

    assertThat(phones).hasSize(19);

    assertThat(phones.get(0).type).isEqualTo("WORK");
    assertThat(phones.get(0).client).isEqualTo("0-B9N-HT-PU-04wolRBPzj");
    assertThat(phones.get(0).number).isEqualTo("+7-385-253-53-56");

    assertThat(phones.get(1).type).isEqualTo("HOME");
    assertThat(phones.get(1).client).isEqualTo("0-B9N-HT-PU-04wolRBPzj");
    assertThat(phones.get(1).number).isEqualTo("+7-878-241-63-94");

    assertThat(phones.get(18).type).isEqualTo("WORK");
    assertThat(phones.get(18).client).isEqualTo("8-3K0-1Q-NP-8EPS3lmuxb");
    assertThat(phones.get(18).number).isEqualTo("+7-653-288-86-15");
  }

  @Test
  public void parseAndFillData_client_address() throws Exception {

    prepareTempTables();

    String fileName = "from_cia_2018-02-21-154532-1-300.xml";
    ftp = migrationRegister.get().getFtpConnection();
    String filePath = String.format("%s/%s", migrationConfig.get().ftpTestPath(), fileName);
    InputStream ciaStream = ftp.retrieveFileStream(filePath);

    //
    //
    ciaMigration = new CiaMigrationImpl(migrationRegister.get().getConnection(), ciaStream, filePath);
    ciaMigration.parseAndFillData();
    //
    //

    List<CiaAddress> addresses = migrationTestDao.get().getClientAddresses();

    assertThat(addresses).hasSize(8);

    assertThat(addresses.get(0).type).isEqualTo("FACT");
    assertThat(addresses.get(0).client).isEqualTo("0-B9N-HT-PU-04wolRBPzj");
    assertThat(addresses.get(0).street).isEqualTo("RцВWAаEkMкёнkOзДfжГк");
    assertThat(addresses.get(0).house).isEqualTo("RП");
    assertThat(addresses.get(0).flat).isEqualTo("hИ");

    assertThat(addresses.get(1).type).isEqualTo("REG");
    assertThat(addresses.get(1).client).isEqualTo("0-B9N-HT-PU-04wolRBPzj");
    assertThat(addresses.get(1).street).isEqualTo("ХfАИKлFщсiхДЗрPгWЗdЭ");
    assertThat(addresses.get(1).house).isEqualTo("оz");
    assertThat(addresses.get(1).flat).isEqualTo("РБ");

    assertThat(addresses.get(7).type).isEqualTo("REG");
    assertThat(addresses.get(7).client).isEqualTo("8-3K0-1Q-NP-8EPS3lmuxb");
    assertThat(addresses.get(7).street).isEqualTo("9BЧфдРЯКЪжlЭйгйFХ3ёи");
    assertThat(addresses.get(7).house).isEqualTo("Eщ");
    assertThat(addresses.get(7).flat).isEqualTo("дЙ");
  }


  @Test
  public void checkForValidness_client_surname() throws Exception {

    prepareTempTables();

    insertClientTemp("0-B9N-HT-PU-04wolRBPzj", "", "WCИTБЯ7щАо", "", "FEMALE", "1995-07-07", "ЩlВOpФpЪИШ");
    insertClientTemp("0-B9N-HT-PU-04wolRBPzj", "ЕDфзEуЧь57", "NIfТDтуЯkТ", "УДтЮцКp5ЁЛайl", "MALE", "1966-03-10", "БОШХы5UЪМЗ");
    insertClientTemp("2-MFR-HN-2X-LPT6Ex2SPW", "ФAcgsHZЪкp", "2вцнwкПnУи", "ЁWЛ2я1КЬОLAФъ", "MALE", "1941-07-23", "пlJХёsъСAШ");
    insertClientTemp("8-3K0-1Q-NP-8EPS3lmuxb", "цWбш0мVцdТ", "", "Sq88КцЪE3ИЗyу", "MALE", "5359-08-16", "ЪЧйLGШmАЙГ");

    //
    //
    ciaMigration.checkForValidness();
    //
    //

    List<Integer> statuses = migrationTestDao.get().getClientsWithoutSurname();

    assertThat(statuses).hasSize(1);

    for (Integer status : statuses) {
      assertThat(status).isEqualTo(2);
    }
  }

  @Test
  public void checkForValidness_client_name() throws Exception {

    prepareTempTables();

    insertClientTemp("0-B9N-HT-PU-04wolRBPzj", "", "WCИTБЯ7щАо", "", "FEMALE", "1995-07-07", "ЩlВOpФpЪИШ");
    insertClientTemp("0-B9N-HT-PU-04wolRBPzj", "ЕDфзEуЧь57", "NIfТDтуЯkТ", "УДтЮцКp5ЁЛайl", "MALE", "1966-03-10", "БОШХы5UЪМЗ");
    insertClientTemp("2-MFR-HN-2X-LPT6Ex2SPW", "ФAcgsHZЪкp", "2вцнwкПnУи", "ЁWЛ2я1КЬОLAФъ", "MALE", "1941-07-23", "пlJХёsъСAШ");
    insertClientTemp("8-3K0-1Q-NP-8EPS3lmuxb", "цWбш0мVцdТ", "", "Sq88КцЪE3ИЗyу", "MALE", "5359-08-16", "ЪЧйLGШmАЙГ");

    //
    //
    ciaMigration.checkForValidness();
    //
    //

    List<Integer> statuses = migrationTestDao.get().getClientsWithoutName();

    assertThat(statuses).hasSize(1);

    for (Integer status : statuses) {
      assertThat(status).isEqualTo(2);
    }
  }

  @Test
  public void checkForValidness_client_birth_date() throws Exception {

    prepareTempTables();

    insertClientTemp("0-B9N-HT-PU-04wolRBPzj", "", "WCИTБЯ7щАо", "", "FEMALE", "1995-07-07", "ЩlВOpФpЪИШ");
    insertClientTemp("0-B9N-HT-PU-04wolRBPzj", "ЕDфзEуЧь57", "NIfТDтуЯkТ", "УДтЮцКp5ЁЛайl", "MALE", "1966-03-10", "БОШХы5UЪМЗ");
    insertClientTemp("2-MFR-HN-2X-LPT6Ex2SPW", "ФAcgsHZЪкp", "2вцнwкПnУи", "ЁWЛ2я1КЬОLAФъ", "MALE", "1941-07-23", "пlJХёsъСAШ");
    insertClientTemp("8-3K0-1Q-NP-8EPS3lmuxb", "цWбш0мVцdТ", "", "Sq88КцЪE3ИЗyу", "MALE", "5359-08-16", "ЪЧйLGШmАЙГ");

    //
    //
    ciaMigration.checkForValidness();
    //
    //

    List<Integer> statuses = migrationTestDao.get().getClientsWithoutBirthDate();

    assertThat(statuses).hasSize(1);

    for (Integer status : statuses) {
      assertThat(status).isEqualTo(2);
    }
  }


  @Test
  public void validateAndMigrateData_insert_client() throws Exception {

    prepareTempTables();

    ClientTemp clientTemp = insertClientTemp("0-B9N-ia-PU-04wolRBPzj", "Лолололо", "WCИTБЯ7щАо", "", "FEMALE", "1995-07-07", "ЩlВOpФpЪИШ");

    //
    //
    ciaMigration.validateAndMigrateData();
    //
    //

    Client client = migrationTestDao.get().getClientByMigrationId(clientTemp.id);
    String charm = migrationTestDao.get().getCharmById(client.charm).name;

    assertThat(client.surname).isEqualTo("Лолололо");
    assertThat(client.name).isEqualTo("WCИTБЯ7щАо");
    assertThat(client.patronymic).isEqualTo("");
    assertThat(charm).isEqualTo("ЩlВOpФpЪИШ");
    assertThat(client.birthDate).isEqualTo("1995-07-07");
    assertThat(client.gender).isEqualTo("FEMALE");
  }

  @Test
  public void validateAndMigrateData_update_duplicate_client() throws Exception {

    prepareTempTables();

    ClientTemp clientTempOld = insertClientTemp("0-B9N-uc-PU-04wolRBPzj", "Лолололо", "WCИTБЯ7щАо", "", "FEMALE", "1995-07-07", "ЩlВOpФpЪИШ");
    ClientTemp clientTempNew = insertClientTemp("0-B9N-uc-PU-04wolRBPzj", "Фамилия", "Имя", "Отчество", "MALE", "2000-10-07", "Характер");

    //
    //
    ciaMigration.validateAndMigrateData();
    //
    //

    Client client = migrationTestDao.get().getClientByMigrationId(clientTempOld.id);
    String charm = migrationTestDao.get().getCharmById(client.charm).name;

    assertThat(client.surname).isEqualTo(clientTempNew.surname);
    assertThat(client.name).isEqualTo(clientTempNew.name);
    assertThat(client.patronymic).isEqualTo(clientTempNew.patronymic);
    assertThat(charm).isEqualTo(clientTempNew.charm);
    assertThat(client.birthDate).isEqualTo(clientTempNew.birthDate);
    assertThat(client.gender).isEqualTo(clientTempNew.gender);
  }


  @Test
  public void validateAndMigrateData_insert_address() throws Exception {

    prepareTempTables();

    ClientTemp clientTemp = insertClientTemp("0-B9N-ia-PU-04wolRBPzj", "Лолололо", "WCИTБЯ7щАо", "", "FEMALE", "1995-07-07", "ЩlВOpФpЪИШ");

    ClientAddressTemp clientAddressTempFact = insertClientAddressTemp(clientTemp.id, "FACT", "Ломоносов", "100", "1");
    ClientAddressTemp clientAddressTempReg = insertClientAddressTemp(clientTemp.id, "REG", "Абая", "12", "2");

    //
    //
    ciaMigration.validateAndMigrateData();
    //
    //

    Client client = migrationTestDao.get().getClientByMigrationId(clientTemp.id);
    List<ClientAddress> addresses = migrationTestDao.get().getClientAddressesById(client.id);

    assertThat(addresses.get(0).type).isEqualTo(clientAddressTempFact.type);
    assertThat(addresses.get(0).street).isEqualTo(clientAddressTempFact.street);
    assertThat(addresses.get(0).house).isEqualTo(clientAddressTempFact.house);
    assertThat(addresses.get(0).flat).isEqualTo(clientAddressTempFact.flat);

    assertThat(addresses.get(1).type).isEqualTo(clientAddressTempReg.type);
    assertThat(addresses.get(1).street).isEqualTo(clientAddressTempReg.street);
    assertThat(addresses.get(1).house).isEqualTo(clientAddressTempReg.house);
    assertThat(addresses.get(1).flat).isEqualTo(clientAddressTempReg.flat);
  }

  @Test
  public void validateAndMigrateData_update_duplicate_address() throws Exception {

    prepareTempTables();

    ClientTemp clientTempOld = insertClientTemp("0-B9N-ua-PU-04wolRBPzj", "Лолололо", "WCИTБЯ7щАо", "", "FEMALE", "1995-07-07", "ЩlВOpФpЪИШ");

    insertClientAddressTemp(clientTempOld.id, "FACT", "Ломоносов", "100", "1");
    insertClientAddressTemp(clientTempOld.id, "REG", "Абая", "12", "2");

    ClientTemp clientTempNew = insertClientTemp("0-B9N-ua-PU-04wolRBPzj", "Лолололо", "WCИTБЯ7щАо", "", "FEMALE", "1995-07-07", "ЩlВOpФpЪИШ");

    ClientAddressTemp clientAddressTempNewFact = insertClientAddressTemp(clientTempNew.id, "FACT", "Новый Факт", "1", "4");
    ClientAddressTemp clientAddressTempNewReg = insertClientAddressTemp(clientTempNew.id, "REG", "Новый Рег", "1", "4");

    //
    //
    ciaMigration.validateAndMigrateData();
    //
    //

    Client client = migrationTestDao.get().getClientByMigrationId(clientTempNew.id);
    List<ClientAddress> addresses = migrationTestDao.get().getClientAddressesById(client.id);

    assertThat(addresses.get(0).type).isEqualTo(clientAddressTempNewFact.type);
    assertThat(addresses.get(0).street).isEqualTo(clientAddressTempNewFact.street);
    assertThat(addresses.get(0).house).isEqualTo(clientAddressTempNewFact.house);
    assertThat(addresses.get(0).flat).isEqualTo(clientAddressTempNewFact.flat);

    assertThat(addresses.get(1).type).isEqualTo(clientAddressTempNewReg.type);
    assertThat(addresses.get(1).street).isEqualTo(clientAddressTempNewReg.street);
    assertThat(addresses.get(1).house).isEqualTo(clientAddressTempNewReg.house);
    assertThat(addresses.get(1).flat).isEqualTo(clientAddressTempNewReg.flat);
  }


  @Test
  public void validateAndMigrateData_insert_phone() throws Exception {

    prepareTempTables();

    ClientTemp clientTemp = insertClientTemp("0-B9N-ip-PU-04wolRBPzj", "Лолололо", "WCИTБЯ7щАо", "", "FEMALE", "1995-07-07", "ЩlВOpФpЪИШ");

    ClientPhoneTemp clientPhoneTempHome1 = insertClientPhoneTemp(clientTemp.id, "HOME", "+7-111-111-14-14");
    ClientPhoneTemp clientPhoneTempMobile1 = insertClientPhoneTemp(clientTemp.id, "WORK", "+7-111-111-13-13");
    ClientPhoneTemp clientPhoneTempWork1 = insertClientPhoneTemp(clientTemp.id, "MOBILE", "+7-111-111-12-12");

    //
    //
    ciaMigration.validateAndMigrateData();
    //
    //

    Client client = migrationTestDao.get().getClientByMigrationId(clientTemp.id);
    List<ClientPhone> phones = migrationTestDao.get().getClientPhonesById(client.id);

    assertThat(phones).hasSize(3);

    assertThat(phones.get(0).type).isEqualTo(clientPhoneTempWork1.type);
    assertThat(phones.get(0).number).isEqualTo(clientPhoneTempWork1.number);

    assertThat(phones.get(1).type).isEqualTo(clientPhoneTempMobile1.type);
    assertThat(phones.get(1).number).isEqualTo(clientPhoneTempMobile1.number);

    assertThat(phones.get(2).type).isEqualTo(clientPhoneTempHome1.type);
    assertThat(phones.get(2).number).isEqualTo(clientPhoneTempHome1.number);
  }

  @Test
  public void validateAndMigrateData_update_phone() throws Exception {

    prepareTempTables();

    ClientTemp clientTempOld = insertClientTemp("0-B9N-up-PU-04wolRBPzj", "Лолололо", "WCИTБЯ7щАо", "", "FEMALE", "1995-07-07", "ЩlВOpФpЪИШ");

    insertClientPhoneTemp(clientTempOld.id, "HOME", "+7-878-241-63-94");
    insertClientPhoneTemp(clientTempOld.id, "WORK", "+7-418-204-55-17");
    insertClientPhoneTemp(clientTempOld.id, "MOBILE", "+7-385-253-53-56");

    ClientTemp clientTempNew = insertClientTemp("0-B9N-up-PU-04wolRBPzj", "Лолололо", "WCИTБЯ7щАо", "", "FEMALE", "1995-07-07", "ЩlВOpФpЪИШ");

    ClientPhoneTemp clientPhoneTempHome1 = insertClientPhoneTemp(clientTempNew.id, "HOME", "+7-222-222-21-21");
    ClientPhoneTemp clientPhoneTempMobile1 = insertClientPhoneTemp(clientTempNew.id, "WORK", "+7-222-222-22-22");

    //
    //
    ciaMigration.validateAndMigrateData();
    //
    //

    Client client = migrationTestDao.get().getClientByMigrationId(clientTempOld.id);
    List<ClientPhone> phones = migrationTestDao.get().getClientPhonesById(client.id);

    assertThat(phones).hasSize(2);

    assertThat(phones.get(0).type).isEqualTo(clientPhoneTempMobile1.type);
    assertThat(phones.get(0).number).isEqualTo(clientPhoneTempMobile1.number);

    assertThat(phones.get(1).type).isEqualTo(clientPhoneTempHome1.type);
    assertThat(phones.get(1).number).isEqualTo(clientPhoneTempHome1.number);
  }


  @Test
  public void validateAndMigrateData() throws Exception {

    prepareTempTables();

    ClientTemp clientTemp1 = insertClientTemp("1-B9N-vv-PU-04wolRBPzj", "Лолололо", "WCИTБЯ7щАо", "", "FEMALE", "1995-07-07", "ЩlВOpФpЪИШ");

    ClientAddressTemp clientAddressTempFact1 = insertClientAddressTemp(clientTemp1.id, "FACT", "RцВWAаEkMкёнkOзДfжГк", "RП", "hИ");
    ClientAddressTemp clientAddressTempReg1 = insertClientAddressTemp(clientTemp1.id, "REG", "ХfАИKлFщсiхДЗрPгWЗdЭ", "оz", "РБ");

    ClientPhoneTemp clientPhoneTempHome1 = insertClientPhoneTemp(clientTemp1.id, "HOME", "+7-111-241-63-94");
    ClientPhoneTemp clientPhoneTempMobile1 = insertClientPhoneTemp(clientTemp1.id, "WORK", "+7-333-204-55-17");
    ClientPhoneTemp clientPhoneTempWork1 = insertClientPhoneTemp(clientTemp1.id, "MOBILE", "+7-222-253-53-56");


    ClientTemp clientTempOld2 = insertClientTemp("2-B9N-vv-PU-04wolRBPzj", "Митяй", "Автор", "Летович", "MALE", "1980-07-07", "Губикат");

    insertClientAddressTemp(clientTempOld2.id, "FACT", "Абая", "2 дом", "2");
    insertClientAddressTemp(clientTempOld2.id, "REG", "Алтынсарие", "2 дом", "10");

    insertClientPhoneTemp(clientTempOld2.id, "HOME", "+7-555-322-33-94");
    insertClientPhoneTemp(clientTempOld2.id, "WORK", "+7-777-232-55-17");

    ClientTemp clientTempNew2 = insertClientTemp("2-B9N-vv-PU-04wolRBPzj", "Фамилия", "Имя", "Отчество", "FEMALE", "1910-10-10", "Характерный");

    ClientAddressTemp clientAddressTempFact3 = insertClientAddressTemp(clientTempNew2.id, "FACT", "Новый аддрес", "100 дом", "12");
    ClientAddressTemp clientAddressTempReg3 = insertClientAddressTemp(clientTempNew2.id, "REG", "Новый аддрес рег", "119", "90");

    ClientPhoneTemp clientPhoneTempHome3 = insertClientPhoneTemp(clientTempNew2.id, "HOME", "+7-111-111-11-11");

    //
    //
    ciaMigration.validateAndMigrateData();
    //
    //

    Client client1 = migrationTestDao.get().getClientByMigrationId(clientTemp1.id);
    List<ClientAddress> addresses1 = migrationTestDao.get().getClientAddressesById(client1.id);
    List<ClientPhone> phones1 = migrationTestDao.get().getClientPhonesById(client1.id);
    String charm1 = migrationTestDao.get().getCharmById(client1.charm).name;

    assertThat(client1.surname).isEqualTo(clientTemp1.surname);
    assertThat(client1.name).isEqualTo(clientTemp1.name);
    assertThat(client1.patronymic).isEqualTo(clientTemp1.patronymic);
    assertThat(charm1).isEqualTo(clientTemp1.charm);
    assertThat(client1.birthDate).isEqualTo(clientTemp1.birthDate);
    assertThat(client1.gender).isEqualTo(clientTemp1.gender);

    assertThat(addresses1).hasSize(2);

    assertThat(addresses1.get(0).type).isEqualTo(clientAddressTempFact1.type);
    assertThat(addresses1.get(0).street).isEqualTo(clientAddressTempFact1.street);
    assertThat(addresses1.get(0).house).isEqualTo(clientAddressTempFact1.house);
    assertThat(addresses1.get(0).flat).isEqualTo(clientAddressTempFact1.flat);

    assertThat(addresses1.get(1).type).isEqualTo(clientAddressTempReg1.type);
    assertThat(addresses1.get(1).street).isEqualTo(clientAddressTempReg1.street);
    assertThat(addresses1.get(1).house).isEqualTo(clientAddressTempReg1.house);
    assertThat(addresses1.get(1).flat).isEqualTo(clientAddressTempReg1.flat);

    assertThat(phones1).hasSize(3);

    assertThat(phones1.get(0).type).isEqualTo(clientPhoneTempWork1.type);
    assertThat(phones1.get(0).number).isEqualTo(clientPhoneTempWork1.number);

    assertThat(phones1.get(1).type).isEqualTo(clientPhoneTempMobile1.type);
    assertThat(phones1.get(1).number).isEqualTo(clientPhoneTempMobile1.number);

    assertThat(phones1.get(2).type).isEqualTo(clientPhoneTempHome1.type);
    assertThat(phones1.get(2).number).isEqualTo(clientPhoneTempHome1.number);


    Client client2 = migrationTestDao.get().getClientByMigrationId(clientTempNew2.id);
    List<ClientAddress> addresses2 = migrationTestDao.get().getClientAddressesById(client2.id);
    List<ClientPhone> phones2 = migrationTestDao.get().getClientPhonesById(client2.id);
    String charm2 = migrationTestDao.get().getCharmById(client2.charm).name;

    assertThat(client2.surname).isEqualTo(clientTempNew2.surname);
    assertThat(client2.name).isEqualTo(clientTempNew2.name);
    assertThat(client2.patronymic).isEqualTo(clientTempNew2.patronymic);
    assertThat(charm2).isEqualTo(clientTempNew2.charm);
    assertThat(client2.birthDate).isEqualTo(clientTempNew2.birthDate);
    assertThat(client2.gender).isEqualTo(clientTempNew2.gender);

    assertThat(addresses2).hasSize(2);

    assertThat(addresses2.get(0).type).isEqualTo(clientAddressTempFact3.type);
    assertThat(addresses2.get(0).street).isEqualTo(clientAddressTempFact3.street);
    assertThat(addresses2.get(0).house).isEqualTo(clientAddressTempFact3.house);
    assertThat(addresses2.get(0).flat).isEqualTo(clientAddressTempFact3.flat);

    assertThat(addresses2.get(1).type).isEqualTo(clientAddressTempReg3.type);
    assertThat(addresses2.get(1).street).isEqualTo(clientAddressTempReg3.street);
    assertThat(addresses2.get(1).house).isEqualTo(clientAddressTempReg3.house);
    assertThat(addresses2.get(1).flat).isEqualTo(clientAddressTempReg3.flat);

    assertThat(phones2).hasSize(1);

    assertThat(phones2.get(0).type).isEqualTo(clientPhoneTempHome3.type);
    assertThat(phones2.get(0).number).isEqualTo(clientPhoneTempHome3.number);
  }
}