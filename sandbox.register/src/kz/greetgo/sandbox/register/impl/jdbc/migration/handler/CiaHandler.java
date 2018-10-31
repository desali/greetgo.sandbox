package kz.greetgo.sandbox.register.impl.jdbc.migration.handler;

import kz.greetgo.sandbox.register.impl.jdbc.migration.model.CiaAddress;
import kz.greetgo.sandbox.register.impl.jdbc.migration.model.CiaClient;
import kz.greetgo.sandbox.register.impl.jdbc.migration.model.CiaPhone;
import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("WeakerAccess,SqlResolve")
public class CiaHandler extends DefaultHandler {

  final static Logger logger = Logger.getLogger("kz.greetgo.sandbox.register.impl.jdbc.migration.handler.CiaHandler");

  public Connection connection;
  public CiaClient client = null;
  public List<CiaPhone> phones = new ArrayList<>();
  public List<CiaAddress> addresses = new ArrayList<>();

  public String currentTag = "";

  public Integer clientCount = 0;
  public Integer addressCount = 0;
  public Integer phoneCount = 0;

  public PreparedStatement clientInsertPS;
  public PreparedStatement phoneInsertPS;
  public PreparedStatement addressInsertPS;

  public final int MAX_BATCH_SIZE = 50000;

  public int clientBatchCount = 0;
  public int phoneBatchCount = 0;
  public int addressBatchCount = 0;

  public int phoneMigrationOrder = 0;
  public int addressMigrationOrder = 0;

  public CiaHandler(Connection con) throws Exception {
    this.connection = con;
    initPreparedStatements();
  }

  @Override
  public void startElement(String uri, String localName, String qName, Attributes attributes) {

    // FIXME: 30.10.18 Лучше сделать aName.toLowerCase и вместо equalsIgnoreCase использовать equals или switch
    switch (qName) {
      case "cia":
        currentTag = "cia";
        break;
      case "client":
        if (currentTag.equals("cia")) {
          currentTag = "client";

          client = new CiaClient();
          client.setId(attributes.getValue("id"));
        }
        break;
      case "surname":
        if (currentTag.equals("client")) {
          client.setSurname(attributes.getValue("value"));
        }
        break;
      case "name":
        // FIXME: 30.10.18 а если встретиться name в другом тэге и это будет не имя клиента а что-нибудь другое
        if (currentTag.equals("client")) {
          client.setName(attributes.getValue("value"));
        }
        break;
      case "patronymic":
        if (currentTag.equals("client")) {
          client.setPatronymic(attributes.getValue("value"));
        }
        break;
      case "gender":
        if (currentTag.equals("client")) {
          client.setGender(attributes.getValue("value"));
        }
        break;
      case "charm":
        if (currentTag.equals("client")) {
          client.setCharm(attributes.getValue("value"));
        }
        break;
      case "birth":
        if (currentTag.equals("client")) {
          client.setBirthDate(attributes.getValue("value"));
        }
        break;
      case "homePhone":
        if (currentTag.equals("client")) {
          currentTag = "homePhone";
        }
        break;
      case "mobilePhone":
        if (currentTag.equals("client")) {
          currentTag = "mobilePhone";
        }
        break;
      case "workPhone":
        if (currentTag.equals("client")) {
          currentTag = "workPhone";
        }
        break;
      case "address":
        if (currentTag.equals("client")) {
          currentTag = "address";
        }
        break;
      case "fact": {
        if (currentTag.equals("address")) {
          CiaAddress address = new CiaAddress();
          address.client = client.getId();
          address.type = "FACT";
          address.street = attributes.getValue("street");
          address.house = attributes.getValue("house");
          address.flat = attributes.getValue("flat");

          addresses.add(address);
        }
        break;
      }
      case "register": {
        if (currentTag.equals("address")) {
          CiaAddress address = new CiaAddress();
          address.client = client.getId();
          address.type = "REG";
          address.street = attributes.getValue("street");
          address.house = attributes.getValue("house");
          address.flat = attributes.getValue("flat");

          addresses.add(address);
        }
        break;
      }
    }
  }

  @Override
  public void endElement(String uri, String localName, String qName) {
    qName = qName.toLowerCase();

    switch (qName) {
      case "cia":
        try {
          executeLeftBatches();
        } catch (Exception e) {
          logger.error("ERROR", e);
          // FIXME: 30.10.18 нельзя глотать ошибку
        }
        break;
      case "client":
        currentTag = "cia";
        try {
          insertClient(client);

          for (CiaPhone phone : phones) {
            insertClientPhone(phone, phoneMigrationOrder);
          }

          phoneMigrationOrder++;
          phones = new ArrayList<>();

          for (CiaAddress address : addresses) {
            insertClientAddress(address, addressMigrationOrder);
          }

          addressMigrationOrder++;
          addresses = new ArrayList<>();
          client = new CiaClient();
        } catch (Exception e) {
          logger.error("ERROR", e);
        }
        break;
      case "address":
        currentTag = "client";
        break;
    }
  }

  @Override
  public void characters(char[] ch, int start, int length) {
    switch (currentTag) {
      case "homePhone": {
        CiaPhone phone = new CiaPhone();
        phone.client = client.getId();
        phone.type = "HOME";
        phone.number = new String(ch, start, length);

        phones.add(phone);
        currentTag = "client";
        break;
      }
      case "mobilePhone": {
        CiaPhone phone = new CiaPhone();
        phone.client = client.getId();
        phone.type = "MOBILE";
        phone.number = new String(ch, start, length);

        phones.add(phone);
        currentTag = "client";
        break;
      }
      case "workPhone": {
        CiaPhone phone = new CiaPhone();
        phone.client = client.getId();
        phone.type = "WORK";
        phone.number = new String(ch, start, length);

        phones.add(phone);
        currentTag = "client";
        break;
      }
    }
  }

  private void insertClient(CiaClient client) throws Exception {

    clientInsertPS.setObject(1, client.id);
    clientInsertPS.setObject(2, client.surname);
    clientInsertPS.setObject(3, client.name);
    clientInsertPS.setObject(4, client.patronymic);
    clientInsertPS.setObject(5, client.gender);
    clientInsertPS.setObject(6, client.birthDate);
    clientInsertPS.setObject(7, client.charm);

    clientInsertPS.addBatch();

    if (clientBatchCount == MAX_BATCH_SIZE) {
      clientInsertPS.executeBatch();
      connection.commit();
      clientBatchCount = 0;
    }

    clientBatchCount++;
    clientCount++;
  }

  private void insertClientAddress(CiaAddress address, int migrationOrder) throws Exception {

    addressInsertPS.setObject(1, address.client);
    addressInsertPS.setObject(2, address.type);
    addressInsertPS.setObject(3, address.street);
    addressInsertPS.setObject(4, address.house);
    addressInsertPS.setObject(5, address.flat);
    addressInsertPS.setObject(6, migrationOrder);

    addressInsertPS.addBatch();

    if (addressBatchCount == MAX_BATCH_SIZE) {
      addressInsertPS.executeBatch();
      connection.commit();
      addressBatchCount = 0;
    }

    addressBatchCount++;
    addressCount++;
  }

  private void insertClientPhone(CiaPhone phone, int migrationOrder) throws Exception {

    phoneInsertPS.setObject(1, phone.client);
    phoneInsertPS.setObject(2, phone.type);
    phoneInsertPS.setObject(3, phone.number);
    phoneInsertPS.setObject(4, migrationOrder);

    phoneInsertPS.addBatch();

    if (phoneBatchCount == MAX_BATCH_SIZE) {
      phoneInsertPS.executeBatch();
      connection.commit();
      phoneBatchCount = 0;
    }

    phoneBatchCount++;
    phoneCount++;
  }


  private void initPreparedStatements() throws Exception {
    String clientTempTableInsert =
      "insert into client_temp (id, surname, name, patronymic, gender, birth_date, charm, migration_order) " +
        " values (?, ?, ?, ?, ?, ?, ?, nextval('migration_order'))";

    String clientAddressTempTableInsert =
      "insert into client_addr_temp (client, type, street, house, flat, migration_order) " +
        " values (?, ?, ?, ?, ?, ?)";

    String clientPhoneTempTableInsert =
      "insert into client_phone_temp (client, type, number, migration_order) " +
        " values (?, ?, ?, ?)";

    clientInsertPS = connection.prepareStatement(clientTempTableInsert);
    addressInsertPS = connection.prepareStatement(clientAddressTempTableInsert);
    phoneInsertPS = connection.prepareStatement(clientPhoneTempTableInsert);
  }

  private void executeLeftBatches() throws Exception {
    clientInsertPS.executeBatch();
    connection.commit();
    addressInsertPS.executeBatch();
    connection.commit();
    phoneInsertPS.executeBatch();
    connection.commit();
  }
}
