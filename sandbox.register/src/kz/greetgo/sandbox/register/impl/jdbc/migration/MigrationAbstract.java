package kz.greetgo.sandbox.register.impl.jdbc.migration;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;

@SuppressWarnings("WeakerAccess")
public abstract class MigrationAbstract {

  public Connection connection;
  public InputStream stream;
  public String fileName;
  public FTPClient ftp;

  final static Logger logger = Logger.getLogger("kz.greetgo.sandbox.register.impl.jdbc.migration.MigrationAbstract");

  public MigrationAbstract(Connection connection) {
    this.connection = connection;
  }

  public MigrationAbstract(Connection connection, InputStream stream, String fileName) {
    this.connection = connection;
    this.stream = stream;
    this.fileName = fileName;
  }

  public MigrationAbstract(Connection connection, InputStream stream, String fileName, FTPClient ftp) {
    this.connection = connection;
    this.stream = stream;
    this.fileName = fileName;
    this.ftp = ftp;
  }

  public void migrate() throws Exception {

    Instant startTime = Instant.now();

    if (logger.isInfoEnabled()) {
      logger.info(String.format("Started migrating file - %s!", fileName));
    }

    dropTemplateTables();

    createTempTables();

    parseAndFillData();

    checkForValidness();

    validateAndMigrateData();

    dropTemplateTables();

    checkForLateUpdates();

    Instant endTime = Instant.now();
    Duration timeSpent = Duration.between(startTime, endTime);

    if (logger.isInfoEnabled()) {
      logger.info(String.format("Ended migrating file - %s! Time taken: %s milliseconds", fileName, timeSpent.toMillis()));
    }
  }

  public abstract void createTempTables() throws Exception;

  public abstract void parseAndFillData() throws Exception;

  public abstract void checkForValidness() throws Exception;

  public abstract void validateAndMigrateData() throws Exception;

  public abstract void dropTemplateTables() throws Exception;

  public abstract void checkForLateUpdates() throws Exception;

  public void executeStatement(String statement) throws Exception {
    try (PreparedStatement ps = connection.prepareStatement(statement)) {
      ps.executeUpdate();
    }
  }
}
