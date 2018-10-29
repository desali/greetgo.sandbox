package kz.greetgo.sandbox.register.impl.jdbc.migration;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;

@SuppressWarnings("WeakerAccess")
public abstract class MigrationAbstract {

  public Connection connection;
  public FTPClient ftp;
  public String filePath;

  final static Logger logger = Logger.getLogger("kz.greetgo.sandbox.register.impl.jdbc.migration.MigrationAbstract");

  public MigrationAbstract(Connection connection) {
    this.connection = connection;
  }

  public MigrationAbstract(Connection connection, FTPClient ftp, String filePath) {
    this.connection = connection;
    this.ftp = ftp;
    this.filePath = filePath;
  }

  public void migrate() throws Exception {

    Instant startTime = Instant.now();

    if (logger.isInfoEnabled()) {
      logger.info(String.format("Started migrating file - %s!", filePath));
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
      logger.info(String.format("Ended migrating file - %s! Time taken: %s milliseconds", filePath, timeSpent.toMillis()));
    }
  }

  public abstract void createTempTables() throws Exception;

  public abstract void parseAndFillData() throws Exception;

  public abstract void checkForValidness() throws Exception;

  public abstract void validateAndMigrateData() throws Exception;

  public abstract void dropTemplateTables() throws Exception;

  public abstract void checkForLateUpdates() throws Exception;
}
