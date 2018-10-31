package kz.greetgo.sandbox.register.impl;

import kz.greetgo.depinject.core.Bean;
import kz.greetgo.depinject.core.BeanGetter;
import kz.greetgo.sandbox.controller.register.MigrationRegister;
import kz.greetgo.sandbox.register.configs.DbConfig;
import kz.greetgo.sandbox.register.configs.MigrationConfig;
import kz.greetgo.sandbox.register.impl.jdbc.migration.CiaMigrationImpl;
import kz.greetgo.sandbox.register.impl.jdbc.migration.FrsMigrationImpl;
import kz.greetgo.sandbox.register.util.JdbcSandbox;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;

@SuppressWarnings("WeakerAccess")
@Bean
public class MigrationRegisterImpl implements MigrationRegister {

  public BeanGetter<JdbcSandbox> jdbc;
  public BeanGetter<MigrationConfig> migrationConfig;
  public BeanGetter<DbConfig> dbConfig;

  public FTPClient ftp;

  @Override
  public void migrate() throws Exception {
    ftp = getFtpConnection();

    ArrayList<FTPFile> ciaFiles = getCiaMigrationFiles(migrationConfig.get().ftpRealPath());
    ArrayList<FTPFile> frsFiles = getFrsMigrationFiles(migrationConfig.get().ftpRealPath());

    while (ciaFiles.size() > 0 || frsFiles.size() > 0) {

      if (ciaFiles.size() > 0) {
        ftp = getFtpConnection();

        String curFilePath = String.format("%s/%s", migrationConfig.get().ftpRealPath(), ciaFiles.get(0).getName());

        InputStream stream = ftp.retrieveFileStream(curFilePath);

        CiaMigrationImpl ciaMigration = new CiaMigrationImpl(getConnection(), stream, curFilePath, ftp);
        ciaMigration.migrate();

        ciaFiles.remove(0);
      }

      if (frsFiles.size() > 0) {
        ftp = getFtpConnection();

        String curFilePath = String.format("%s/%s", migrationConfig.get().ftpRealPath(), frsFiles.get(0).getName());

        InputStream stream = ftp.retrieveFileStream(curFilePath);

        FrsMigrationImpl frsMigration = new FrsMigrationImpl(getConnection(), stream, curFilePath, ftp);
        frsMigration.migrate();

        frsFiles.remove(0);
      }
    }
  }

  public Connection getConnection() throws Exception {
    return DriverManager.getConnection(
      dbConfig.get().url(),
      dbConfig.get().username(),
      dbConfig.get().password()
    );
  }

  public FTPClient getFtpConnection() throws Exception {
    FTPClient ftp = new FTPClient();
    ftp.connect(migrationConfig.get().ftpHostname());
    ftp.login(migrationConfig.get().ftpLogin(), migrationConfig.get().ftpPassword());

    return ftp;
  }

  public ArrayList<FTPFile> getCiaMigrationFiles(String path) throws Exception {
    FTPFile[] ftpFiles = ftp.listFiles(path, ftpFile -> (ftpFile.isFile() && ftpFile.getName().endsWith(".xml")));

    return new ArrayList<>(Arrays.asList(ftpFiles));
  }

  public ArrayList<FTPFile> getFrsMigrationFiles(String path) throws Exception {
    FTPFile[] ftpFiles = ftp.listFiles(path, ftpFile -> (ftpFile.isFile() && ftpFile.getName().endsWith(".json_row")));

    return new ArrayList<>(Arrays.asList(ftpFiles));
  }
}
