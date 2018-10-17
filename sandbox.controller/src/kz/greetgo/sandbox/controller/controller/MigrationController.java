package kz.greetgo.sandbox.controller.controller;

import kz.greetgo.depinject.core.Bean;
import kz.greetgo.depinject.core.BeanGetter;
import kz.greetgo.mvc.annotations.on_methods.ControllerPrefix;
import kz.greetgo.mvc.annotations.on_methods.OnGet;
import kz.greetgo.sandbox.controller.register.MigrationRegister;
import kz.greetgo.sandbox.controller.util.Controller;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

@SuppressWarnings("WeakerAccess")
@Bean
@ControllerPrefix("/migrate")
public class MigrationController implements Controller {
  public BeanGetter<MigrationRegister> migrationRegister;

  @OnGet("")
  public void migrate() throws Exception {
    migrationRegister.get().migrate();
  }
}
