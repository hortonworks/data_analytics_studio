package com.hortonworks.hivestudio.common.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.hortonworks.hivestudio.common.actor.AkkaFactory;
import io.dropwizard.Configuration;
import io.dropwizard.db.DataSourceFactory;
import lombok.Getter;
import lombok.Setter;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;

@Getter
@Setter
public class DASDropwizardConfiguration extends Configuration {
  @NotNull
  @Valid
  @JsonProperty("database")
  private DataSourceFactory database = null;

  @NotNull
  @Valid
  @JsonProperty("akka")
  private AkkaFactory akkaFactory;

  @Valid
  @NotNull
  private String serviceConfigDirectory = null;

  @Valid
  private AuthConfig authConfig = null;

  @Valid
  private String hiveSessionParams = null;

  @Valid
  private String credentialProviderPath = null;

  @Valid
  private List<String> passwordAliases = null;

}
