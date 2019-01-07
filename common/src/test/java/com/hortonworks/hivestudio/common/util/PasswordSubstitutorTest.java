package com.hortonworks.hivestudio.common.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class PasswordSubstitutorTest {

  protected static String JSON_FILE_PATH = "src/test/resources/das-app.json";

  @Test
  public void replace() throws Exception {
    PasswordSubstitutor passwordSubstitutor = new PasswordSubstitutor();

    String jsonString = FileUtils.readFileToString(new File(JSON_FILE_PATH));
    String credentialProviderPath = CredentialProviderTest.getCredentialProviderPath();
    jsonString = jsonString.replace("{{das_credential_provider_paths}}", credentialProviderPath);

    jsonString = passwordSubstitutor.replace(jsonString);

    ObjectMapper mapper = new ObjectMapper();
    JsonNode json = mapper.readTree(jsonString);

    Assert.assertEquals("testDBPswd", json.path("database").path("password").asText());
    Assert.assertEquals("testWebappPswd", json.path("server").path("applicationConnectors").get(0).get("keyStorePassword").asText());
    Assert.assertEquals("testWebappPswd", json.path("server").path("adminConnectors").get(0).get("keyStorePassword").asText());
    Assert.assertEquals("", json.path("noPasswordAlias").asText());
  }

}