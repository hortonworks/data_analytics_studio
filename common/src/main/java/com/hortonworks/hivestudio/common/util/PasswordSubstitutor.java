package com.hortonworks.hivestudio.common.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.text.StrSubstitutor;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.HashMap;

@Slf4j
public class PasswordSubstitutor extends StrSubstitutor {

  public String replace(String source) {
    JSONParser parser = new JSONParser();
    try {
      JSONObject json = (JSONObject) parser.parse(source);

      String providerPath = json.get("credentialProviderPath").toString();
      if(!StringUtils.isEmpty(providerPath)) {
        CredentialProvider credentialProvider = new CredentialProvider(providerPath);

        JSONArray passwordAliases = (JSONArray)json.get("passwordAliases");

        HashMap<String, String> valueMap = new HashMap<>();
        for (Object passwordAlias : passwordAliases) {
          String alias = passwordAlias.toString();
          String password = credentialProvider.getPassword(alias);
          password = StringUtils.isEmpty(password) ? "" : password.trim();
          valueMap.put(alias, password);
        }

        source = super.replace(source, valueMap);
      }
    } catch(ParseException | IOException e){
      log.error("Password replace failed", e);
    }
    return source;
  }

}
