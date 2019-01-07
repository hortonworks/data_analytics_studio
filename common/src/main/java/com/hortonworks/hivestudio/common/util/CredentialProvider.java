package com.hortonworks.hivestudio.common.util;

import com.google.inject.Inject;
import com.hortonworks.hivestudio.common.config.DASDropwizardConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProviderFactory;

import java.io.IOException;

public class CredentialProvider {

  // Apparently this is the best way to create a hadoop JCEKS reader!
  private Configuration jceksReader;

  @Inject
  public CredentialProvider(DASDropwizardConfiguration conf) {
    this(conf.getCredentialProviderPath());
  }

  public CredentialProvider(String path) {
    this.jceksReader = createReader(path);
  }

  private Configuration createReader(String path) {
    Configuration conf = new Configuration();
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, path);
    return conf;
  }

  public String getPassword(String alias) throws IOException {
    String passwd = null;
    char[] pwdCharArray = jceksReader.getPassword(alias);
    if (pwdCharArray != null) {
      passwd = new String(pwdCharArray);
    }
    return passwd;
  }

}
