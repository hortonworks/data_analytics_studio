/*
 *
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *   LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *   FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *   DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *   DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *   OR LOSS OR CORRUPTION OF DATA.
 *
 */

package com.hortonworks.hivestudio.debugBundler.framework;

import lombok.extern.log4j.Log4j;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.ssl.SSLFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;

/**
 * Class to download a http resource into a given path.
 */
@Log4j
public class HttpArtifact implements Artifact {

  private final String name;
  private final String url;
  private final boolean isTemp;

  private final Configuration hadoopConf;

  public HttpArtifact(Configuration hadoopConf, String name, String url, boolean isTemp) {
    this.name = name;
    this.url = url;
    this.isTemp = isTemp;
    this.hadoopConf = hadoopConf;
  }

  private SSLFactory getSSLFactory() throws GeneralSecurityException, IOException {
    SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, hadoopConf);
    sslFactory.init();
    return sslFactory;
  }

  @Override
  public String getName() {
    return name;
  }

  private InputStream getStream(String urlStr)
      throws IOException, GeneralSecurityException, AuthenticationException {

    URL url = new URL(urlStr);
    SSLFactory sslFactory = null;

    if(url.getProtocol().equals("https")) {
      sslFactory = getSSLFactory();
    }

    AuthenticatedURL authenticatedURL = new AuthenticatedURL(null, sslFactory);

    AuthenticatedURL.Token token = new AuthenticatedURL.Token();
    HttpURLConnection conn = authenticatedURL.openConnection(url, token);

    return conn.getInputStream();
  }

  @Override
  public void downloadInto(Path path) throws ArtifactDownloadException {
    // Try to use nio to transfer the streaming data from http into the outputstream of the path.
    // We are using 3 buffers while with nio we should be able to download with one.
    InputStream entityStream = null;
    try {
      entityStream = getStream(url);
      Files.copy(entityStream, path);
    } catch (IOException | GeneralSecurityException | AuthenticationException e) {
      throw new ArtifactDownloadException("Error downloading from url: " + url, e);
    } finally {
      IOUtils.closeQuietly(entityStream);
    }
  }

  @Override
  public boolean isTemp() {
    return isTemp;
  }

  @Override
  public String toString() {
    return "HttpArtifact[Name: " + name + ", URL: " + url + "]";
  }
}
