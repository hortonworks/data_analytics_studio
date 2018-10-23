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

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Class to download a http resource into a given path.
 */
public class HttpArtifact implements Artifact {

  private final HttpClient client;
  private final String name;
  private final String url;
  private final boolean isTemp;

  public HttpArtifact(HttpClient client, String name, String url, boolean isTemp) {
    this.client = client;
    this.name = name;
    this.url = url;
    this.isTemp = isTemp;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void downloadInto(Path path) throws ArtifactDownloadException {
    // Try to use nio to transfer the streaming data from http into the outputstream of the path.
    // We are using 3 buffers while with nio we should be able to download with one.
    InputStream entityStream = null;
    try {
      HttpGet httpGet = new HttpGet(url);
      HttpResponse response = client.execute(httpGet);
      entityStream = response.getEntity().getContent();
      Files.copy(entityStream, path);
    } catch (IOException e) {
      throw new ArtifactDownloadException(e);
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
