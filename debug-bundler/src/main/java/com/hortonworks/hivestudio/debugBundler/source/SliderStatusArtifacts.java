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

package com.hortonworks.hivestudio.debugBundler.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.hortonworks.hivestudio.debugBundler.framework.Artifact;
import com.hortonworks.hivestudio.debugBundler.framework.ArtifactDownloadException;
import com.hortonworks.hivestudio.debugBundler.framework.ArtifactSource;
import com.hortonworks.hivestudio.debugBundler.framework.Params;

import java.io.IOException;
import java.io.PushbackReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SliderStatusArtifacts implements ArtifactSource {

  private final ObjectMapper objectMapper;

  @Inject
  public SliderStatusArtifacts(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  @Override
  public boolean hasRequiredParams(Params params) {
    return params.getAppType() != null && params.getAppType().equals("LLAP");
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    return Collections.<Artifact>singletonList(new Artifact() {
      @Override
      public boolean isTemp() {
        return false;
      }

      @Override
      public String getName() {
        return "SLIDER_AM/STATUS";
      }

      @Override
      public void downloadInto(Path path) throws ArtifactDownloadException {
        // Run the driver and get the status.

        throw new ArtifactDownloadException("Not implemented!");

//  TODO: Must be fixed once the dependency issues in artifact-hivetools pom is resolved.
//        LlapStatusServiceDriver serviceDriver = new LlapStatusServiceDriver();
//        int ret = serviceDriver.run(new LlapStatusOptionsProcessor.LlapStatusOptions(null), 0);
//        if (false){//ret == ExitCode.SUCCESS.getInt()) {
//          try {
//            PrintWriter writer = new PrintWriter(Files.newBufferedWriter(path));
//            serviceDriver.outputJson(writer);
//            writer.flush();
//            writer.close();
//          } catch (Exception e) {
//            throw new ArtifactDownloadException("Error trying to serialize status.", e);
//          }
//        } else {
//          throw new ArtifactDownloadException("Error trying to fetch status, ret code: " + ret);
//        }
      }
    });
  }

  @Override
  public void updateParams(Params params, Artifact artifact, Path path)
      throws ArtifactDownloadException {
    JsonNode tree;
    try {
      PushbackReader reader = new PushbackReader(Files.newBufferedReader(path));
      for (;;) {
        int ch = reader.read();
        if (ch < 0) {
          reader.close();
          return;
        }
        if (ch == '{') {
          reader.unread(ch);
          break;
        }
      }
      tree = objectMapper.readTree(reader);
    } catch (IOException e) {
      throw new ArtifactDownloadException(e);
    }
    if (tree == null) {
      return;
    }
    String sliderAppId = tree.path("amInfo").path("appId").asText();
    if (!sliderAppId.isEmpty()) {
      params.setSliderAppId(sliderAppId);
    }
    JsonNode instances = tree.path("runningInstances");
    if (instances.isArray()) {
      Set<String> inst = new HashSet<>();
      for (int i = 0; i < instances.size(); ++i) {
        String nodeUrl = instances.path(i).path("webUrl").asText();
        if (!nodeUrl.isEmpty()) {
          inst.add(nodeUrl);
        }
      }
      params.setSliderInstanceUrls(inst);
    }
  }
}
