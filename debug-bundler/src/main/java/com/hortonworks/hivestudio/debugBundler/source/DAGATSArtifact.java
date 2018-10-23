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
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.hortonworks.hivestudio.debugBundler.framework.Artifact;
import com.hortonworks.hivestudio.debugBundler.framework.ArtifactDownloadException;
import com.hortonworks.hivestudio.debugBundler.framework.ArtifactSource;
import com.hortonworks.hivestudio.debugBundler.framework.Params;
import com.hortonworks.hivestudio.debugBundler.helpers.ATSArtifactHelper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

// Does a reverse lookup and fetch Dag Id
public class DAGATSArtifact implements ArtifactSource {

  private final ATSArtifactHelper helper;
  private final ObjectMapper objectMapper;

  @Inject
  public DAGATSArtifact(ATSArtifactHelper helper, ObjectMapper objectMapper) {
    this.helper = helper;
    this.objectMapper = objectMapper;
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    return ImmutableList.of(helper.getChildEntityArtifact("TEZ_DAG_ATS_TMP", "TEZ_DAG_ID", "callerId",
      params.getHiveQueryId(), true));
  }

  @Override
  public void updateParams(Params params, Artifact artifact, Path path)
    throws ArtifactDownloadException {
    try {

      InputStream stream = Files.newInputStream(path);
      JsonNode node = objectMapper.readTree(stream);
      if (node == null) {
        return;
      }
      node = node.get("entities");
      if (node == null || !node.isArray()) {
        return;
      }

      if (params.getTezDagId() == null) {
        String dagId = node.path(0).path("entity").asText();
        if (dagId != null && !dagId.isEmpty()) {
          params.setTezDagId(dagId);
        }
      }

    } catch (IOException e) {
      throw new ArtifactDownloadException(e);
    }
  }

  @Override
  public boolean hasRequiredParams(Params params) {
    return params.getTezDagId() == null;
  }
}
