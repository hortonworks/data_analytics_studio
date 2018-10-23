package com.hortonworks.hivestudio.debugBundler.source;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.hortonworks.hivestudio.common.entities.DagInfo;
import com.hortonworks.hivestudio.debugBundler.entities.history.DAGEntityType;
import com.hortonworks.hivestudio.debugBundler.entities.history.TaskAttemptEntityType;
import com.hortonworks.hivestudio.debugBundler.entities.history.TaskEntityType;
import com.hortonworks.hivestudio.debugBundler.entities.history.VertexEntityType;
import com.hortonworks.hivestudio.debugBundler.framework.Artifact;
import com.hortonworks.hivestudio.debugBundler.framework.ArtifactDownloadException;
import com.hortonworks.hivestudio.debugBundler.framework.ArtifactSource;
import com.hortonworks.hivestudio.debugBundler.framework.HistoryEventsArtifact;
import com.hortonworks.hivestudio.debugBundler.framework.Params;
import com.hortonworks.hivestudio.query.services.DagInfoService;

public class TezHDFSArtifacts implements ArtifactSource {

  private final DagInfoService dagInfoService;

  private final DAGEntityType dagEntityType;
  private final VertexEntityType vertexEntityType;
  private final TaskEntityType taskEntityType;
  private final TaskAttemptEntityType taskAttemptEntityType;

  @Inject
  public TezHDFSArtifacts(DagInfoService dagInfoService,
                          DAGEntityType dagEntityType, VertexEntityType vertexEntityType,
                          TaskEntityType taskEntityType, TaskAttemptEntityType taskAttemptEntityType) {
    this.dagInfoService = dagInfoService;

    this.dagEntityType = dagEntityType;
    this.vertexEntityType = vertexEntityType;
    this.taskEntityType = taskEntityType;
    this.taskAttemptEntityType = taskAttemptEntityType;
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    Optional<DagInfo> dagInfo = dagInfoService.getOneByDagId(params.getTezDagId());

    if(!dagInfo.isPresent()) {
      return ImmutableList.of();
    }

    org.apache.hadoop.fs.Path sourceFile = new org.apache.hadoop.fs.Path(dagInfo.get().getSourceFile());

    Artifact dagArtifact = new HistoryEventsArtifact(dagEntityType, sourceFile);
    Artifact vertexArtifact = new HistoryEventsArtifact(vertexEntityType, sourceFile);
    Artifact taskArtifact = new HistoryEventsArtifact(taskEntityType, sourceFile);
    Artifact taskAttemptArtifact = new HistoryEventsArtifact(taskAttemptEntityType, sourceFile);

    return ImmutableList.of(dagArtifact, vertexArtifact, taskArtifact, taskAttemptArtifact);
  }

  @Override
  public void updateParams(Params params, Artifact artifact, Path path) throws ArtifactDownloadException {
//    try {
//      // Set log paths from task attempt data
//    } catch (IOException e) {
//      throw new ArtifactDownloadException(e);
//    }
  }

  @Override
  public boolean hasRequiredParams(Params params) {
    return params.getTezDagId() != null;
  }

}
