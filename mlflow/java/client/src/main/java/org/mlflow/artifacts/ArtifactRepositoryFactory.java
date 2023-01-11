package org.mlflow.artifacts;

import java.net.URI;

import org.mlflow.tracking.creds.MlflowHostCredsProvider;

public class ArtifactRepositoryFactory {
  private final MlflowHostCredsProvider hostCredsProvider;
  private final boolean nativeArtifactRepository;

  public ArtifactRepositoryFactory(MlflowHostCredsProvider hostCredsProvider, boolean nativeArtifactRepository) {
    this.hostCredsProvider = hostCredsProvider;
    this.nativeArtifactRepository = nativeArtifactRepository;
  }

  public ArtifactRepository getArtifactRepository() {
    if(this.nativeArtifactRepository) {
      return new NativeArtifactRepository(null, null, hostCredsProvider);
    } else {
      return new CliBasedArtifactRepository(null, null,hostCredsProvider);
    }
  }

  public ArtifactRepository getArtifactRepository(URI baseArtifactUri, String runId) {
    if(this.nativeArtifactRepository) {
      return new NativeArtifactRepository(baseArtifactUri.toString(), runId, hostCredsProvider);
    } else {
      return new CliBasedArtifactRepository(baseArtifactUri.toString(), runId, hostCredsProvider);
    }
  }
}
