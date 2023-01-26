package org.mlflow.artifacts;

import com.google.common.annotations.VisibleForTesting;
import org.mlflow.api.proto.Service;
import org.mlflow.tracking.creds.MlflowHostCredsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.net.URI;
import java.util.List;

public class S3ArtifactRepository implements ArtifactRepository {
  private final String artifactBaseDir;

  private final MlflowHostCredsProvider hostCredsProvider;

  private final String runId;

  private S3Client s3Client;

  public S3ArtifactRepository(
    String artifactBaseDir,
    String runId,
    MlflowHostCredsProvider hostCredsProvider) {

    this.artifactBaseDir = artifactBaseDir;
    this.runId = runId;
    this.hostCredsProvider = hostCredsProvider;

    this.s3Client = S3Client.builder()
      .credentialsProvider(
        ProfileCredentialsProvider.create()
      )
      .build();
  }

  @VisibleForTesting
  S3ArtifactRepository(
    String artifactBaseDir,
    String runId,
    MlflowHostCredsProvider hostCredsProvider,
    S3Client client) {
    this(artifactBaseDir, runId, hostCredsProvider);
    this.s3Client = client;
  }

  @Override
  public void logArtifact(File localFile) {
    PutObjectRequest por = PutObjectRequest.builder()
      .bucket(URI.create(artifactBaseDir).getHost())
      .key(this.runId + "/" + localFile.getName())
      .build();

    this.s3Client.putObject(
      por,
      RequestBody.fromFile(localFile)
    );
  }

  @Override
  public void logArtifact(File localFile, String artifactPath) {

  }

  @Override
  public void logArtifacts(File localDir) {

  }

  @Override
  public void logArtifacts(File localDir, String artifactPath) {

  }

  @Override
  public List<Service.FileInfo> listArtifacts() {
    return null;
  }

  @Override
  public List<Service.FileInfo> listArtifacts(String artifactPath) {
    return null;
  }

  @Override
  public File downloadArtifacts() {
    return null;
  }

  @Override
  public File downloadArtifacts(String artifactPath) {
    return null;
  }

  public String getRunId() {
    return runId;
  }

}
