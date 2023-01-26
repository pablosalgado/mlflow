package org.mlflow.artifacts;


import io.findify.s3mock.S3Mock;
import org.apache.commons.io.FileUtils;
import org.mlflow.api.proto.Service.RunInfo;
import org.mlflow.tracking.MlflowClient;
import org.mlflow.tracking.TestClientProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.mlflow.tracking.TestUtils.createExperimentName;

public class S3ArtifactRepositoryTest {

  private static final Logger logger = LoggerFactory.getLogger(
    S3ArtifactRepositoryTest.class);

  private final TestClientProvider testClientProvider = new TestClientProvider();

  private MlflowClient mlflowClient;

  private S3Mock s3Mock;

  @BeforeSuite
  public void beforeAll() throws IOException {
    s3Mock = new S3Mock.Builder()
      .withPort(8001)
      .withInMemoryBackend()
      .build();
    s3Mock.start();

    CreateBucketRequest cbr = CreateBucketRequest.builder()
      .bucket("test")
      .build();
    buildS3Client().createBucket(cbr);

    mlflowClient = testClientProvider.initializeClientAndServer("s3://test");
  }

  @AfterSuite
  public void afterAll() throws InterruptedException {
    testClientProvider.cleanupClientAndServer();
    s3Mock.shutdown();
  }

  @Test
  public void testLogArtifact() throws IOException {
    S3ArtifactRepository repo = newRepo();

    Path tempFile = Files.createTempFile(getClass().getSimpleName(), ".txt");
    FileUtils.writeStringToFile(tempFile.toFile(), "Hello, World!", StandardCharsets.UTF_8);

    repo.logArtifact(tempFile.toFile());

    ListObjectsRequest lor = ListObjectsRequest.builder()
      .bucket("test")
      .build();
    List<S3Object> objects = buildS3Client().listObjects(lor).contents();

    Assert.assertEquals(1, objects.size());
    Assert.assertEquals(repo.getRunId() + "/" + tempFile.getFileName().toString(), objects.get(0).key());
  }

  private S3Client buildS3Client() {
    return S3Client.builder()
      .forcePathStyle(true)
      .endpointOverride(
        URI.create("http://localhost:8001")
      )
      .credentialsProvider(
        AnonymousCredentialsProvider.create()
      )
      .build();
  }

  private S3ArtifactRepository newRepo() {
    String expName = createExperimentName();
    String expId = mlflowClient.createExperiment(expName);
    RunInfo runInfo = mlflowClient.createRun(expId);

    logger.info("Created run with id=" + runInfo.getRunUuid() + " and artifactUri=" +
      runInfo.getArtifactUri());

    return new S3ArtifactRepository(
      runInfo.getArtifactUri(),
      runInfo.getRunId(),
      testClientProvider.getClientHostCredsProvider(mlflowClient),
      buildS3Client());
  }

}
