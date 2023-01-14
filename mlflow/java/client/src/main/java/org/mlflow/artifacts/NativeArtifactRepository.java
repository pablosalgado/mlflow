package org.mlflow.artifacts;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.FileEntity;
import org.apache.http.util.EntityUtils;
import org.mlflow.api.proto.Service;
import org.mlflow.tracking.MlflowClientException;
import org.mlflow.tracking.MlflowClientVersion;
import org.mlflow.tracking.MlflowHttpCaller;
import org.mlflow.tracking.creds.DatabricksMlflowHostCreds;
import org.mlflow.tracking.creds.MlflowHostCreds;
import org.mlflow.tracking.creds.MlflowHostCredsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Java Native implementation to artifact repositories. Supersedes {@link CliBasedArtifactRepository}
 */
public class NativeArtifactRepository implements ArtifactRepository {
  private class HttpCaller extends MlflowHttpCaller {
    private final Logger logger = LoggerFactory.getLogger(MlflowHttpCaller.class);

    HttpCaller() {
      super(NativeArtifactRepository.this.hostCredsProvider);
    }

    private String get(String uri) {
      logger.debug("Sending GET " + uri);
      HttpGet request = new HttpGet();
      fillRequestSettings(request, uri);
      try {
        HttpResponse response = executeRequest(request);
        String responseJson = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        logger.debug("Response: " + responseJson);
        return responseJson;
      } catch (IOException e) {
        throw new MlflowClientException(e);
      }
    }

    public void put(String uri, FileEntity fileEntity) {
      logger.debug("Sending PUT " + uri);
      HttpPut request = new HttpPut();
      fillRequestSettings(request, uri);
      request.setEntity(fileEntity);
      try {
        HttpResponse response = executeRequest(request);
        String responseJson = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        logger.debug("Response: " + responseJson);
      } catch (IOException e) {
        throw new MlflowClientException(e);
      }
    }

    public File download(String uri, String artifactPath) {
      logger.debug("Sending GET " + uri);
      HttpGet request = new HttpGet();
      fillRequestSettings(request, uri);
      try {
        HttpResponse response = executeRequest(request);
        Path tempDir = Files.createTempDirectory(null);
        File f = tempDir.resolve(artifactPath).toFile();
        FileUtils.writeByteArrayToFile(f, EntityUtils.toByteArray(response.getEntity()));
        logger.debug("Response: " + f);
        return f;
      } catch (IOException e) {
        throw new MlflowClientException(e);
      }
    }

    private void fillRequestSettings(HttpRequestBase request, String uri) {
      MlflowHostCreds hostCreds = hostCredsProvider.getHostCreds();
      createHttpClientIfNecessary(hostCreds.shouldIgnoreTlsVerification());
      request.setURI(URI.create(uri));
      String username = hostCreds.getUsername();
      String password = hostCreds.getPassword();
      String token = hostCreds.getToken();
      if (username != null && password != null) {
        String authHeader = Base64.getEncoder()
          .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
        request.addHeader("Authorization", "Basic " + authHeader);
      } else if (token != null) {
        request.addHeader("Authorization", "Bearer " + token);
      }

      String userAgent = "mlflow-java-client";
      String clientVersion = MlflowClientVersion.getClientVersion();
      if (!clientVersion.isEmpty()) {
        userAgent += "/" + clientVersion;
      }
      request.addHeader("User-Agent", userAgent);
    }
  }

  // Base directory of the artifactory, used to let the user know why this repository was chosen.
  private final String artifactBaseDir;

  // Used to pass credentials as environment variables
  // (e.g., MLFLOW_TRACKING_URI or DATABRICKS_HOST) to the mlflow process.
  private final MlflowHostCredsProvider hostCredsProvider;

  private final HttpCaller httpCaller;

  private final String base_url = "/api/2.0/mlflow-artifacts/artifacts";

  public NativeArtifactRepository(
    String artifactBaseDir,
    MlflowHostCredsProvider hostCredsProvider) {
    this.artifactBaseDir = artifactBaseDir;
    this.hostCredsProvider = hostCredsProvider;
    this.httpCaller = new HttpCaller();
  }

  @Override
  public void logArtifact(File localFile, String artifactPath) {
    if (!localFile.exists()) {
      throw new MlflowClientException("Local file does not exist: " + localFile);
    }

    if (localFile.isDirectory()) {
      throw new MlflowClientException("Local path points to a directory. Use logArtifacts" +
        " instead: " + localFile);
    }

    verifyArtifactPath(artifactPath);

    Path p = Paths.get("/", StringUtils.defaultIfEmpty(artifactPath, ""), localFile.getName());

    URIBuilder artifactUriBuilder = newURIBuilder(this.artifactBaseDir);

    URIBuilder trackUriBuilder = newURIBuilder(this.hostCredsProvider.getHostCreds().getHost());
    trackUriBuilder.setPath(this.base_url + artifactUriBuilder.getPath() + p);

    FileEntity fileEntity = new FileEntity(localFile);

    this.httpCaller.put(trackUriBuilder.toString(), fileEntity);
  }

  @Override
  public void logArtifact(File localFile) {
    logArtifact(localFile, null);
  }

  @Override
  public void logArtifacts(File localDir) {
    logArtifacts(localDir, null);
  }

  @Override
  public void logArtifacts(File localDir, String artifactPath) {
    Path root = Paths.get(localDir.getAbsolutePath());

    try (Stream<Path> paths = Files.walk(root)) {
      paths.filter(Files::isRegularFile).forEach(path -> {
        Path parent = path.getParent();
        Path relPath = root.relativize(parent);

        logArtifact(
          path.toFile(),
          artifactPath == null ? relPath.toString() : Paths.get("", artifactPath, relPath.toString()).toString()
        );
      });
    } catch (IOException e) {
      throw new MlflowClientException(e);
    }
  }

  @Override
  public List<Service.FileInfo> listArtifacts() {
    return listArtifacts(null);
  }

  @Override
  public List<Service.FileInfo> listArtifacts(String artifactPath) {
    URIBuilder artifactUriBuilder = newURIBuilder(this.artifactBaseDir);
    String path = StringUtils.removeStart(artifactUriBuilder.getPath(), "/");

    URIBuilder trackUriBuilder = newURIBuilder(hostCredsProvider.getHostCreds().getHost());
    trackUriBuilder.setPath(base_url);
    trackUriBuilder.setParameter("path", path);

    String jsonOutput = httpCaller.get(trackUriBuilder.toString());

    return parseFileInfos(jsonOutput);
  }

  @Override
  public File downloadArtifacts() {
    return downloadArtifacts(null);
  }

  @Override
  public File downloadArtifacts(String artifactPath) {
    URIBuilder artifactUriBuilder = newURIBuilder(this.artifactBaseDir);

    URIBuilder trackUriBuilder = newURIBuilder(hostCredsProvider.getHostCreds().getHost());
    trackUriBuilder.setPath(base_url + artifactUriBuilder.getPath() + "/" + artifactPath);

    return httpCaller.download(trackUriBuilder.toString(), artifactPath);
  }

  @Override
  public File downloadArtifactFromUri(String artifactUri) {
    return null;
  }

  @VisibleForTesting
  void setProcessEnvironment(Map<String, String> environment, MlflowHostCreds hostCreds) {
    environment.put("MLFLOW_TRACKING_URI", hostCreds.getHost());
    if (hostCreds.getUsername() != null) {
      environment.put("MLFLOW_TRACKING_USERNAME", hostCreds.getUsername());
    }
    if (hostCreds.getPassword() != null) {
      environment.put("MLFLOW_TRACKING_PASSWORD", hostCreds.getPassword());
    }
    if (hostCreds.getToken() != null) {
      environment.put("MLFLOW_TRACKING_TOKEN", hostCreds.getToken());
    }
    if (hostCreds.shouldIgnoreTlsVerification()) {
      environment.put("MLFLOW_TRACKING_INSECURE_TLS", "true");
    }
  }

  @VisibleForTesting
  void setProcessEnvironmentDatabricks(
    Map<String, String> environment,
    DatabricksMlflowHostCreds hostCreds) {
    environment.put("DATABRICKS_HOST", hostCreds.getHost());
    if (hostCreds.getUsername() != null) {
      environment.put("DATABRICKS_USERNAME", hostCreds.getUsername());
    }
    if (hostCreds.getPassword() != null) {
      environment.put("DATABRICKS_PASSWORD", hostCreds.getPassword());
    }
    if (hostCreds.getToken() != null) {
      environment.put("DATABRICKS_TOKEN", hostCreds.getToken());
    }
    if (hostCreds.shouldIgnoreTlsVerification()) {
      environment.put("DATABRICKS_INSECURE", "true");
    }
  }

  private URIBuilder newURIBuilder(String base) {
    try {
      return new URIBuilder(base);
    } catch (URISyntaxException e) {
      throw new MlflowClientException("Failed to construct URI for " + base, e);
    }
  }

  private void verifyArtifactPath(String artifactPath) {
    if (artifactPath == null) return;

    String normalized = Paths.get("", artifactPath).normalize().toString();
    if (!normalized.equals(artifactPath)
      || normalized.equals(".")
      || normalized.startsWith(".")
      || normalized.startsWith("..")
      || normalized.startsWith("/")
    ) {
      throw new MlflowClientException(
        String.format("Invalid artifact path: %s.", artifactPath)
      );
    }
  }

  /**
   * Parses a list of JSON FileInfos, as returned by 'mlflow artifacts list'.
   */
  private List<Service.FileInfo> parseFileInfos(String json) {
    // The protobuf deserializer doesn't allow us to directly deserialize a list, so we
    // deserialize a list-of-dictionaries, and then reserialize each dictionary to pass it to
    // the protobuf deserializer.
    Gson gson = new Gson();
    Type type = new TypeToken<List<Map<String, Object>>>() {
    }.getType();
    List<Map<String, Object>> listOfDicts = gson.fromJson(json, type);
    List<Service.FileInfo> fileInfos = new ArrayList<>();
    for (Map<String, Object> dict : listOfDicts) {
      String fileInfoJson = gson.toJson(dict);
      try {
        Service.FileInfo.Builder builder = Service.FileInfo.newBuilder();
        JsonFormat.parser().merge(fileInfoJson, builder);
        fileInfos.add(builder.build());
      } catch (InvalidProtocolBufferException e) {
        throw new MlflowClientException("Failed to deserialize JSON into FileInfo: " + json, e);
      }
    }
    return fileInfos;
  }
}
