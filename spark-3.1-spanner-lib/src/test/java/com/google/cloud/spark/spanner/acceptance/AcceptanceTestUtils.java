package com.google.cloud.spark.spanner.acceptance;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.Comparator;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

public final class AcceptanceTestUtils {
  static final String BUCKET =
      Preconditions.checkNotNull(
          System.getenv("ACCEPTANCE_TEST_BUCKET"),
          "Please set the 'ACCEPTANCE_TEST_BUCKET' environment variable");

  static Storage storage =
      new StorageOptions.DefaultStorageFactory().create(StorageOptions.getDefaultInstance());

  public static Path getArtifact(Path targetDir, String prefix, String suffix) {
    Predicate<Path> prefixSuffixChecker = prefixSuffixChecker(prefix, suffix);
    try {
      System.out.println(Files.list(targetDir));
      return Files.list(targetDir)
          .filter(Files::isRegularFile)
          .filter(prefixSuffixChecker)
          .max(Comparator.comparing(AcceptanceTestUtils::lastModifiedTime))
          .get();
    } catch (IOException e) {
      throw new UncheckedIOException(e.getMessage(), e);
    }
  }

  public static String getCsv(String resultsDirUri) throws Exception {
    URI uri = new URI(resultsDirUri);
    Blob csvBlob =
        StreamSupport.stream(
                storage
                    .list(
                        uri.getAuthority(),
                        Storage.BlobListOption.prefix(uri.getPath().substring(1)))
                    .iterateAll()
                    .spliterator(),
                false)
            .filter(blob -> blob.getName().endsWith("csv"))
            .findFirst()
            .get();
    return new String(storage.readAllBytes(csvBlob.getBlobId()), StandardCharsets.UTF_8);
  }

  private static Predicate<Path> prefixSuffixChecker(final String prefix, final String suffix) {
    return path -> {
      String name = path.toFile().getName();
      return name.startsWith(prefix) && name.endsWith(suffix) && name.indexOf("-javadoc") == -1;
    };
  }

  private static FileTime lastModifiedTime(Path path) {
    try {
      return Files.getLastModifiedTime(path);
    } catch (IOException e) {
      throw new UncheckedIOException(e.getMessage(), e);
    }
  }

  public static BlobId copyToGcs(Path source, String destinationUri, String contentType)
      throws Exception {
    File sourceFile = source.toFile();
    try (FileInputStream sourceInputStream = new FileInputStream(sourceFile)) {
      FileChannel sourceFileChannel = sourceInputStream.getChannel();
      MappedByteBuffer sourceContent =
          sourceFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, sourceFile.length());
      return uploadToGcs(sourceContent, destinationUri, contentType);
    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Failed to write '%s' to '%s'", source, destinationUri), e);
    }
  }

  public static BlobId uploadToGcs(InputStream source, String destinationUri, String contentType)
      throws Exception {
    try {
      ByteBuffer sourceContent = ByteBuffer.wrap(ByteStreams.toByteArray(source));
      return uploadToGcs(sourceContent, destinationUri, contentType);
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to write to '%s'", destinationUri), e);
    }
  }

  public static BlobId uploadToGcs(ByteBuffer content, String destinationUri, String contentType)
      throws Exception {
    URI uri = new URI(destinationUri);
    BlobId blobId = BlobId.of(uri.getAuthority(), uri.getPath().substring(1));
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType(contentType).build();
    try (WriteChannel writer = storage.writer(blobInfo)) {
      writer.write(content);
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to write to '%s'", destinationUri), e);
    }
    return blobId;
  }

  public static String createTestBaseGcsDir(String testId) {
    return String.format("gs://%s/tests/%s", BUCKET, testId);
  }

  static void uploadConnectorJar(String targetDir, String prefix, String connectorJarUri)
      throws Exception {
    Path targetDirPath = Paths.get(targetDir);
    Path assemblyJar = AcceptanceTestUtils.getArtifact(targetDirPath, prefix, ".jar");
    AcceptanceTestUtils.copyToGcs(assemblyJar, connectorJarUri, "application/java-archive");
  }

  public static void deleteGcsDir(String testBaseGcsDir) throws Exception {
    URI uri = new URI(testBaseGcsDir);
    BlobId[] blobIds =
        StreamSupport.stream(
                storage
                    .list(
                        uri.getAuthority(),
                        Storage.BlobListOption.prefix(uri.getPath().substring(1)))
                    .iterateAll()
                    .spliterator(),
                false)
            .map(Blob::getBlobId)
            .toArray(BlobId[]::new);
    if (blobIds.length > 1) {
      storage.delete(blobIds);
    }
  }
}
