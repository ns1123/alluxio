/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client;

import alluxio.Constants;
import alluxio.proto.layout.FileFooter;
import alluxio.proto.security.EncryptionProto;
import alluxio.util.FormatUtils;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The util class for block and chunk layout translation.
 */
@ThreadSafe
public final class LayoutUtils {
  private static final int FOOTER_MAGIC_BYTES_LENGTH = 8;
  private static final int FOOTER_SIZE_BYTES_LENGTH = 8;
  private static final int FOOTER_METADATA_MAX_SIZE = initializeFileMetadataSize();

  /**
   * Translates a logical offset to the physical chunk start offset.
   *
   * @param meta the encryption metadata
   * @param logicalOffset the logical offset
   * @return the translated chunk physical start
   */
  public static long getPhysicalChunkStart(EncryptionProto.Meta meta, long logicalOffset) {
    final long physicalChunkSize =
        meta.getChunkHeaderSize() + meta.getChunkSize() + meta.getChunkFooterSize();
    final long numChunksBeforeOffset = logicalOffset / meta.getChunkSize();
    return meta.getBlockHeaderSize() + numChunksBeforeOffset * physicalChunkSize;
  }

  /**
   * Translates a logical offset to a physical offset starting from the physical chunk start.
   *
   * @param meta the encryption metadata
   * @param logicalOffset the logical offset
   * @return the translated physical offset from the chunk physical start
   */
  public static long getPhysicalOffsetFromChunkStart(
      EncryptionProto.Meta meta, long logicalOffset) {
    return meta.getChunkHeaderSize() + logicalOffset % meta.getChunkSize();
  }

  /**
   * Translates a logical offset to a physical offset.
   *
   * @param meta the encryption metadata
   * @param logicalOffset the logical offset
   * @return the translated physical offset
   */
  public static long toPhysicalOffset(EncryptionProto.Meta meta, long logicalOffset) {
    return getPhysicalChunkStart(meta, logicalOffset)
        + getPhysicalOffsetFromChunkStart(meta, logicalOffset);
  }

  /**
   * Translates a physical offset to a logical offset.
   * It is assumed that the physical offset reaches the end of a chunk, with inclusive chunk footer.
   *
   * @param meta the encryption metadata
   * @param physicalOffset the physical offset
   * @return the translated logical offset
   */
  public static long toLogicalOffset(EncryptionProto.Meta meta, long physicalOffset) {
    final long blockHeaderSize = meta.getBlockHeaderSize();
    if (physicalOffset == 0 || physicalOffset == blockHeaderSize) {
      return 0L;
    }
    final long chunkSize = meta.getChunkSize();
    final long physicalChunkSize =
        meta.getChunkHeaderSize() + meta.getChunkSize() + meta.getChunkFooterSize();
    final long numChunksBeforeOffset = (physicalOffset - blockHeaderSize) / physicalChunkSize;
    Preconditions.checkState(
        physicalOffset >= blockHeaderSize + meta.getChunkHeaderSize() + meta.getChunkFooterSize());
    final long secondPart = (physicalOffset - blockHeaderSize) % physicalChunkSize;
    final long logicalOffsetInChunk =
        secondPart == 0 ? 0 : secondPart - meta.getChunkHeaderSize() - meta.getChunkFooterSize();
    Preconditions.checkState(logicalOffsetInChunk >= 0 && logicalOffsetInChunk < chunkSize);
    return numChunksBeforeOffset * chunkSize + logicalOffsetInChunk;
  }

  /**
   * Translates a logical length to a physical length, given the logical offset.
   * It is assumed that the physical length reaches the end of a chunk, with inclusive chunk footer.
   *
   * @param meta the encryption metadata
   * @param logicalOffset the logical offset
   * @param logicalLength the logical length
   * @return the physical length
   */
  public static long toPhysicalLength(
      EncryptionProto.Meta meta, long logicalOffset, long logicalLength) {
    final long bytesLeftInChunk = meta.getChunkSize() - logicalOffset % meta.getChunkSize();
    final long chunkSize = meta.getChunkSize();
    if (logicalLength <= bytesLeftInChunk) {
      return logicalLength + meta.getChunkFooterSize();
    }
    final long physicalChunkSize =
        meta.getChunkHeaderSize() + meta.getChunkSize() + meta.getChunkFooterSize();
    long secondPart = logicalLength - bytesLeftInChunk;
    return bytesLeftInChunk + meta.getChunkFooterSize() + secondPart / chunkSize * physicalChunkSize
        + (secondPart % chunkSize == 0 ? 0
            : meta.getChunkHeaderSize() + secondPart % chunkSize + meta.getChunkFooterSize());
  }

  /**
   * Translates a physical length to a logical length, given the physical offset.
   * It is assumed that the physical length reaches the end of a chunk, with inclusive chunk footer.
   *
   * @param meta the encryption metadata
   * @param physicalOffset the physical offset
   * @param physicalLength the physical length
   * @return the logical length
   */
  public static long toLogicalLength(
      EncryptionProto.Meta meta, long physicalOffset, long physicalLength) {
    if (physicalLength == 0L) {
      return 0L;
    }
    final long chunkHeaderSize = meta.getChunkHeaderSize();
    final long chunkSize = meta.getChunkSize();
    final long chunkFooterSize = meta.getChunkFooterSize();
    final long physicalChunkSize = chunkHeaderSize + chunkSize + chunkFooterSize;
    // Adjust the physical offset 0 to start at the logical offset 0.
    final long adjustedPhysicalOffset = physicalOffset == 0
        ? meta.getBlockHeaderSize() + chunkHeaderSize : physicalOffset;
    Preconditions.checkState(adjustedPhysicalOffset >= meta.getBlockHeaderSize() + chunkHeaderSize);
    Preconditions.checkState(physicalLength >= chunkFooterSize);
    final long logicalOffsetInChunk =
        (adjustedPhysicalOffset - meta.getBlockHeaderSize()) % physicalChunkSize - chunkHeaderSize;
    Preconditions.checkState(logicalOffsetInChunk >= 0 && logicalOffsetInChunk < chunkSize);
    final long bytesLeftInChunk = chunkSize - logicalOffsetInChunk;
    if (physicalLength <= bytesLeftInChunk + chunkFooterSize) {
      return physicalLength - chunkFooterSize;
    }
    final long secondPart = physicalLength - bytesLeftInChunk - chunkFooterSize;
    final long physicalLengthInLastChunk = secondPart % physicalChunkSize;
    Preconditions.checkState(physicalLengthInLastChunk == 0
        || (physicalLengthInLastChunk > chunkHeaderSize + chunkFooterSize
        && physicalLengthInLastChunk < chunkHeaderSize + chunkSize + chunkFooterSize));
    return bytesLeftInChunk + secondPart / physicalChunkSize * chunkSize
        + (physicalLengthInLastChunk == 0 ? 0
            : (physicalLengthInLastChunk - chunkHeaderSize - chunkFooterSize));
  }

  public static int getFooterFixedOverhead(){
    return FOOTER_SIZE_BYTES_LENGTH + FOOTER_MAGIC_BYTES_LENGTH;
  }

  /**
   * @return the file footer max size
   */
  public static int getFooterMaxSize() {
    return FOOTER_METADATA_MAX_SIZE + getFooterFixedOverhead();
  }

  /**
   * @param meta the input encryption metadata
   * @return the encoded footer from encryption metadata
   */
  public static byte[] encodeFooter(EncryptionProto.Meta meta) {
    FileFooter.FileMetadata fileMetadata = fromEncryptionMeta(meta);
    byte[] encodedMeta = fileMetadata.toByteArray();
    Preconditions.checkState(fileMetadata.getSerializedSize() == meta.getEncodedMetaSize());
    ByteBuffer footer = ByteBuffer.allocate(
        encodedMeta.length + FOOTER_SIZE_BYTES_LENGTH + FOOTER_MAGIC_BYTES_LENGTH);
    footer.put(encodedMeta);
    footer.putLong(encodedMeta.length);
    footer.put(Constants.ENCRYPTION_MAGIC.getBytes());
    return footer.array();
  }

  /**
   * Decodes a file footer to {@link EncryptionProto.Meta}. Since the encoded footer metadata size
   * can be variant, the input footer might have redundant bytes in the beginning. The last 8 bytes
   * are the footer magic bytes. The 8 bytes before the magic bytes indicates the size of the actual
   * {@link EncryptionProto.Meta}.
   *
   * @param fileId the file id
   * @param footer the encoded representation of the footer
   * @param cryptoKey the crypto key
   * @return the parsed encryption metadata
   */
  public static EncryptionProto.Meta decodeFooter(
      long fileId, byte[] footer, EncryptionProto.CryptoKey cryptoKey) throws IOException {
    int len = footer.length;
    int metaMaxLen = len - FOOTER_SIZE_BYTES_LENGTH - FOOTER_MAGIC_BYTES_LENGTH;
    Preconditions.checkState(len > 0);
    ByteBuffer buf = ByteBuffer.wrap(footer);
    int metaSize = (int) buf.getLong(metaMaxLen);
    byte[] metaBytes = new byte[metaSize];
    Preconditions.checkState(metaMaxLen >= metaSize);
    buf.get(metaBytes, metaMaxLen - metaSize, metaSize);
    return fromFooterMetadata(fileId, FileFooter.FileMetadata.parseFrom(metaBytes), cryptoKey);
  }

  /**
   * Creates a {@link FileFooter.FileMetadata} from an {@link EncryptionProto.Meta}.
   *
   * @param meta the encryption meta
   * @return the parsed {@link FileFooter.FileMetadata}
   */
  public static FileFooter.FileMetadata fromEncryptionMeta(EncryptionProto.Meta meta) {
    return FileFooter.FileMetadata.newBuilder()
        .setBlockHeaderSize(meta.getBlockHeaderSize())
        .setBlockFooterSize(meta.getBlockFooterSize())
        .setChunkHeaderSize(meta.getChunkHeaderSize())
        .setChunkSize(meta.getChunkSize())
        .setChunkFooterSize(meta.getChunkFooterSize())
        .setPhysicalBlockSize(meta.getPhysicalBlockSize())
        .setEncryptionId(meta.getEncryptionId())
        .build();
  }

  /**
   * Creates an {@link EncryptionProto.Meta} from a {@link FileFooter.FileMetadata}.
   *
   * @param fileId the file id
   * @param fileMetadata the file metadata
   * @param cryptoKey the crypto key
   * @return the parsed encryption metadata
   */
  public static EncryptionProto.Meta fromFooterMetadata(
      long fileId, FileFooter.FileMetadata fileMetadata, EncryptionProto.CryptoKey cryptoKey) {
    long physicalChunkSize = fileMetadata.getChunkHeaderSize() + fileMetadata.getChunkSize()
        + fileMetadata.getChunkFooterSize();
    long logicalBlockSize = (fileMetadata.getPhysicalBlockSize() - fileMetadata.getBlockHeaderSize()
        - fileMetadata.getBlockFooterSize()) / physicalChunkSize * fileMetadata.getChunkSize();
    return EncryptionProto.Meta.newBuilder()
        .setBlockHeaderSize(fileMetadata.getBlockHeaderSize())
        .setBlockFooterSize(fileMetadata.getBlockFooterSize())
        .setChunkHeaderSize(fileMetadata.getChunkHeaderSize())
        .setChunkSize(fileMetadata.getChunkSize())
        .setChunkFooterSize(fileMetadata.getChunkFooterSize())
        .setPhysicalBlockSize(fileMetadata.getPhysicalBlockSize())
        .setLogicalBlockSize(logicalBlockSize)
        .setFileId(fileId)
        .setEncryptionId(fileMetadata.getEncryptionId())
        .setEncodedMetaSize(fileMetadata.getSerializedSize())
        .setCryptoKey(cryptoKey)
        .build();
  }

  private static int initializeFileMetadataSize() {
    // Create a file metadata proto with all fields present. This should show the max file of the
    // serialized proto. Note: this must be updated once new fields are added in the
    // FileMetadata proto.
    FileFooter.FileMetadata fileMetadata = FileFooter.FileMetadata.newBuilder()
        .setBlockHeaderSize(0)
        .setBlockFooterSize(0)
        .setChunkHeaderSize(0)
        .setChunkSize(0)
        .setChunkFooterSize(0)
        .setPhysicalBlockSize(0)
        .setEncryptionId(0)
        .build();
    return fileMetadata.getSerializedSize();
  }

  private LayoutUtils() {} // prevent instantiation
}
