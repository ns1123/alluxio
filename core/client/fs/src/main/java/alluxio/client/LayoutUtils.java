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
import alluxio.wire.BlockInfo;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

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
   * Translates a logical offset to the logical chunk start position, starting from the file start.
   *
   * @param meta the encryption metadata
   * @param logicalOffset the logical offset
   * @return the translated chunk logical start position
   */
  public static long getLogicalChunkStart(EncryptionProto.Meta meta, long logicalOffset) {
    long numBlocksBeforeOffset = logicalOffset / meta.getLogicalBlockSize();
    long logicalOffsetWithinBlock = logicalOffset % meta.getLogicalBlockSize();
    long numChunksBeforeOffsetWithinBlock = logicalOffsetWithinBlock / meta.getChunkSize();
    return numBlocksBeforeOffset * meta.getLogicalBlockSize()
        + numChunksBeforeOffsetWithinBlock * meta.getChunkSize();
  }

  /**
   * Translates a logical offset to the physical chunk start position, starting from the file start.
   *
   * @param meta the encryption metadata
   * @param logicalOffset the logical offset
   * @return the translated chunk physical start position
   */
  public static long getPhysicalChunkStart(EncryptionProto.Meta meta, long logicalOffset) {
    long physicalChunkSize =
        meta.getChunkHeaderSize() + meta.getChunkSize() + meta.getChunkFooterSize();
    long numBlocksBeforeOffset = logicalOffset / meta.getLogicalBlockSize();
    long logicalOffsetWithinBlock = logicalOffset % meta.getLogicalBlockSize();
    long numChunksBeforeOffsetWithinBlock = logicalOffsetWithinBlock / meta.getChunkSize();
    return numBlocksBeforeOffset * meta.getPhysicalBlockSize()
        + numChunksBeforeOffsetWithinBlock * physicalChunkSize;
  }

  /**
   * Translates a logical offset to a logical offset within the chunk, starting from the logical
   * chunk start.
   *
   * @param meta the encryption metadata
   * @param logicalOffset the logical offset
   * @return the translated logical offset within the chunk
   */
  public static long getLogicalOffsetFromChunkStart(EncryptionProto.Meta meta, long logicalOffset) {
    long logicalOffsetWithinBlock = logicalOffset % meta.getLogicalBlockSize();
    return logicalOffsetWithinBlock % meta.getChunkSize();
  }

  /**
   * Translates a logical offset to a physical offset within the physical chunk, starting from the
   * physical chunk start.
   *
   * @param meta the encryption metadata
   * @param logicalOffset the logical offset
   * @return the translated physical offset within the chunk
   */
  public static long getPhysicalOffsetFromChunkStart(
      EncryptionProto.Meta meta, long logicalOffset) {
    return meta.getChunkHeaderSize() + getLogicalOffsetFromChunkStart(meta, logicalOffset);
  }

  /**
   * Translates a logical offset to a physical offset, starting from the file start.
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
   * Translates a logical length (for one or multiple chunks) to the physical length.
   * It is assumed that the physical length starts at the beginning of a chunk (with inclusive chunk
   * header) and reaches the end of a chunk (with inclusive chunk footer).
   *
   * @param meta the encryption metadata
   * @param logicalChunksLength the logical length for the chunks
   * @return the physical length for the chunks
   */
  public static long toPhysicalChunksLength(EncryptionProto.Meta meta, long logicalChunksLength) {
    if (logicalChunksLength == 0L) {
      return 0L;
    }
    long chunkHeaderSize = meta.getChunkHeaderSize();
    long chunkSize = meta.getChunkSize();
    long chunkFooterSize = meta.getChunkFooterSize();
    long physicalChunkSize = chunkHeaderSize + chunkSize + chunkFooterSize;
    long numFullChunks = logicalChunksLength / chunkSize;
    long lastPartialChunkPhysicalSize = logicalChunksLength % chunkSize == 0
        ? 0 : chunkHeaderSize + logicalChunksLength % chunkSize + chunkFooterSize;
    return numFullChunks * physicalChunkSize + lastPartialChunkPhysicalSize;
  }

  /**
   * Translates a logical block length to a physical block length.
   * It is assumed that the physical length starts at the beginning of a block (with inclusive block
   * header) and reaches the end of a block (with inclusive block footer).
   *
   * @param meta the encryption metadata
   * @param logicalBlockLength the logical block length
   * @return the physical block length
   */
  public static long toPhysicalBlockLength(EncryptionProto.Meta meta, long logicalBlockLength) {
    if (logicalBlockLength == 0L) {
      return 0L;
    }
    return meta.getBlockHeaderSize()
        + toPhysicalChunksLength(meta, logicalBlockLength)
        + meta.getBlockFooterSize();
  }

  /**
   * Translates a logical length to a physical length.
   * It is assumed that the physical length starts at the beginning of a file and reaches the end
   * of a file (with inclusive file footer).
   *
   * @param meta the encryption metadata
   * @param logicalFileLength the logical file length
   * @return the physical file length
   */
  public static long toPhysicalFileLength(EncryptionProto.Meta meta, long logicalFileLength) {
    Preconditions.checkState(meta.getLogicalBlockSize() > 0);
    Preconditions.checkState(meta.getPhysicalBlockSize() > 0);
    long numFullBlocks = logicalFileLength / meta.getLogicalBlockSize();
    long lastBlockLogicalSize = logicalFileLength % meta.getLogicalBlockSize();
    return numFullBlocks * meta.getPhysicalBlockSize()
        + toPhysicalBlockLength(meta, lastBlockLogicalSize)
        + meta.getEncodedMetaSize() + LayoutUtils.getFooterFixedOverhead();
  }

  /**
   * Translates a physical length (for one or multiple chunks) to a logical length.
   * It is assumed that the physical length starts at the beginning of a chunk (with inclusive chunk
   * header) and reaches the end of a chunk (with inclusive chunk footer).
   *
   * @param meta the encryption metadata
   * @param physicalChunksLength the physical length for the chunks
   * @return the logical length for the chunks
   */
  public static long toLogicalChunksLength(EncryptionProto.Meta meta, long physicalChunksLength) {
    if (physicalChunksLength == 0L) {
      return 0L;
    }
    long chunkHeaderSize = meta.getChunkHeaderSize();
    long chunkSize = meta.getChunkSize();
    long chunkFooterSize = meta.getChunkFooterSize();
    long physicalChunkSize = chunkHeaderSize + chunkSize + chunkFooterSize;
    long numFullChunks = physicalChunksLength / physicalChunkSize;
    long lastPartialChunkLogicalSize = physicalChunksLength % physicalChunkSize == 0
        ? 0 : physicalChunksLength % physicalChunkSize - chunkHeaderSize - chunkFooterSize;
    return numFullChunks * chunkSize + lastPartialChunkLogicalSize;
  }

  /**
   * Translates a physical block length to a logical block length.
   * It is assumed that the physical length starts at the beginning of a block (with inclusive block
   * header) and reaches the end of a block (with inclusive block footer).
   *
   * @param meta the encryption metadata
   * @param physicalBlockLength the physical block length
   * @return the logical block length
   */
  public static long toLogicalBlockLength(EncryptionProto.Meta meta, long physicalBlockLength) {
    if (physicalBlockLength == 0L) {
      return 0L;
    }
    long physicalChunksLength =
        physicalBlockLength - meta.getBlockHeaderSize() - meta.getBlockFooterSize();
    return toLogicalChunksLength(meta, physicalChunksLength);
  }

  /**
   * Translates a physical file length to a logical file length.
   * It is assumed that the physical length starts at the beginning of a file and reaches the end
   * of a file (with inclusive file footer).
   *
   * @param meta the encryption metadata
   * @param physicalFileLength the physical file length
   * @return the logical file length
   */
  public static long toLogicalFileLength(EncryptionProto.Meta meta, long physicalFileLength) {
    if (physicalFileLength == 0L) {
      return 0L;
    }
    long footerSize = meta.getEncodedMetaSize() + LayoutUtils.getFooterFixedOverhead();
    Preconditions.checkState(meta.getLogicalBlockSize() > 0);
    Preconditions.checkState(meta.getPhysicalBlockSize() > 0);
    long numFullBlocks = physicalFileLength / meta.getPhysicalBlockSize();
    long lastBlockPhysicalSize = physicalFileLength % meta.getPhysicalBlockSize();
    long lastBlockLogicalSize;
    if (lastBlockPhysicalSize > footerSize) {
      lastBlockLogicalSize = toLogicalBlockLength(meta, lastBlockPhysicalSize - footerSize);
    } else {
      // in this case, footer is split in the last two block. Should subtract the footer part
      // within the 2nd to last block.
      lastBlockLogicalSize = lastBlockPhysicalSize - footerSize;
    }
    return numFullBlocks * meta.getLogicalBlockSize() + lastBlockLogicalSize;
  }

  /**
   * @return the fixed overhead of the file footer, excluding the metadata section
   */
  public static int getFooterFixedOverhead() {
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
   * Decodes a file footer to {@link FileFooter.FileMetadata}. Since the encoded footer metadata
   * size can be variant, the input footer might have redundant bytes in the beginning.
   * The last 8 bytes are the footer magic bytes. The 8 bytes before the magic bytes indicates the
   * size of the actual {@link FileFooter.FileMetadata}.
   *
   * @param footer the encoded representation of the footer
   * @return the parsed {@link FileFooter.FileMetadata}
   */
  public static FileFooter.FileMetadata decodeFooter(byte[] footer)
      throws IOException {
    int len = footer.length;
    int metaMaxLen = len - FOOTER_SIZE_BYTES_LENGTH - FOOTER_MAGIC_BYTES_LENGTH;
    Preconditions.checkState(metaMaxLen > 0);
    ByteBuffer buf = ByteBuffer.wrap(footer);
    int metaSize = (int) buf.getLong(metaMaxLen);
    Preconditions.checkState(metaMaxLen >= metaSize);
    buf = ByteBuffer.wrap(footer, metaMaxLen - metaSize, metaSize);
    byte[] metaBytes = new byte[metaSize];
    buf.get(metaBytes, 0, metaSize);
    return FileFooter.FileMetadata.parseFrom(metaBytes);
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
      long fileId, FileFooter.FileMetadata fileMetadata, EncryptionProto.CryptoKey cryptoKey)
      throws IOException {
    EncryptionProto.Meta partialMeta = EncryptionProto.Meta.newBuilder()
        .setBlockHeaderSize(fileMetadata.getBlockHeaderSize())
        .setBlockFooterSize(fileMetadata.getBlockFooterSize())
        .setChunkHeaderSize(fileMetadata.getChunkHeaderSize())
        .setChunkSize(fileMetadata.getChunkSize())
        .setChunkFooterSize(fileMetadata.getChunkFooterSize())
        .buildPartial();
    long logicalBlockSize = toLogicalBlockLength(partialMeta, fileMetadata.getPhysicalBlockSize());
    return partialMeta.toBuilder()
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

  /**
   * Converts a physical {@link FileInfo} to logical.
   *
   * @param fileInfo the file info
   * @param meta the encryption metadata
   * @return the converted logical {@link FileInfo}
   */
  public static FileInfo convertFileInfoToLogical(FileInfo fileInfo, EncryptionProto.Meta meta) {
    int footerSize = (int) meta.getEncodedMetaSize() + LayoutUtils.getFooterFixedOverhead();
    FileInfo converted = fileInfo;
    // When a file is encrypted, translate the physical file and block lengths to logical.
    converted.setLength(LayoutUtils.toLogicalFileLength(meta, fileInfo.getLength()));
    List<FileBlockInfo> fileBlockInfos = new java.util.ArrayList<>();
    long firstSplitOfFileFooter = 0L;
    if (fileInfo.getFileBlockInfos().size() > 0) {
      BlockInfo lastBlockInfo =
          fileInfo.getFileBlockInfos().get(fileInfo.getFileBlockInfos().size() - 1).getBlockInfo();
      if (lastBlockInfo.getLength() <= footerSize) {
        firstSplitOfFileFooter = footerSize - lastBlockInfo.getLength();
      }
    }
    for (int i = 0; i < fileInfo.getFileBlockInfos().size(); i++) {
      FileBlockInfo info = fileInfo.getFileBlockInfos().get(i);
      BlockInfo blockInfo = info.getBlockInfo();
      if (i == fileInfo.getFileBlockInfos().size() - 1) {
        if (blockInfo.getLength() <= footerSize) {
          // Last block only include footer, this is not a valid logical block.
          continue;
        }
        blockInfo.setLength(
            LayoutUtils.toLogicalBlockLength(meta, blockInfo.getLength() - footerSize));
      } else if (i == fileInfo.getFileBlockInfos().size() - 2) {
        blockInfo.setLength(LayoutUtils.toLogicalBlockLength(
            meta, blockInfo.getLength() - firstSplitOfFileFooter));
      } else {
        blockInfo.setLength(LayoutUtils.toLogicalBlockLength(meta, blockInfo.getLength()));
      }
      info.setBlockInfo(blockInfo);
      fileBlockInfos.add(info);
    }
    converted.setFileBlockInfos(fileBlockInfos);
    converted.setBlockSizeBytes(meta.getLogicalBlockSize());
    return converted;
  }

  private LayoutUtils() {} // prevent instantiation
}
