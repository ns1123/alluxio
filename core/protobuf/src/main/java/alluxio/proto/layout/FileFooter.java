// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: layout/file_footer.proto

package alluxio.proto.layout;

public final class FileFooter {
  private FileFooter() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface FileMetadataOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // optional fixed64 block_header_size = 1;
    /**
     * <code>optional fixed64 block_header_size = 1;</code>
     *
     * <pre>
     * in bytes
     * </pre>
     */
    boolean hasBlockHeaderSize();
    /**
     * <code>optional fixed64 block_header_size = 1;</code>
     *
     * <pre>
     * in bytes
     * </pre>
     */
    long getBlockHeaderSize();

    // optional fixed64 block_footer_size = 2;
    /**
     * <code>optional fixed64 block_footer_size = 2;</code>
     *
     * <pre>
     * in bytes
     * </pre>
     */
    boolean hasBlockFooterSize();
    /**
     * <code>optional fixed64 block_footer_size = 2;</code>
     *
     * <pre>
     * in bytes
     * </pre>
     */
    long getBlockFooterSize();

    // optional fixed64 chunk_header_size = 3;
    /**
     * <code>optional fixed64 chunk_header_size = 3;</code>
     *
     * <pre>
     * in bytes
     * </pre>
     */
    boolean hasChunkHeaderSize();
    /**
     * <code>optional fixed64 chunk_header_size = 3;</code>
     *
     * <pre>
     * in bytes
     * </pre>
     */
    long getChunkHeaderSize();

    // optional fixed64 chunk_size = 4;
    /**
     * <code>optional fixed64 chunk_size = 4;</code>
     *
     * <pre>
     * in bytes
     * </pre>
     */
    boolean hasChunkSize();
    /**
     * <code>optional fixed64 chunk_size = 4;</code>
     *
     * <pre>
     * in bytes
     * </pre>
     */
    long getChunkSize();

    // optional fixed64 chunk_footer_size = 5;
    /**
     * <code>optional fixed64 chunk_footer_size = 5;</code>
     *
     * <pre>
     * in bytes
     * </pre>
     */
    boolean hasChunkFooterSize();
    /**
     * <code>optional fixed64 chunk_footer_size = 5;</code>
     *
     * <pre>
     * in bytes
     * </pre>
     */
    long getChunkFooterSize();

    // optional fixed64 physical_block_size = 6;
    /**
     * <code>optional fixed64 physical_block_size = 6;</code>
     *
     * <pre>
     * in bytes
     * </pre>
     */
    boolean hasPhysicalBlockSize();
    /**
     * <code>optional fixed64 physical_block_size = 6;</code>
     *
     * <pre>
     * in bytes
     * </pre>
     */
    long getPhysicalBlockSize();

    // optional fixed64 encryption_id = 7;
    /**
     * <code>optional fixed64 encryption_id = 7;</code>
     */
    boolean hasEncryptionId();
    /**
     * <code>optional fixed64 encryption_id = 7;</code>
     */
    long getEncryptionId();
  }
  /**
   * Protobuf type {@code alluxio.proto.layout.FileMetadata}
   *
   * <pre>
   * The protobuf that includes file-level metadata information. Use fixed type here to avoid
   * unpredictable size with protobuf varint.
   * </pre>
   */
  public static final class FileMetadata extends
      com.google.protobuf.GeneratedMessage
      implements FileMetadataOrBuilder {
    // Use FileMetadata.newBuilder() to construct.
    private FileMetadata(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private FileMetadata(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final FileMetadata defaultInstance;
    public static FileMetadata getDefaultInstance() {
      return defaultInstance;
    }

    public FileMetadata getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private FileMetadata(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 9: {
              bitField0_ |= 0x00000001;
              blockHeaderSize_ = input.readFixed64();
              break;
            }
            case 17: {
              bitField0_ |= 0x00000002;
              blockFooterSize_ = input.readFixed64();
              break;
            }
            case 25: {
              bitField0_ |= 0x00000004;
              chunkHeaderSize_ = input.readFixed64();
              break;
            }
            case 33: {
              bitField0_ |= 0x00000008;
              chunkSize_ = input.readFixed64();
              break;
            }
            case 41: {
              bitField0_ |= 0x00000010;
              chunkFooterSize_ = input.readFixed64();
              break;
            }
            case 49: {
              bitField0_ |= 0x00000020;
              physicalBlockSize_ = input.readFixed64();
              break;
            }
            case 57: {
              bitField0_ |= 0x00000040;
              encryptionId_ = input.readFixed64();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.proto.layout.FileFooter.internal_static_alluxio_proto_layout_FileMetadata_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.proto.layout.FileFooter.internal_static_alluxio_proto_layout_FileMetadata_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.proto.layout.FileFooter.FileMetadata.class, alluxio.proto.layout.FileFooter.FileMetadata.Builder.class);
    }

    public static com.google.protobuf.Parser<FileMetadata> PARSER =
        new com.google.protobuf.AbstractParser<FileMetadata>() {
      public FileMetadata parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new FileMetadata(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<FileMetadata> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // optional fixed64 block_header_size = 1;
    public static final int BLOCK_HEADER_SIZE_FIELD_NUMBER = 1;
    private long blockHeaderSize_;
    /**
     * <code>optional fixed64 block_header_size = 1;</code>
     *
     * <pre>
     * in bytes
     * </pre>
     */
    public boolean hasBlockHeaderSize() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional fixed64 block_header_size = 1;</code>
     *
     * <pre>
     * in bytes
     * </pre>
     */
    public long getBlockHeaderSize() {
      return blockHeaderSize_;
    }

    // optional fixed64 block_footer_size = 2;
    public static final int BLOCK_FOOTER_SIZE_FIELD_NUMBER = 2;
    private long blockFooterSize_;
    /**
     * <code>optional fixed64 block_footer_size = 2;</code>
     *
     * <pre>
     * in bytes
     * </pre>
     */
    public boolean hasBlockFooterSize() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional fixed64 block_footer_size = 2;</code>
     *
     * <pre>
     * in bytes
     * </pre>
     */
    public long getBlockFooterSize() {
      return blockFooterSize_;
    }

    // optional fixed64 chunk_header_size = 3;
    public static final int CHUNK_HEADER_SIZE_FIELD_NUMBER = 3;
    private long chunkHeaderSize_;
    /**
     * <code>optional fixed64 chunk_header_size = 3;</code>
     *
     * <pre>
     * in bytes
     * </pre>
     */
    public boolean hasChunkHeaderSize() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional fixed64 chunk_header_size = 3;</code>
     *
     * <pre>
     * in bytes
     * </pre>
     */
    public long getChunkHeaderSize() {
      return chunkHeaderSize_;
    }

    // optional fixed64 chunk_size = 4;
    public static final int CHUNK_SIZE_FIELD_NUMBER = 4;
    private long chunkSize_;
    /**
     * <code>optional fixed64 chunk_size = 4;</code>
     *
     * <pre>
     * in bytes
     * </pre>
     */
    public boolean hasChunkSize() {
      return ((bitField0_ & 0x00000008) == 0x00000008);
    }
    /**
     * <code>optional fixed64 chunk_size = 4;</code>
     *
     * <pre>
     * in bytes
     * </pre>
     */
    public long getChunkSize() {
      return chunkSize_;
    }

    // optional fixed64 chunk_footer_size = 5;
    public static final int CHUNK_FOOTER_SIZE_FIELD_NUMBER = 5;
    private long chunkFooterSize_;
    /**
     * <code>optional fixed64 chunk_footer_size = 5;</code>
     *
     * <pre>
     * in bytes
     * </pre>
     */
    public boolean hasChunkFooterSize() {
      return ((bitField0_ & 0x00000010) == 0x00000010);
    }
    /**
     * <code>optional fixed64 chunk_footer_size = 5;</code>
     *
     * <pre>
     * in bytes
     * </pre>
     */
    public long getChunkFooterSize() {
      return chunkFooterSize_;
    }

    // optional fixed64 physical_block_size = 6;
    public static final int PHYSICAL_BLOCK_SIZE_FIELD_NUMBER = 6;
    private long physicalBlockSize_;
    /**
     * <code>optional fixed64 physical_block_size = 6;</code>
     *
     * <pre>
     * in bytes
     * </pre>
     */
    public boolean hasPhysicalBlockSize() {
      return ((bitField0_ & 0x00000020) == 0x00000020);
    }
    /**
     * <code>optional fixed64 physical_block_size = 6;</code>
     *
     * <pre>
     * in bytes
     * </pre>
     */
    public long getPhysicalBlockSize() {
      return physicalBlockSize_;
    }

    // optional fixed64 encryption_id = 7;
    public static final int ENCRYPTION_ID_FIELD_NUMBER = 7;
    private long encryptionId_;
    /**
     * <code>optional fixed64 encryption_id = 7;</code>
     */
    public boolean hasEncryptionId() {
      return ((bitField0_ & 0x00000040) == 0x00000040);
    }
    /**
     * <code>optional fixed64 encryption_id = 7;</code>
     */
    public long getEncryptionId() {
      return encryptionId_;
    }

    private void initFields() {
      blockHeaderSize_ = 0L;
      blockFooterSize_ = 0L;
      chunkHeaderSize_ = 0L;
      chunkSize_ = 0L;
      chunkFooterSize_ = 0L;
      physicalBlockSize_ = 0L;
      encryptionId_ = 0L;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeFixed64(1, blockHeaderSize_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeFixed64(2, blockFooterSize_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        output.writeFixed64(3, chunkHeaderSize_);
      }
      if (((bitField0_ & 0x00000008) == 0x00000008)) {
        output.writeFixed64(4, chunkSize_);
      }
      if (((bitField0_ & 0x00000010) == 0x00000010)) {
        output.writeFixed64(5, chunkFooterSize_);
      }
      if (((bitField0_ & 0x00000020) == 0x00000020)) {
        output.writeFixed64(6, physicalBlockSize_);
      }
      if (((bitField0_ & 0x00000040) == 0x00000040)) {
        output.writeFixed64(7, encryptionId_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeFixed64Size(1, blockHeaderSize_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeFixed64Size(2, blockFooterSize_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.CodedOutputStream
          .computeFixed64Size(3, chunkHeaderSize_);
      }
      if (((bitField0_ & 0x00000008) == 0x00000008)) {
        size += com.google.protobuf.CodedOutputStream
          .computeFixed64Size(4, chunkSize_);
      }
      if (((bitField0_ & 0x00000010) == 0x00000010)) {
        size += com.google.protobuf.CodedOutputStream
          .computeFixed64Size(5, chunkFooterSize_);
      }
      if (((bitField0_ & 0x00000020) == 0x00000020)) {
        size += com.google.protobuf.CodedOutputStream
          .computeFixed64Size(6, physicalBlockSize_);
      }
      if (((bitField0_ & 0x00000040) == 0x00000040)) {
        size += com.google.protobuf.CodedOutputStream
          .computeFixed64Size(7, encryptionId_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static alluxio.proto.layout.FileFooter.FileMetadata parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static alluxio.proto.layout.FileFooter.FileMetadata parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static alluxio.proto.layout.FileFooter.FileMetadata parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static alluxio.proto.layout.FileFooter.FileMetadata parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static alluxio.proto.layout.FileFooter.FileMetadata parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static alluxio.proto.layout.FileFooter.FileMetadata parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static alluxio.proto.layout.FileFooter.FileMetadata parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static alluxio.proto.layout.FileFooter.FileMetadata parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static alluxio.proto.layout.FileFooter.FileMetadata parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static alluxio.proto.layout.FileFooter.FileMetadata parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(alluxio.proto.layout.FileFooter.FileMetadata prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code alluxio.proto.layout.FileMetadata}
     *
     * <pre>
     * The protobuf that includes file-level metadata information. Use fixed type here to avoid
     * unpredictable size with protobuf varint.
     * </pre>
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements alluxio.proto.layout.FileFooter.FileMetadataOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return alluxio.proto.layout.FileFooter.internal_static_alluxio_proto_layout_FileMetadata_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return alluxio.proto.layout.FileFooter.internal_static_alluxio_proto_layout_FileMetadata_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                alluxio.proto.layout.FileFooter.FileMetadata.class, alluxio.proto.layout.FileFooter.FileMetadata.Builder.class);
      }

      // Construct using alluxio.proto.layout.FileFooter.FileMetadata.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        blockHeaderSize_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000001);
        blockFooterSize_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000002);
        chunkHeaderSize_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000004);
        chunkSize_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000008);
        chunkFooterSize_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000010);
        physicalBlockSize_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000020);
        encryptionId_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000040);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return alluxio.proto.layout.FileFooter.internal_static_alluxio_proto_layout_FileMetadata_descriptor;
      }

      public alluxio.proto.layout.FileFooter.FileMetadata getDefaultInstanceForType() {
        return alluxio.proto.layout.FileFooter.FileMetadata.getDefaultInstance();
      }

      public alluxio.proto.layout.FileFooter.FileMetadata build() {
        alluxio.proto.layout.FileFooter.FileMetadata result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public alluxio.proto.layout.FileFooter.FileMetadata buildPartial() {
        alluxio.proto.layout.FileFooter.FileMetadata result = new alluxio.proto.layout.FileFooter.FileMetadata(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.blockHeaderSize_ = blockHeaderSize_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.blockFooterSize_ = blockFooterSize_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        result.chunkHeaderSize_ = chunkHeaderSize_;
        if (((from_bitField0_ & 0x00000008) == 0x00000008)) {
          to_bitField0_ |= 0x00000008;
        }
        result.chunkSize_ = chunkSize_;
        if (((from_bitField0_ & 0x00000010) == 0x00000010)) {
          to_bitField0_ |= 0x00000010;
        }
        result.chunkFooterSize_ = chunkFooterSize_;
        if (((from_bitField0_ & 0x00000020) == 0x00000020)) {
          to_bitField0_ |= 0x00000020;
        }
        result.physicalBlockSize_ = physicalBlockSize_;
        if (((from_bitField0_ & 0x00000040) == 0x00000040)) {
          to_bitField0_ |= 0x00000040;
        }
        result.encryptionId_ = encryptionId_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof alluxio.proto.layout.FileFooter.FileMetadata) {
          return mergeFrom((alluxio.proto.layout.FileFooter.FileMetadata)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(alluxio.proto.layout.FileFooter.FileMetadata other) {
        if (other == alluxio.proto.layout.FileFooter.FileMetadata.getDefaultInstance()) return this;
        if (other.hasBlockHeaderSize()) {
          setBlockHeaderSize(other.getBlockHeaderSize());
        }
        if (other.hasBlockFooterSize()) {
          setBlockFooterSize(other.getBlockFooterSize());
        }
        if (other.hasChunkHeaderSize()) {
          setChunkHeaderSize(other.getChunkHeaderSize());
        }
        if (other.hasChunkSize()) {
          setChunkSize(other.getChunkSize());
        }
        if (other.hasChunkFooterSize()) {
          setChunkFooterSize(other.getChunkFooterSize());
        }
        if (other.hasPhysicalBlockSize()) {
          setPhysicalBlockSize(other.getPhysicalBlockSize());
        }
        if (other.hasEncryptionId()) {
          setEncryptionId(other.getEncryptionId());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        alluxio.proto.layout.FileFooter.FileMetadata parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (alluxio.proto.layout.FileFooter.FileMetadata) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // optional fixed64 block_header_size = 1;
      private long blockHeaderSize_ ;
      /**
       * <code>optional fixed64 block_header_size = 1;</code>
       *
       * <pre>
       * in bytes
       * </pre>
       */
      public boolean hasBlockHeaderSize() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>optional fixed64 block_header_size = 1;</code>
       *
       * <pre>
       * in bytes
       * </pre>
       */
      public long getBlockHeaderSize() {
        return blockHeaderSize_;
      }
      /**
       * <code>optional fixed64 block_header_size = 1;</code>
       *
       * <pre>
       * in bytes
       * </pre>
       */
      public Builder setBlockHeaderSize(long value) {
        bitField0_ |= 0x00000001;
        blockHeaderSize_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional fixed64 block_header_size = 1;</code>
       *
       * <pre>
       * in bytes
       * </pre>
       */
      public Builder clearBlockHeaderSize() {
        bitField0_ = (bitField0_ & ~0x00000001);
        blockHeaderSize_ = 0L;
        onChanged();
        return this;
      }

      // optional fixed64 block_footer_size = 2;
      private long blockFooterSize_ ;
      /**
       * <code>optional fixed64 block_footer_size = 2;</code>
       *
       * <pre>
       * in bytes
       * </pre>
       */
      public boolean hasBlockFooterSize() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>optional fixed64 block_footer_size = 2;</code>
       *
       * <pre>
       * in bytes
       * </pre>
       */
      public long getBlockFooterSize() {
        return blockFooterSize_;
      }
      /**
       * <code>optional fixed64 block_footer_size = 2;</code>
       *
       * <pre>
       * in bytes
       * </pre>
       */
      public Builder setBlockFooterSize(long value) {
        bitField0_ |= 0x00000002;
        blockFooterSize_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional fixed64 block_footer_size = 2;</code>
       *
       * <pre>
       * in bytes
       * </pre>
       */
      public Builder clearBlockFooterSize() {
        bitField0_ = (bitField0_ & ~0x00000002);
        blockFooterSize_ = 0L;
        onChanged();
        return this;
      }

      // optional fixed64 chunk_header_size = 3;
      private long chunkHeaderSize_ ;
      /**
       * <code>optional fixed64 chunk_header_size = 3;</code>
       *
       * <pre>
       * in bytes
       * </pre>
       */
      public boolean hasChunkHeaderSize() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <code>optional fixed64 chunk_header_size = 3;</code>
       *
       * <pre>
       * in bytes
       * </pre>
       */
      public long getChunkHeaderSize() {
        return chunkHeaderSize_;
      }
      /**
       * <code>optional fixed64 chunk_header_size = 3;</code>
       *
       * <pre>
       * in bytes
       * </pre>
       */
      public Builder setChunkHeaderSize(long value) {
        bitField0_ |= 0x00000004;
        chunkHeaderSize_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional fixed64 chunk_header_size = 3;</code>
       *
       * <pre>
       * in bytes
       * </pre>
       */
      public Builder clearChunkHeaderSize() {
        bitField0_ = (bitField0_ & ~0x00000004);
        chunkHeaderSize_ = 0L;
        onChanged();
        return this;
      }

      // optional fixed64 chunk_size = 4;
      private long chunkSize_ ;
      /**
       * <code>optional fixed64 chunk_size = 4;</code>
       *
       * <pre>
       * in bytes
       * </pre>
       */
      public boolean hasChunkSize() {
        return ((bitField0_ & 0x00000008) == 0x00000008);
      }
      /**
       * <code>optional fixed64 chunk_size = 4;</code>
       *
       * <pre>
       * in bytes
       * </pre>
       */
      public long getChunkSize() {
        return chunkSize_;
      }
      /**
       * <code>optional fixed64 chunk_size = 4;</code>
       *
       * <pre>
       * in bytes
       * </pre>
       */
      public Builder setChunkSize(long value) {
        bitField0_ |= 0x00000008;
        chunkSize_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional fixed64 chunk_size = 4;</code>
       *
       * <pre>
       * in bytes
       * </pre>
       */
      public Builder clearChunkSize() {
        bitField0_ = (bitField0_ & ~0x00000008);
        chunkSize_ = 0L;
        onChanged();
        return this;
      }

      // optional fixed64 chunk_footer_size = 5;
      private long chunkFooterSize_ ;
      /**
       * <code>optional fixed64 chunk_footer_size = 5;</code>
       *
       * <pre>
       * in bytes
       * </pre>
       */
      public boolean hasChunkFooterSize() {
        return ((bitField0_ & 0x00000010) == 0x00000010);
      }
      /**
       * <code>optional fixed64 chunk_footer_size = 5;</code>
       *
       * <pre>
       * in bytes
       * </pre>
       */
      public long getChunkFooterSize() {
        return chunkFooterSize_;
      }
      /**
       * <code>optional fixed64 chunk_footer_size = 5;</code>
       *
       * <pre>
       * in bytes
       * </pre>
       */
      public Builder setChunkFooterSize(long value) {
        bitField0_ |= 0x00000010;
        chunkFooterSize_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional fixed64 chunk_footer_size = 5;</code>
       *
       * <pre>
       * in bytes
       * </pre>
       */
      public Builder clearChunkFooterSize() {
        bitField0_ = (bitField0_ & ~0x00000010);
        chunkFooterSize_ = 0L;
        onChanged();
        return this;
      }

      // optional fixed64 physical_block_size = 6;
      private long physicalBlockSize_ ;
      /**
       * <code>optional fixed64 physical_block_size = 6;</code>
       *
       * <pre>
       * in bytes
       * </pre>
       */
      public boolean hasPhysicalBlockSize() {
        return ((bitField0_ & 0x00000020) == 0x00000020);
      }
      /**
       * <code>optional fixed64 physical_block_size = 6;</code>
       *
       * <pre>
       * in bytes
       * </pre>
       */
      public long getPhysicalBlockSize() {
        return physicalBlockSize_;
      }
      /**
       * <code>optional fixed64 physical_block_size = 6;</code>
       *
       * <pre>
       * in bytes
       * </pre>
       */
      public Builder setPhysicalBlockSize(long value) {
        bitField0_ |= 0x00000020;
        physicalBlockSize_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional fixed64 physical_block_size = 6;</code>
       *
       * <pre>
       * in bytes
       * </pre>
       */
      public Builder clearPhysicalBlockSize() {
        bitField0_ = (bitField0_ & ~0x00000020);
        physicalBlockSize_ = 0L;
        onChanged();
        return this;
      }

      // optional fixed64 encryption_id = 7;
      private long encryptionId_ ;
      /**
       * <code>optional fixed64 encryption_id = 7;</code>
       */
      public boolean hasEncryptionId() {
        return ((bitField0_ & 0x00000040) == 0x00000040);
      }
      /**
       * <code>optional fixed64 encryption_id = 7;</code>
       */
      public long getEncryptionId() {
        return encryptionId_;
      }
      /**
       * <code>optional fixed64 encryption_id = 7;</code>
       */
      public Builder setEncryptionId(long value) {
        bitField0_ |= 0x00000040;
        encryptionId_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional fixed64 encryption_id = 7;</code>
       */
      public Builder clearEncryptionId() {
        bitField0_ = (bitField0_ & ~0x00000040);
        encryptionId_ = 0L;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:alluxio.proto.layout.FileMetadata)
    }

    static {
      defaultInstance = new FileMetadata(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:alluxio.proto.layout.FileMetadata)
  }

  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_proto_layout_FileMetadata_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_alluxio_proto_layout_FileMetadata_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\030layout/file_footer.proto\022\024alluxio.prot" +
      "o.layout\"\302\001\n\014FileMetadata\022\031\n\021block_heade" +
      "r_size\030\001 \001(\006\022\031\n\021block_footer_size\030\002 \001(\006\022" +
      "\031\n\021chunk_header_size\030\003 \001(\006\022\022\n\nchunk_size" +
      "\030\004 \001(\006\022\031\n\021chunk_footer_size\030\005 \001(\006\022\033\n\023phy" +
      "sical_block_size\030\006 \001(\006\022\025\n\rencryption_id\030" +
      "\007 \001(\006"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_alluxio_proto_layout_FileMetadata_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_alluxio_proto_layout_FileMetadata_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_alluxio_proto_layout_FileMetadata_descriptor,
              new java.lang.String[] { "BlockHeaderSize", "BlockFooterSize", "ChunkHeaderSize", "ChunkSize", "ChunkFooterSize", "PhysicalBlockSize", "EncryptionId", });
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }

  // @@protoc_insertion_point(outer_class_scope)
}