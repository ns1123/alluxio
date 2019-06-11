// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/block_worker.proto

package alluxio.grpc;

/**
 * <pre>
 * Request for caching a block asynchronously
 * next available id: 6
 * </pre>
 *
 * Protobuf type {@code alluxio.grpc.block.AsyncCacheRequest}
 */
public  final class AsyncCacheRequest extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.block.AsyncCacheRequest)
    AsyncCacheRequestOrBuilder {
private static final long serialVersionUID = 0L;
  // Use AsyncCacheRequest.newBuilder() to construct.
  private AsyncCacheRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private AsyncCacheRequest() {
    blockId_ = 0L;
    sourceHost_ = "";
    sourcePort_ = 0;
    length_ = 0L;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private AsyncCacheRequest(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    this();
    if (extensionRegistry == null) {
      throw new java.lang.NullPointerException();
    }
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
            if (!parseUnknownField(
                input, unknownFields, extensionRegistry, tag)) {
              done = true;
            }
            break;
          }
          case 8: {
            bitField0_ |= 0x00000001;
            blockId_ = input.readInt64();
            break;
          }
          case 18: {
            com.google.protobuf.ByteString bs = input.readBytes();
            bitField0_ |= 0x00000002;
            sourceHost_ = bs;
            break;
          }
          case 24: {
            bitField0_ |= 0x00000004;
            sourcePort_ = input.readInt32();
            break;
          }
          case 34: {
            alluxio.proto.dataserver.Protocol.OpenUfsBlockOptions.Builder subBuilder = null;
            if (((bitField0_ & 0x00000008) == 0x00000008)) {
              subBuilder = openUfsBlockOptions_.toBuilder();
            }
            openUfsBlockOptions_ = input.readMessage(alluxio.proto.dataserver.Protocol.OpenUfsBlockOptions.PARSER, extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom(openUfsBlockOptions_);
              openUfsBlockOptions_ = subBuilder.buildPartial();
            }
            bitField0_ |= 0x00000008;
            break;
          }
          case 40: {
            bitField0_ |= 0x00000010;
            length_ = input.readInt64();
            break;
          }
        }
      }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw e.setUnfinishedMessage(this);
    } catch (java.io.IOException e) {
      throw new com.google.protobuf.InvalidProtocolBufferException(
          e).setUnfinishedMessage(this);
    } finally {
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return alluxio.grpc.BlockWorkerProto.internal_static_alluxio_grpc_block_AsyncCacheRequest_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.BlockWorkerProto.internal_static_alluxio_grpc_block_AsyncCacheRequest_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.AsyncCacheRequest.class, alluxio.grpc.AsyncCacheRequest.Builder.class);
  }

  private int bitField0_;
  public static final int BLOCK_ID_FIELD_NUMBER = 1;
  private long blockId_;
  /**
   * <code>optional int64 block_id = 1;</code>
   */
  public boolean hasBlockId() {
    return ((bitField0_ & 0x00000001) == 0x00000001);
  }
  /**
   * <code>optional int64 block_id = 1;</code>
   */
  public long getBlockId() {
    return blockId_;
  }

  public static final int SOURCE_HOST_FIELD_NUMBER = 2;
  private volatile java.lang.Object sourceHost_;
  /**
   * <pre>
   * TODO(calvin): source host and port should be replace with WorkerNetAddress
   * </pre>
   *
   * <code>optional string source_host = 2;</code>
   */
  public boolean hasSourceHost() {
    return ((bitField0_ & 0x00000002) == 0x00000002);
  }
  /**
   * <pre>
   * TODO(calvin): source host and port should be replace with WorkerNetAddress
   * </pre>
   *
   * <code>optional string source_host = 2;</code>
   */
  public java.lang.String getSourceHost() {
    java.lang.Object ref = sourceHost_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (bs.isValidUtf8()) {
        sourceHost_ = s;
      }
      return s;
    }
  }
  /**
   * <pre>
   * TODO(calvin): source host and port should be replace with WorkerNetAddress
   * </pre>
   *
   * <code>optional string source_host = 2;</code>
   */
  public com.google.protobuf.ByteString
      getSourceHostBytes() {
    java.lang.Object ref = sourceHost_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      sourceHost_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int SOURCE_PORT_FIELD_NUMBER = 3;
  private int sourcePort_;
  /**
   * <code>optional int32 source_port = 3;</code>
   */
  public boolean hasSourcePort() {
    return ((bitField0_ & 0x00000004) == 0x00000004);
  }
  /**
   * <code>optional int32 source_port = 3;</code>
   */
  public int getSourcePort() {
    return sourcePort_;
  }

  public static final int OPEN_UFS_BLOCK_OPTIONS_FIELD_NUMBER = 4;
  private alluxio.proto.dataserver.Protocol.OpenUfsBlockOptions openUfsBlockOptions_;
  /**
   * <code>optional .alluxio.proto.dataserver.OpenUfsBlockOptions open_ufs_block_options = 4;</code>
   */
  public boolean hasOpenUfsBlockOptions() {
    return ((bitField0_ & 0x00000008) == 0x00000008);
  }
  /**
   * <code>optional .alluxio.proto.dataserver.OpenUfsBlockOptions open_ufs_block_options = 4;</code>
   */
  public alluxio.proto.dataserver.Protocol.OpenUfsBlockOptions getOpenUfsBlockOptions() {
    return openUfsBlockOptions_ == null ? alluxio.proto.dataserver.Protocol.OpenUfsBlockOptions.getDefaultInstance() : openUfsBlockOptions_;
  }
  /**
   * <code>optional .alluxio.proto.dataserver.OpenUfsBlockOptions open_ufs_block_options = 4;</code>
   */
  public alluxio.proto.dataserver.Protocol.OpenUfsBlockOptionsOrBuilder getOpenUfsBlockOptionsOrBuilder() {
    return openUfsBlockOptions_ == null ? alluxio.proto.dataserver.Protocol.OpenUfsBlockOptions.getDefaultInstance() : openUfsBlockOptions_;
  }

  public static final int LENGTH_FIELD_NUMBER = 5;
  private long length_;
  /**
   * <code>optional int64 length = 5;</code>
   */
  public boolean hasLength() {
    return ((bitField0_ & 0x00000010) == 0x00000010);
  }
  /**
   * <code>optional int64 length = 5;</code>
   */
  public long getLength() {
    return length_;
  }

  private byte memoizedIsInitialized = -1;
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    if (hasOpenUfsBlockOptions()) {
      if (!getOpenUfsBlockOptions().isInitialized()) {
        memoizedIsInitialized = 0;
        return false;
      }
    }
    memoizedIsInitialized = 1;
    return true;
  }

  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      output.writeInt64(1, blockId_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 2, sourceHost_);
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      output.writeInt32(3, sourcePort_);
    }
    if (((bitField0_ & 0x00000008) == 0x00000008)) {
      output.writeMessage(4, getOpenUfsBlockOptions());
    }
    if (((bitField0_ & 0x00000010) == 0x00000010)) {
      output.writeInt64(5, length_);
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(1, blockId_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, sourceHost_);
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt32Size(3, sourcePort_);
    }
    if (((bitField0_ & 0x00000008) == 0x00000008)) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(4, getOpenUfsBlockOptions());
    }
    if (((bitField0_ & 0x00000010) == 0x00000010)) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(5, length_);
    }
    size += unknownFields.getSerializedSize();
    memoizedSize = size;
    return size;
  }

  @java.lang.Override
  public boolean equals(final java.lang.Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof alluxio.grpc.AsyncCacheRequest)) {
      return super.equals(obj);
    }
    alluxio.grpc.AsyncCacheRequest other = (alluxio.grpc.AsyncCacheRequest) obj;

    boolean result = true;
    result = result && (hasBlockId() == other.hasBlockId());
    if (hasBlockId()) {
      result = result && (getBlockId()
          == other.getBlockId());
    }
    result = result && (hasSourceHost() == other.hasSourceHost());
    if (hasSourceHost()) {
      result = result && getSourceHost()
          .equals(other.getSourceHost());
    }
    result = result && (hasSourcePort() == other.hasSourcePort());
    if (hasSourcePort()) {
      result = result && (getSourcePort()
          == other.getSourcePort());
    }
    result = result && (hasOpenUfsBlockOptions() == other.hasOpenUfsBlockOptions());
    if (hasOpenUfsBlockOptions()) {
      result = result && getOpenUfsBlockOptions()
          .equals(other.getOpenUfsBlockOptions());
    }
    result = result && (hasLength() == other.hasLength());
    if (hasLength()) {
      result = result && (getLength()
          == other.getLength());
    }
    result = result && unknownFields.equals(other.unknownFields);
    return result;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptor().hashCode();
    if (hasBlockId()) {
      hash = (37 * hash) + BLOCK_ID_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
          getBlockId());
    }
    if (hasSourceHost()) {
      hash = (37 * hash) + SOURCE_HOST_FIELD_NUMBER;
      hash = (53 * hash) + getSourceHost().hashCode();
    }
    if (hasSourcePort()) {
      hash = (37 * hash) + SOURCE_PORT_FIELD_NUMBER;
      hash = (53 * hash) + getSourcePort();
    }
    if (hasOpenUfsBlockOptions()) {
      hash = (37 * hash) + OPEN_UFS_BLOCK_OPTIONS_FIELD_NUMBER;
      hash = (53 * hash) + getOpenUfsBlockOptions().hashCode();
    }
    if (hasLength()) {
      hash = (37 * hash) + LENGTH_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
          getLength());
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.AsyncCacheRequest parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.AsyncCacheRequest parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.AsyncCacheRequest parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.AsyncCacheRequest parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.AsyncCacheRequest parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.AsyncCacheRequest parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.AsyncCacheRequest parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.AsyncCacheRequest parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.AsyncCacheRequest parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.AsyncCacheRequest parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.AsyncCacheRequest parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.AsyncCacheRequest parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(alluxio.grpc.AsyncCacheRequest prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE
        ? new Builder() : new Builder().mergeFrom(this);
  }

  @java.lang.Override
  protected Builder newBuilderForType(
      com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * <pre>
   * Request for caching a block asynchronously
   * next available id: 6
   * </pre>
   *
   * Protobuf type {@code alluxio.grpc.block.AsyncCacheRequest}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.block.AsyncCacheRequest)
      alluxio.grpc.AsyncCacheRequestOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.BlockWorkerProto.internal_static_alluxio_grpc_block_AsyncCacheRequest_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.BlockWorkerProto.internal_static_alluxio_grpc_block_AsyncCacheRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.AsyncCacheRequest.class, alluxio.grpc.AsyncCacheRequest.Builder.class);
    }

    // Construct using alluxio.grpc.AsyncCacheRequest.newBuilder()
    private Builder() {
      maybeForceBuilderInitialization();
    }

    private Builder(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      super(parent);
      maybeForceBuilderInitialization();
    }
    private void maybeForceBuilderInitialization() {
      if (com.google.protobuf.GeneratedMessageV3
              .alwaysUseFieldBuilders) {
        getOpenUfsBlockOptionsFieldBuilder();
      }
    }
    public Builder clear() {
      super.clear();
      blockId_ = 0L;
      bitField0_ = (bitField0_ & ~0x00000001);
      sourceHost_ = "";
      bitField0_ = (bitField0_ & ~0x00000002);
      sourcePort_ = 0;
      bitField0_ = (bitField0_ & ~0x00000004);
      if (openUfsBlockOptionsBuilder_ == null) {
        openUfsBlockOptions_ = null;
      } else {
        openUfsBlockOptionsBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000008);
      length_ = 0L;
      bitField0_ = (bitField0_ & ~0x00000010);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.BlockWorkerProto.internal_static_alluxio_grpc_block_AsyncCacheRequest_descriptor;
    }

    public alluxio.grpc.AsyncCacheRequest getDefaultInstanceForType() {
      return alluxio.grpc.AsyncCacheRequest.getDefaultInstance();
    }

    public alluxio.grpc.AsyncCacheRequest build() {
      alluxio.grpc.AsyncCacheRequest result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.AsyncCacheRequest buildPartial() {
      alluxio.grpc.AsyncCacheRequest result = new alluxio.grpc.AsyncCacheRequest(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
        to_bitField0_ |= 0x00000001;
      }
      result.blockId_ = blockId_;
      if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
        to_bitField0_ |= 0x00000002;
      }
      result.sourceHost_ = sourceHost_;
      if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
        to_bitField0_ |= 0x00000004;
      }
      result.sourcePort_ = sourcePort_;
      if (((from_bitField0_ & 0x00000008) == 0x00000008)) {
        to_bitField0_ |= 0x00000008;
      }
      if (openUfsBlockOptionsBuilder_ == null) {
        result.openUfsBlockOptions_ = openUfsBlockOptions_;
      } else {
        result.openUfsBlockOptions_ = openUfsBlockOptionsBuilder_.build();
      }
      if (((from_bitField0_ & 0x00000010) == 0x00000010)) {
        to_bitField0_ |= 0x00000010;
      }
      result.length_ = length_;
      result.bitField0_ = to_bitField0_;
      onBuilt();
      return result;
    }

    public Builder clone() {
      return (Builder) super.clone();
    }
    public Builder setField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return (Builder) super.setField(field, value);
    }
    public Builder clearField(
        com.google.protobuf.Descriptors.FieldDescriptor field) {
      return (Builder) super.clearField(field);
    }
    public Builder clearOneof(
        com.google.protobuf.Descriptors.OneofDescriptor oneof) {
      return (Builder) super.clearOneof(oneof);
    }
    public Builder setRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        int index, java.lang.Object value) {
      return (Builder) super.setRepeatedField(field, index, value);
    }
    public Builder addRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return (Builder) super.addRepeatedField(field, value);
    }
    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof alluxio.grpc.AsyncCacheRequest) {
        return mergeFrom((alluxio.grpc.AsyncCacheRequest)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.AsyncCacheRequest other) {
      if (other == alluxio.grpc.AsyncCacheRequest.getDefaultInstance()) return this;
      if (other.hasBlockId()) {
        setBlockId(other.getBlockId());
      }
      if (other.hasSourceHost()) {
        bitField0_ |= 0x00000002;
        sourceHost_ = other.sourceHost_;
        onChanged();
      }
      if (other.hasSourcePort()) {
        setSourcePort(other.getSourcePort());
      }
      if (other.hasOpenUfsBlockOptions()) {
        mergeOpenUfsBlockOptions(other.getOpenUfsBlockOptions());
      }
      if (other.hasLength()) {
        setLength(other.getLength());
      }
      this.mergeUnknownFields(other.unknownFields);
      onChanged();
      return this;
    }

    public final boolean isInitialized() {
      if (hasOpenUfsBlockOptions()) {
        if (!getOpenUfsBlockOptions().isInitialized()) {
          return false;
        }
      }
      return true;
    }

    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      alluxio.grpc.AsyncCacheRequest parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.AsyncCacheRequest) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private long blockId_ ;
    /**
     * <code>optional int64 block_id = 1;</code>
     */
    public boolean hasBlockId() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional int64 block_id = 1;</code>
     */
    public long getBlockId() {
      return blockId_;
    }
    /**
     * <code>optional int64 block_id = 1;</code>
     */
    public Builder setBlockId(long value) {
      bitField0_ |= 0x00000001;
      blockId_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int64 block_id = 1;</code>
     */
    public Builder clearBlockId() {
      bitField0_ = (bitField0_ & ~0x00000001);
      blockId_ = 0L;
      onChanged();
      return this;
    }

    private java.lang.Object sourceHost_ = "";
    /**
     * <pre>
     * TODO(calvin): source host and port should be replace with WorkerNetAddress
     * </pre>
     *
     * <code>optional string source_host = 2;</code>
     */
    public boolean hasSourceHost() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <pre>
     * TODO(calvin): source host and port should be replace with WorkerNetAddress
     * </pre>
     *
     * <code>optional string source_host = 2;</code>
     */
    public java.lang.String getSourceHost() {
      java.lang.Object ref = sourceHost_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          sourceHost_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <pre>
     * TODO(calvin): source host and port should be replace with WorkerNetAddress
     * </pre>
     *
     * <code>optional string source_host = 2;</code>
     */
    public com.google.protobuf.ByteString
        getSourceHostBytes() {
      java.lang.Object ref = sourceHost_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        sourceHost_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <pre>
     * TODO(calvin): source host and port should be replace with WorkerNetAddress
     * </pre>
     *
     * <code>optional string source_host = 2;</code>
     */
    public Builder setSourceHost(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
      sourceHost_ = value;
      onChanged();
      return this;
    }
    /**
     * <pre>
     * TODO(calvin): source host and port should be replace with WorkerNetAddress
     * </pre>
     *
     * <code>optional string source_host = 2;</code>
     */
    public Builder clearSourceHost() {
      bitField0_ = (bitField0_ & ~0x00000002);
      sourceHost_ = getDefaultInstance().getSourceHost();
      onChanged();
      return this;
    }
    /**
     * <pre>
     * TODO(calvin): source host and port should be replace with WorkerNetAddress
     * </pre>
     *
     * <code>optional string source_host = 2;</code>
     */
    public Builder setSourceHostBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
      sourceHost_ = value;
      onChanged();
      return this;
    }

    private int sourcePort_ ;
    /**
     * <code>optional int32 source_port = 3;</code>
     */
    public boolean hasSourcePort() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional int32 source_port = 3;</code>
     */
    public int getSourcePort() {
      return sourcePort_;
    }
    /**
     * <code>optional int32 source_port = 3;</code>
     */
    public Builder setSourcePort(int value) {
      bitField0_ |= 0x00000004;
      sourcePort_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int32 source_port = 3;</code>
     */
    public Builder clearSourcePort() {
      bitField0_ = (bitField0_ & ~0x00000004);
      sourcePort_ = 0;
      onChanged();
      return this;
    }

    private alluxio.proto.dataserver.Protocol.OpenUfsBlockOptions openUfsBlockOptions_ = null;
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.proto.dataserver.Protocol.OpenUfsBlockOptions, alluxio.proto.dataserver.Protocol.OpenUfsBlockOptions.Builder, alluxio.proto.dataserver.Protocol.OpenUfsBlockOptionsOrBuilder> openUfsBlockOptionsBuilder_;
    /**
     * <code>optional .alluxio.proto.dataserver.OpenUfsBlockOptions open_ufs_block_options = 4;</code>
     */
    public boolean hasOpenUfsBlockOptions() {
      return ((bitField0_ & 0x00000008) == 0x00000008);
    }
    /**
     * <code>optional .alluxio.proto.dataserver.OpenUfsBlockOptions open_ufs_block_options = 4;</code>
     */
    public alluxio.proto.dataserver.Protocol.OpenUfsBlockOptions getOpenUfsBlockOptions() {
      if (openUfsBlockOptionsBuilder_ == null) {
        return openUfsBlockOptions_ == null ? alluxio.proto.dataserver.Protocol.OpenUfsBlockOptions.getDefaultInstance() : openUfsBlockOptions_;
      } else {
        return openUfsBlockOptionsBuilder_.getMessage();
      }
    }
    /**
     * <code>optional .alluxio.proto.dataserver.OpenUfsBlockOptions open_ufs_block_options = 4;</code>
     */
    public Builder setOpenUfsBlockOptions(alluxio.proto.dataserver.Protocol.OpenUfsBlockOptions value) {
      if (openUfsBlockOptionsBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        openUfsBlockOptions_ = value;
        onChanged();
      } else {
        openUfsBlockOptionsBuilder_.setMessage(value);
      }
      bitField0_ |= 0x00000008;
      return this;
    }
    /**
     * <code>optional .alluxio.proto.dataserver.OpenUfsBlockOptions open_ufs_block_options = 4;</code>
     */
    public Builder setOpenUfsBlockOptions(
        alluxio.proto.dataserver.Protocol.OpenUfsBlockOptions.Builder builderForValue) {
      if (openUfsBlockOptionsBuilder_ == null) {
        openUfsBlockOptions_ = builderForValue.build();
        onChanged();
      } else {
        openUfsBlockOptionsBuilder_.setMessage(builderForValue.build());
      }
      bitField0_ |= 0x00000008;
      return this;
    }
    /**
     * <code>optional .alluxio.proto.dataserver.OpenUfsBlockOptions open_ufs_block_options = 4;</code>
     */
    public Builder mergeOpenUfsBlockOptions(alluxio.proto.dataserver.Protocol.OpenUfsBlockOptions value) {
      if (openUfsBlockOptionsBuilder_ == null) {
        if (((bitField0_ & 0x00000008) == 0x00000008) &&
            openUfsBlockOptions_ != null &&
            openUfsBlockOptions_ != alluxio.proto.dataserver.Protocol.OpenUfsBlockOptions.getDefaultInstance()) {
          openUfsBlockOptions_ =
            alluxio.proto.dataserver.Protocol.OpenUfsBlockOptions.newBuilder(openUfsBlockOptions_).mergeFrom(value).buildPartial();
        } else {
          openUfsBlockOptions_ = value;
        }
        onChanged();
      } else {
        openUfsBlockOptionsBuilder_.mergeFrom(value);
      }
      bitField0_ |= 0x00000008;
      return this;
    }
    /**
     * <code>optional .alluxio.proto.dataserver.OpenUfsBlockOptions open_ufs_block_options = 4;</code>
     */
    public Builder clearOpenUfsBlockOptions() {
      if (openUfsBlockOptionsBuilder_ == null) {
        openUfsBlockOptions_ = null;
        onChanged();
      } else {
        openUfsBlockOptionsBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000008);
      return this;
    }
    /**
     * <code>optional .alluxio.proto.dataserver.OpenUfsBlockOptions open_ufs_block_options = 4;</code>
     */
    public alluxio.proto.dataserver.Protocol.OpenUfsBlockOptions.Builder getOpenUfsBlockOptionsBuilder() {
      bitField0_ |= 0x00000008;
      onChanged();
      return getOpenUfsBlockOptionsFieldBuilder().getBuilder();
    }
    /**
     * <code>optional .alluxio.proto.dataserver.OpenUfsBlockOptions open_ufs_block_options = 4;</code>
     */
    public alluxio.proto.dataserver.Protocol.OpenUfsBlockOptionsOrBuilder getOpenUfsBlockOptionsOrBuilder() {
      if (openUfsBlockOptionsBuilder_ != null) {
        return openUfsBlockOptionsBuilder_.getMessageOrBuilder();
      } else {
        return openUfsBlockOptions_ == null ?
            alluxio.proto.dataserver.Protocol.OpenUfsBlockOptions.getDefaultInstance() : openUfsBlockOptions_;
      }
    }
    /**
     * <code>optional .alluxio.proto.dataserver.OpenUfsBlockOptions open_ufs_block_options = 4;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.proto.dataserver.Protocol.OpenUfsBlockOptions, alluxio.proto.dataserver.Protocol.OpenUfsBlockOptions.Builder, alluxio.proto.dataserver.Protocol.OpenUfsBlockOptionsOrBuilder> 
        getOpenUfsBlockOptionsFieldBuilder() {
      if (openUfsBlockOptionsBuilder_ == null) {
        openUfsBlockOptionsBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            alluxio.proto.dataserver.Protocol.OpenUfsBlockOptions, alluxio.proto.dataserver.Protocol.OpenUfsBlockOptions.Builder, alluxio.proto.dataserver.Protocol.OpenUfsBlockOptionsOrBuilder>(
                getOpenUfsBlockOptions(),
                getParentForChildren(),
                isClean());
        openUfsBlockOptions_ = null;
      }
      return openUfsBlockOptionsBuilder_;
    }

    private long length_ ;
    /**
     * <code>optional int64 length = 5;</code>
     */
    public boolean hasLength() {
      return ((bitField0_ & 0x00000010) == 0x00000010);
    }
    /**
     * <code>optional int64 length = 5;</code>
     */
    public long getLength() {
      return length_;
    }
    /**
     * <code>optional int64 length = 5;</code>
     */
    public Builder setLength(long value) {
      bitField0_ |= 0x00000010;
      length_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int64 length = 5;</code>
     */
    public Builder clearLength() {
      bitField0_ = (bitField0_ & ~0x00000010);
      length_ = 0L;
      onChanged();
      return this;
    }
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.block.AsyncCacheRequest)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.block.AsyncCacheRequest)
  private static final alluxio.grpc.AsyncCacheRequest DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.AsyncCacheRequest();
  }

  public static alluxio.grpc.AsyncCacheRequest getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<AsyncCacheRequest>
      PARSER = new com.google.protobuf.AbstractParser<AsyncCacheRequest>() {
    public AsyncCacheRequest parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new AsyncCacheRequest(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<AsyncCacheRequest> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<AsyncCacheRequest> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.AsyncCacheRequest getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

