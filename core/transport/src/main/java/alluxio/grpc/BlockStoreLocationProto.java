// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/common.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.BlockStoreLocationProto}
 */
public  final class BlockStoreLocationProto extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.BlockStoreLocationProto)
    BlockStoreLocationProtoOrBuilder {
private static final long serialVersionUID = 0L;
  // Use BlockStoreLocationProto.newBuilder() to construct.
  private BlockStoreLocationProto(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private BlockStoreLocationProto() {
    tierAlias_ = "";
    mediumType_ = "";
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private BlockStoreLocationProto(
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
          case 10: {
            com.google.protobuf.ByteString bs = input.readBytes();
            bitField0_ |= 0x00000001;
            tierAlias_ = bs;
            break;
          }
          case 18: {
            com.google.protobuf.ByteString bs = input.readBytes();
            bitField0_ |= 0x00000002;
            mediumType_ = bs;
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
    return alluxio.grpc.CommonProto.internal_static_alluxio_grpc_BlockStoreLocationProto_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.CommonProto.internal_static_alluxio_grpc_BlockStoreLocationProto_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.BlockStoreLocationProto.class, alluxio.grpc.BlockStoreLocationProto.Builder.class);
  }

  private int bitField0_;
  public static final int TIERALIAS_FIELD_NUMBER = 1;
  private volatile java.lang.Object tierAlias_;
  /**
   * <code>optional string tierAlias = 1;</code>
   */
  public boolean hasTierAlias() {
    return ((bitField0_ & 0x00000001) == 0x00000001);
  }
  /**
   * <code>optional string tierAlias = 1;</code>
   */
  public java.lang.String getTierAlias() {
    java.lang.Object ref = tierAlias_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (bs.isValidUtf8()) {
        tierAlias_ = s;
      }
      return s;
    }
  }
  /**
   * <code>optional string tierAlias = 1;</code>
   */
  public com.google.protobuf.ByteString
      getTierAliasBytes() {
    java.lang.Object ref = tierAlias_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      tierAlias_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int MEDIUMTYPE_FIELD_NUMBER = 2;
  private volatile java.lang.Object mediumType_;
  /**
   * <code>optional string mediumType = 2;</code>
   */
  public boolean hasMediumType() {
    return ((bitField0_ & 0x00000002) == 0x00000002);
  }
  /**
   * <code>optional string mediumType = 2;</code>
   */
  public java.lang.String getMediumType() {
    java.lang.Object ref = mediumType_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (bs.isValidUtf8()) {
        mediumType_ = s;
      }
      return s;
    }
  }
  /**
   * <code>optional string mediumType = 2;</code>
   */
  public com.google.protobuf.ByteString
      getMediumTypeBytes() {
    java.lang.Object ref = mediumType_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      mediumType_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  private byte memoizedIsInitialized = -1;
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, tierAlias_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 2, mediumType_);
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, tierAlias_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, mediumType_);
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
    if (!(obj instanceof alluxio.grpc.BlockStoreLocationProto)) {
      return super.equals(obj);
    }
    alluxio.grpc.BlockStoreLocationProto other = (alluxio.grpc.BlockStoreLocationProto) obj;

    boolean result = true;
    result = result && (hasTierAlias() == other.hasTierAlias());
    if (hasTierAlias()) {
      result = result && getTierAlias()
          .equals(other.getTierAlias());
    }
    result = result && (hasMediumType() == other.hasMediumType());
    if (hasMediumType()) {
      result = result && getMediumType()
          .equals(other.getMediumType());
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
    if (hasTierAlias()) {
      hash = (37 * hash) + TIERALIAS_FIELD_NUMBER;
      hash = (53 * hash) + getTierAlias().hashCode();
    }
    if (hasMediumType()) {
      hash = (37 * hash) + MEDIUMTYPE_FIELD_NUMBER;
      hash = (53 * hash) + getMediumType().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.BlockStoreLocationProto parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.BlockStoreLocationProto parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.BlockStoreLocationProto parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.BlockStoreLocationProto parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.BlockStoreLocationProto parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.BlockStoreLocationProto parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.BlockStoreLocationProto parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.BlockStoreLocationProto parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.BlockStoreLocationProto parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.BlockStoreLocationProto parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.BlockStoreLocationProto parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.BlockStoreLocationProto parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.BlockStoreLocationProto prototype) {
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
   * Protobuf type {@code alluxio.grpc.BlockStoreLocationProto}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.BlockStoreLocationProto)
      alluxio.grpc.BlockStoreLocationProtoOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.CommonProto.internal_static_alluxio_grpc_BlockStoreLocationProto_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.CommonProto.internal_static_alluxio_grpc_BlockStoreLocationProto_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.BlockStoreLocationProto.class, alluxio.grpc.BlockStoreLocationProto.Builder.class);
    }

    // Construct using alluxio.grpc.BlockStoreLocationProto.newBuilder()
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
      }
    }
    public Builder clear() {
      super.clear();
      tierAlias_ = "";
      bitField0_ = (bitField0_ & ~0x00000001);
      mediumType_ = "";
      bitField0_ = (bitField0_ & ~0x00000002);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.CommonProto.internal_static_alluxio_grpc_BlockStoreLocationProto_descriptor;
    }

    public alluxio.grpc.BlockStoreLocationProto getDefaultInstanceForType() {
      return alluxio.grpc.BlockStoreLocationProto.getDefaultInstance();
    }

    public alluxio.grpc.BlockStoreLocationProto build() {
      alluxio.grpc.BlockStoreLocationProto result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.BlockStoreLocationProto buildPartial() {
      alluxio.grpc.BlockStoreLocationProto result = new alluxio.grpc.BlockStoreLocationProto(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
        to_bitField0_ |= 0x00000001;
      }
      result.tierAlias_ = tierAlias_;
      if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
        to_bitField0_ |= 0x00000002;
      }
      result.mediumType_ = mediumType_;
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
      if (other instanceof alluxio.grpc.BlockStoreLocationProto) {
        return mergeFrom((alluxio.grpc.BlockStoreLocationProto)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.BlockStoreLocationProto other) {
      if (other == alluxio.grpc.BlockStoreLocationProto.getDefaultInstance()) return this;
      if (other.hasTierAlias()) {
        bitField0_ |= 0x00000001;
        tierAlias_ = other.tierAlias_;
        onChanged();
      }
      if (other.hasMediumType()) {
        bitField0_ |= 0x00000002;
        mediumType_ = other.mediumType_;
        onChanged();
      }
      this.mergeUnknownFields(other.unknownFields);
      onChanged();
      return this;
    }

    public final boolean isInitialized() {
      return true;
    }

    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      alluxio.grpc.BlockStoreLocationProto parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.BlockStoreLocationProto) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private java.lang.Object tierAlias_ = "";
    /**
     * <code>optional string tierAlias = 1;</code>
     */
    public boolean hasTierAlias() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional string tierAlias = 1;</code>
     */
    public java.lang.String getTierAlias() {
      java.lang.Object ref = tierAlias_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          tierAlias_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string tierAlias = 1;</code>
     */
    public com.google.protobuf.ByteString
        getTierAliasBytes() {
      java.lang.Object ref = tierAlias_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        tierAlias_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string tierAlias = 1;</code>
     */
    public Builder setTierAlias(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
      tierAlias_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string tierAlias = 1;</code>
     */
    public Builder clearTierAlias() {
      bitField0_ = (bitField0_ & ~0x00000001);
      tierAlias_ = getDefaultInstance().getTierAlias();
      onChanged();
      return this;
    }
    /**
     * <code>optional string tierAlias = 1;</code>
     */
    public Builder setTierAliasBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
      tierAlias_ = value;
      onChanged();
      return this;
    }

    private java.lang.Object mediumType_ = "";
    /**
     * <code>optional string mediumType = 2;</code>
     */
    public boolean hasMediumType() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional string mediumType = 2;</code>
     */
    public java.lang.String getMediumType() {
      java.lang.Object ref = mediumType_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          mediumType_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string mediumType = 2;</code>
     */
    public com.google.protobuf.ByteString
        getMediumTypeBytes() {
      java.lang.Object ref = mediumType_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        mediumType_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string mediumType = 2;</code>
     */
    public Builder setMediumType(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
      mediumType_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string mediumType = 2;</code>
     */
    public Builder clearMediumType() {
      bitField0_ = (bitField0_ & ~0x00000002);
      mediumType_ = getDefaultInstance().getMediumType();
      onChanged();
      return this;
    }
    /**
     * <code>optional string mediumType = 2;</code>
     */
    public Builder setMediumTypeBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
      mediumType_ = value;
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


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.BlockStoreLocationProto)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.BlockStoreLocationProto)
  private static final alluxio.grpc.BlockStoreLocationProto DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.BlockStoreLocationProto();
  }

  public static alluxio.grpc.BlockStoreLocationProto getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<BlockStoreLocationProto>
      PARSER = new com.google.protobuf.AbstractParser<BlockStoreLocationProto>() {
    public BlockStoreLocationProto parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new BlockStoreLocationProto(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<BlockStoreLocationProto> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<BlockStoreLocationProto> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.BlockStoreLocationProto getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

