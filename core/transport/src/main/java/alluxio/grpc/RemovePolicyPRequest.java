// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/policy_master.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.policy.RemovePolicyPRequest}
 */
public  final class RemovePolicyPRequest extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.policy.RemovePolicyPRequest)
    RemovePolicyPRequestOrBuilder {
private static final long serialVersionUID = 0L;
  // Use RemovePolicyPRequest.newBuilder() to construct.
  private RemovePolicyPRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private RemovePolicyPRequest() {
    policyName_ = "";
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private RemovePolicyPRequest(
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
            policyName_ = bs;
            break;
          }
          case 18: {
            alluxio.grpc.RemovePolicyPOptions.Builder subBuilder = null;
            if (((bitField0_ & 0x00000002) == 0x00000002)) {
              subBuilder = options_.toBuilder();
            }
            options_ = input.readMessage(alluxio.grpc.RemovePolicyPOptions.PARSER, extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom(options_);
              options_ = subBuilder.buildPartial();
            }
            bitField0_ |= 0x00000002;
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
    return alluxio.grpc.PolicyMasterProto.internal_static_alluxio_grpc_policy_RemovePolicyPRequest_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.PolicyMasterProto.internal_static_alluxio_grpc_policy_RemovePolicyPRequest_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.RemovePolicyPRequest.class, alluxio.grpc.RemovePolicyPRequest.Builder.class);
  }

  private int bitField0_;
  public static final int POLICYNAME_FIELD_NUMBER = 1;
  private volatile java.lang.Object policyName_;
  /**
   * <code>optional string policyName = 1;</code>
   */
  public boolean hasPolicyName() {
    return ((bitField0_ & 0x00000001) == 0x00000001);
  }
  /**
   * <code>optional string policyName = 1;</code>
   */
  public java.lang.String getPolicyName() {
    java.lang.Object ref = policyName_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (bs.isValidUtf8()) {
        policyName_ = s;
      }
      return s;
    }
  }
  /**
   * <code>optional string policyName = 1;</code>
   */
  public com.google.protobuf.ByteString
      getPolicyNameBytes() {
    java.lang.Object ref = policyName_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      policyName_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int OPTIONS_FIELD_NUMBER = 2;
  private alluxio.grpc.RemovePolicyPOptions options_;
  /**
   * <code>optional .alluxio.grpc.policy.RemovePolicyPOptions options = 2;</code>
   */
  public boolean hasOptions() {
    return ((bitField0_ & 0x00000002) == 0x00000002);
  }
  /**
   * <code>optional .alluxio.grpc.policy.RemovePolicyPOptions options = 2;</code>
   */
  public alluxio.grpc.RemovePolicyPOptions getOptions() {
    return options_ == null ? alluxio.grpc.RemovePolicyPOptions.getDefaultInstance() : options_;
  }
  /**
   * <code>optional .alluxio.grpc.policy.RemovePolicyPOptions options = 2;</code>
   */
  public alluxio.grpc.RemovePolicyPOptionsOrBuilder getOptionsOrBuilder() {
    return options_ == null ? alluxio.grpc.RemovePolicyPOptions.getDefaultInstance() : options_;
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
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, policyName_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      output.writeMessage(2, getOptions());
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, policyName_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(2, getOptions());
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
    if (!(obj instanceof alluxio.grpc.RemovePolicyPRequest)) {
      return super.equals(obj);
    }
    alluxio.grpc.RemovePolicyPRequest other = (alluxio.grpc.RemovePolicyPRequest) obj;

    boolean result = true;
    result = result && (hasPolicyName() == other.hasPolicyName());
    if (hasPolicyName()) {
      result = result && getPolicyName()
          .equals(other.getPolicyName());
    }
    result = result && (hasOptions() == other.hasOptions());
    if (hasOptions()) {
      result = result && getOptions()
          .equals(other.getOptions());
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
    if (hasPolicyName()) {
      hash = (37 * hash) + POLICYNAME_FIELD_NUMBER;
      hash = (53 * hash) + getPolicyName().hashCode();
    }
    if (hasOptions()) {
      hash = (37 * hash) + OPTIONS_FIELD_NUMBER;
      hash = (53 * hash) + getOptions().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.RemovePolicyPRequest parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.RemovePolicyPRequest parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.RemovePolicyPRequest parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.RemovePolicyPRequest parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.RemovePolicyPRequest parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.RemovePolicyPRequest parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.RemovePolicyPRequest parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.RemovePolicyPRequest parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.RemovePolicyPRequest parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.RemovePolicyPRequest parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.RemovePolicyPRequest parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.RemovePolicyPRequest parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.RemovePolicyPRequest prototype) {
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
   * Protobuf type {@code alluxio.grpc.policy.RemovePolicyPRequest}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.policy.RemovePolicyPRequest)
      alluxio.grpc.RemovePolicyPRequestOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.PolicyMasterProto.internal_static_alluxio_grpc_policy_RemovePolicyPRequest_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.PolicyMasterProto.internal_static_alluxio_grpc_policy_RemovePolicyPRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.RemovePolicyPRequest.class, alluxio.grpc.RemovePolicyPRequest.Builder.class);
    }

    // Construct using alluxio.grpc.RemovePolicyPRequest.newBuilder()
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
        getOptionsFieldBuilder();
      }
    }
    public Builder clear() {
      super.clear();
      policyName_ = "";
      bitField0_ = (bitField0_ & ~0x00000001);
      if (optionsBuilder_ == null) {
        options_ = null;
      } else {
        optionsBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000002);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.PolicyMasterProto.internal_static_alluxio_grpc_policy_RemovePolicyPRequest_descriptor;
    }

    public alluxio.grpc.RemovePolicyPRequest getDefaultInstanceForType() {
      return alluxio.grpc.RemovePolicyPRequest.getDefaultInstance();
    }

    public alluxio.grpc.RemovePolicyPRequest build() {
      alluxio.grpc.RemovePolicyPRequest result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.RemovePolicyPRequest buildPartial() {
      alluxio.grpc.RemovePolicyPRequest result = new alluxio.grpc.RemovePolicyPRequest(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
        to_bitField0_ |= 0x00000001;
      }
      result.policyName_ = policyName_;
      if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
        to_bitField0_ |= 0x00000002;
      }
      if (optionsBuilder_ == null) {
        result.options_ = options_;
      } else {
        result.options_ = optionsBuilder_.build();
      }
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
      if (other instanceof alluxio.grpc.RemovePolicyPRequest) {
        return mergeFrom((alluxio.grpc.RemovePolicyPRequest)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.RemovePolicyPRequest other) {
      if (other == alluxio.grpc.RemovePolicyPRequest.getDefaultInstance()) return this;
      if (other.hasPolicyName()) {
        bitField0_ |= 0x00000001;
        policyName_ = other.policyName_;
        onChanged();
      }
      if (other.hasOptions()) {
        mergeOptions(other.getOptions());
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
      alluxio.grpc.RemovePolicyPRequest parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.RemovePolicyPRequest) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private java.lang.Object policyName_ = "";
    /**
     * <code>optional string policyName = 1;</code>
     */
    public boolean hasPolicyName() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional string policyName = 1;</code>
     */
    public java.lang.String getPolicyName() {
      java.lang.Object ref = policyName_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          policyName_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string policyName = 1;</code>
     */
    public com.google.protobuf.ByteString
        getPolicyNameBytes() {
      java.lang.Object ref = policyName_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        policyName_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string policyName = 1;</code>
     */
    public Builder setPolicyName(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
      policyName_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string policyName = 1;</code>
     */
    public Builder clearPolicyName() {
      bitField0_ = (bitField0_ & ~0x00000001);
      policyName_ = getDefaultInstance().getPolicyName();
      onChanged();
      return this;
    }
    /**
     * <code>optional string policyName = 1;</code>
     */
    public Builder setPolicyNameBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
      policyName_ = value;
      onChanged();
      return this;
    }

    private alluxio.grpc.RemovePolicyPOptions options_ = null;
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.grpc.RemovePolicyPOptions, alluxio.grpc.RemovePolicyPOptions.Builder, alluxio.grpc.RemovePolicyPOptionsOrBuilder> optionsBuilder_;
    /**
     * <code>optional .alluxio.grpc.policy.RemovePolicyPOptions options = 2;</code>
     */
    public boolean hasOptions() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional .alluxio.grpc.policy.RemovePolicyPOptions options = 2;</code>
     */
    public alluxio.grpc.RemovePolicyPOptions getOptions() {
      if (optionsBuilder_ == null) {
        return options_ == null ? alluxio.grpc.RemovePolicyPOptions.getDefaultInstance() : options_;
      } else {
        return optionsBuilder_.getMessage();
      }
    }
    /**
     * <code>optional .alluxio.grpc.policy.RemovePolicyPOptions options = 2;</code>
     */
    public Builder setOptions(alluxio.grpc.RemovePolicyPOptions value) {
      if (optionsBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        options_ = value;
        onChanged();
      } else {
        optionsBuilder_.setMessage(value);
      }
      bitField0_ |= 0x00000002;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.policy.RemovePolicyPOptions options = 2;</code>
     */
    public Builder setOptions(
        alluxio.grpc.RemovePolicyPOptions.Builder builderForValue) {
      if (optionsBuilder_ == null) {
        options_ = builderForValue.build();
        onChanged();
      } else {
        optionsBuilder_.setMessage(builderForValue.build());
      }
      bitField0_ |= 0x00000002;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.policy.RemovePolicyPOptions options = 2;</code>
     */
    public Builder mergeOptions(alluxio.grpc.RemovePolicyPOptions value) {
      if (optionsBuilder_ == null) {
        if (((bitField0_ & 0x00000002) == 0x00000002) &&
            options_ != null &&
            options_ != alluxio.grpc.RemovePolicyPOptions.getDefaultInstance()) {
          options_ =
            alluxio.grpc.RemovePolicyPOptions.newBuilder(options_).mergeFrom(value).buildPartial();
        } else {
          options_ = value;
        }
        onChanged();
      } else {
        optionsBuilder_.mergeFrom(value);
      }
      bitField0_ |= 0x00000002;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.policy.RemovePolicyPOptions options = 2;</code>
     */
    public Builder clearOptions() {
      if (optionsBuilder_ == null) {
        options_ = null;
        onChanged();
      } else {
        optionsBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000002);
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.policy.RemovePolicyPOptions options = 2;</code>
     */
    public alluxio.grpc.RemovePolicyPOptions.Builder getOptionsBuilder() {
      bitField0_ |= 0x00000002;
      onChanged();
      return getOptionsFieldBuilder().getBuilder();
    }
    /**
     * <code>optional .alluxio.grpc.policy.RemovePolicyPOptions options = 2;</code>
     */
    public alluxio.grpc.RemovePolicyPOptionsOrBuilder getOptionsOrBuilder() {
      if (optionsBuilder_ != null) {
        return optionsBuilder_.getMessageOrBuilder();
      } else {
        return options_ == null ?
            alluxio.grpc.RemovePolicyPOptions.getDefaultInstance() : options_;
      }
    }
    /**
     * <code>optional .alluxio.grpc.policy.RemovePolicyPOptions options = 2;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.grpc.RemovePolicyPOptions, alluxio.grpc.RemovePolicyPOptions.Builder, alluxio.grpc.RemovePolicyPOptionsOrBuilder> 
        getOptionsFieldBuilder() {
      if (optionsBuilder_ == null) {
        optionsBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            alluxio.grpc.RemovePolicyPOptions, alluxio.grpc.RemovePolicyPOptions.Builder, alluxio.grpc.RemovePolicyPOptionsOrBuilder>(
                getOptions(),
                getParentForChildren(),
                isClean());
        options_ = null;
      }
      return optionsBuilder_;
    }
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.policy.RemovePolicyPRequest)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.policy.RemovePolicyPRequest)
  private static final alluxio.grpc.RemovePolicyPRequest DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.RemovePolicyPRequest();
  }

  public static alluxio.grpc.RemovePolicyPRequest getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<RemovePolicyPRequest>
      PARSER = new com.google.protobuf.AbstractParser<RemovePolicyPRequest>() {
    public RemovePolicyPRequest parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new RemovePolicyPRequest(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<RemovePolicyPRequest> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<RemovePolicyPRequest> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.RemovePolicyPRequest getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

