// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/policy_master.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.policy.ListPolicyPResponse}
 */
public  final class ListPolicyPResponse extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.policy.ListPolicyPResponse)
    ListPolicyPResponseOrBuilder {
private static final long serialVersionUID = 0L;
  // Use ListPolicyPResponse.newBuilder() to construct.
  private ListPolicyPResponse(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private ListPolicyPResponse() {
    policy_ = java.util.Collections.emptyList();
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private ListPolicyPResponse(
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
            if (!((mutable_bitField0_ & 0x00000001) == 0x00000001)) {
              policy_ = new java.util.ArrayList<alluxio.grpc.PolicyInfo>();
              mutable_bitField0_ |= 0x00000001;
            }
            policy_.add(
                input.readMessage(alluxio.grpc.PolicyInfo.PARSER, extensionRegistry));
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
      if (((mutable_bitField0_ & 0x00000001) == 0x00000001)) {
        policy_ = java.util.Collections.unmodifiableList(policy_);
      }
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return alluxio.grpc.PolicyMasterProto.internal_static_alluxio_grpc_policy_ListPolicyPResponse_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.PolicyMasterProto.internal_static_alluxio_grpc_policy_ListPolicyPResponse_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.ListPolicyPResponse.class, alluxio.grpc.ListPolicyPResponse.Builder.class);
  }

  public static final int POLICY_FIELD_NUMBER = 1;
  private java.util.List<alluxio.grpc.PolicyInfo> policy_;
  /**
   * <code>repeated .alluxio.grpc.policy.PolicyInfo policy = 1;</code>
   */
  public java.util.List<alluxio.grpc.PolicyInfo> getPolicyList() {
    return policy_;
  }
  /**
   * <code>repeated .alluxio.grpc.policy.PolicyInfo policy = 1;</code>
   */
  public java.util.List<? extends alluxio.grpc.PolicyInfoOrBuilder> 
      getPolicyOrBuilderList() {
    return policy_;
  }
  /**
   * <code>repeated .alluxio.grpc.policy.PolicyInfo policy = 1;</code>
   */
  public int getPolicyCount() {
    return policy_.size();
  }
  /**
   * <code>repeated .alluxio.grpc.policy.PolicyInfo policy = 1;</code>
   */
  public alluxio.grpc.PolicyInfo getPolicy(int index) {
    return policy_.get(index);
  }
  /**
   * <code>repeated .alluxio.grpc.policy.PolicyInfo policy = 1;</code>
   */
  public alluxio.grpc.PolicyInfoOrBuilder getPolicyOrBuilder(
      int index) {
    return policy_.get(index);
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
    for (int i = 0; i < policy_.size(); i++) {
      output.writeMessage(1, policy_.get(i));
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    for (int i = 0; i < policy_.size(); i++) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(1, policy_.get(i));
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
    if (!(obj instanceof alluxio.grpc.ListPolicyPResponse)) {
      return super.equals(obj);
    }
    alluxio.grpc.ListPolicyPResponse other = (alluxio.grpc.ListPolicyPResponse) obj;

    boolean result = true;
    result = result && getPolicyList()
        .equals(other.getPolicyList());
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
    if (getPolicyCount() > 0) {
      hash = (37 * hash) + POLICY_FIELD_NUMBER;
      hash = (53 * hash) + getPolicyList().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.ListPolicyPResponse parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.ListPolicyPResponse parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.ListPolicyPResponse parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.ListPolicyPResponse parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.ListPolicyPResponse parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.ListPolicyPResponse parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.ListPolicyPResponse parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.ListPolicyPResponse parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.ListPolicyPResponse parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.ListPolicyPResponse parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.ListPolicyPResponse parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.ListPolicyPResponse parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.ListPolicyPResponse prototype) {
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
   * Protobuf type {@code alluxio.grpc.policy.ListPolicyPResponse}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.policy.ListPolicyPResponse)
      alluxio.grpc.ListPolicyPResponseOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.PolicyMasterProto.internal_static_alluxio_grpc_policy_ListPolicyPResponse_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.PolicyMasterProto.internal_static_alluxio_grpc_policy_ListPolicyPResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.ListPolicyPResponse.class, alluxio.grpc.ListPolicyPResponse.Builder.class);
    }

    // Construct using alluxio.grpc.ListPolicyPResponse.newBuilder()
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
        getPolicyFieldBuilder();
      }
    }
    public Builder clear() {
      super.clear();
      if (policyBuilder_ == null) {
        policy_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000001);
      } else {
        policyBuilder_.clear();
      }
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.PolicyMasterProto.internal_static_alluxio_grpc_policy_ListPolicyPResponse_descriptor;
    }

    public alluxio.grpc.ListPolicyPResponse getDefaultInstanceForType() {
      return alluxio.grpc.ListPolicyPResponse.getDefaultInstance();
    }

    public alluxio.grpc.ListPolicyPResponse build() {
      alluxio.grpc.ListPolicyPResponse result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.ListPolicyPResponse buildPartial() {
      alluxio.grpc.ListPolicyPResponse result = new alluxio.grpc.ListPolicyPResponse(this);
      int from_bitField0_ = bitField0_;
      if (policyBuilder_ == null) {
        if (((bitField0_ & 0x00000001) == 0x00000001)) {
          policy_ = java.util.Collections.unmodifiableList(policy_);
          bitField0_ = (bitField0_ & ~0x00000001);
        }
        result.policy_ = policy_;
      } else {
        result.policy_ = policyBuilder_.build();
      }
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
      if (other instanceof alluxio.grpc.ListPolicyPResponse) {
        return mergeFrom((alluxio.grpc.ListPolicyPResponse)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.ListPolicyPResponse other) {
      if (other == alluxio.grpc.ListPolicyPResponse.getDefaultInstance()) return this;
      if (policyBuilder_ == null) {
        if (!other.policy_.isEmpty()) {
          if (policy_.isEmpty()) {
            policy_ = other.policy_;
            bitField0_ = (bitField0_ & ~0x00000001);
          } else {
            ensurePolicyIsMutable();
            policy_.addAll(other.policy_);
          }
          onChanged();
        }
      } else {
        if (!other.policy_.isEmpty()) {
          if (policyBuilder_.isEmpty()) {
            policyBuilder_.dispose();
            policyBuilder_ = null;
            policy_ = other.policy_;
            bitField0_ = (bitField0_ & ~0x00000001);
            policyBuilder_ = 
              com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders ?
                 getPolicyFieldBuilder() : null;
          } else {
            policyBuilder_.addAllMessages(other.policy_);
          }
        }
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
      alluxio.grpc.ListPolicyPResponse parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.ListPolicyPResponse) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private java.util.List<alluxio.grpc.PolicyInfo> policy_ =
      java.util.Collections.emptyList();
    private void ensurePolicyIsMutable() {
      if (!((bitField0_ & 0x00000001) == 0x00000001)) {
        policy_ = new java.util.ArrayList<alluxio.grpc.PolicyInfo>(policy_);
        bitField0_ |= 0x00000001;
       }
    }

    private com.google.protobuf.RepeatedFieldBuilderV3<
        alluxio.grpc.PolicyInfo, alluxio.grpc.PolicyInfo.Builder, alluxio.grpc.PolicyInfoOrBuilder> policyBuilder_;

    /**
     * <code>repeated .alluxio.grpc.policy.PolicyInfo policy = 1;</code>
     */
    public java.util.List<alluxio.grpc.PolicyInfo> getPolicyList() {
      if (policyBuilder_ == null) {
        return java.util.Collections.unmodifiableList(policy_);
      } else {
        return policyBuilder_.getMessageList();
      }
    }
    /**
     * <code>repeated .alluxio.grpc.policy.PolicyInfo policy = 1;</code>
     */
    public int getPolicyCount() {
      if (policyBuilder_ == null) {
        return policy_.size();
      } else {
        return policyBuilder_.getCount();
      }
    }
    /**
     * <code>repeated .alluxio.grpc.policy.PolicyInfo policy = 1;</code>
     */
    public alluxio.grpc.PolicyInfo getPolicy(int index) {
      if (policyBuilder_ == null) {
        return policy_.get(index);
      } else {
        return policyBuilder_.getMessage(index);
      }
    }
    /**
     * <code>repeated .alluxio.grpc.policy.PolicyInfo policy = 1;</code>
     */
    public Builder setPolicy(
        int index, alluxio.grpc.PolicyInfo value) {
      if (policyBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensurePolicyIsMutable();
        policy_.set(index, value);
        onChanged();
      } else {
        policyBuilder_.setMessage(index, value);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.policy.PolicyInfo policy = 1;</code>
     */
    public Builder setPolicy(
        int index, alluxio.grpc.PolicyInfo.Builder builderForValue) {
      if (policyBuilder_ == null) {
        ensurePolicyIsMutable();
        policy_.set(index, builderForValue.build());
        onChanged();
      } else {
        policyBuilder_.setMessage(index, builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.policy.PolicyInfo policy = 1;</code>
     */
    public Builder addPolicy(alluxio.grpc.PolicyInfo value) {
      if (policyBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensurePolicyIsMutable();
        policy_.add(value);
        onChanged();
      } else {
        policyBuilder_.addMessage(value);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.policy.PolicyInfo policy = 1;</code>
     */
    public Builder addPolicy(
        int index, alluxio.grpc.PolicyInfo value) {
      if (policyBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensurePolicyIsMutable();
        policy_.add(index, value);
        onChanged();
      } else {
        policyBuilder_.addMessage(index, value);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.policy.PolicyInfo policy = 1;</code>
     */
    public Builder addPolicy(
        alluxio.grpc.PolicyInfo.Builder builderForValue) {
      if (policyBuilder_ == null) {
        ensurePolicyIsMutable();
        policy_.add(builderForValue.build());
        onChanged();
      } else {
        policyBuilder_.addMessage(builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.policy.PolicyInfo policy = 1;</code>
     */
    public Builder addPolicy(
        int index, alluxio.grpc.PolicyInfo.Builder builderForValue) {
      if (policyBuilder_ == null) {
        ensurePolicyIsMutable();
        policy_.add(index, builderForValue.build());
        onChanged();
      } else {
        policyBuilder_.addMessage(index, builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.policy.PolicyInfo policy = 1;</code>
     */
    public Builder addAllPolicy(
        java.lang.Iterable<? extends alluxio.grpc.PolicyInfo> values) {
      if (policyBuilder_ == null) {
        ensurePolicyIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
            values, policy_);
        onChanged();
      } else {
        policyBuilder_.addAllMessages(values);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.policy.PolicyInfo policy = 1;</code>
     */
    public Builder clearPolicy() {
      if (policyBuilder_ == null) {
        policy_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000001);
        onChanged();
      } else {
        policyBuilder_.clear();
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.policy.PolicyInfo policy = 1;</code>
     */
    public Builder removePolicy(int index) {
      if (policyBuilder_ == null) {
        ensurePolicyIsMutable();
        policy_.remove(index);
        onChanged();
      } else {
        policyBuilder_.remove(index);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.policy.PolicyInfo policy = 1;</code>
     */
    public alluxio.grpc.PolicyInfo.Builder getPolicyBuilder(
        int index) {
      return getPolicyFieldBuilder().getBuilder(index);
    }
    /**
     * <code>repeated .alluxio.grpc.policy.PolicyInfo policy = 1;</code>
     */
    public alluxio.grpc.PolicyInfoOrBuilder getPolicyOrBuilder(
        int index) {
      if (policyBuilder_ == null) {
        return policy_.get(index);  } else {
        return policyBuilder_.getMessageOrBuilder(index);
      }
    }
    /**
     * <code>repeated .alluxio.grpc.policy.PolicyInfo policy = 1;</code>
     */
    public java.util.List<? extends alluxio.grpc.PolicyInfoOrBuilder> 
         getPolicyOrBuilderList() {
      if (policyBuilder_ != null) {
        return policyBuilder_.getMessageOrBuilderList();
      } else {
        return java.util.Collections.unmodifiableList(policy_);
      }
    }
    /**
     * <code>repeated .alluxio.grpc.policy.PolicyInfo policy = 1;</code>
     */
    public alluxio.grpc.PolicyInfo.Builder addPolicyBuilder() {
      return getPolicyFieldBuilder().addBuilder(
          alluxio.grpc.PolicyInfo.getDefaultInstance());
    }
    /**
     * <code>repeated .alluxio.grpc.policy.PolicyInfo policy = 1;</code>
     */
    public alluxio.grpc.PolicyInfo.Builder addPolicyBuilder(
        int index) {
      return getPolicyFieldBuilder().addBuilder(
          index, alluxio.grpc.PolicyInfo.getDefaultInstance());
    }
    /**
     * <code>repeated .alluxio.grpc.policy.PolicyInfo policy = 1;</code>
     */
    public java.util.List<alluxio.grpc.PolicyInfo.Builder> 
         getPolicyBuilderList() {
      return getPolicyFieldBuilder().getBuilderList();
    }
    private com.google.protobuf.RepeatedFieldBuilderV3<
        alluxio.grpc.PolicyInfo, alluxio.grpc.PolicyInfo.Builder, alluxio.grpc.PolicyInfoOrBuilder> 
        getPolicyFieldBuilder() {
      if (policyBuilder_ == null) {
        policyBuilder_ = new com.google.protobuf.RepeatedFieldBuilderV3<
            alluxio.grpc.PolicyInfo, alluxio.grpc.PolicyInfo.Builder, alluxio.grpc.PolicyInfoOrBuilder>(
                policy_,
                ((bitField0_ & 0x00000001) == 0x00000001),
                getParentForChildren(),
                isClean());
        policy_ = null;
      }
      return policyBuilder_;
    }
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.policy.ListPolicyPResponse)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.policy.ListPolicyPResponse)
  private static final alluxio.grpc.ListPolicyPResponse DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.ListPolicyPResponse();
  }

  public static alluxio.grpc.ListPolicyPResponse getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<ListPolicyPResponse>
      PARSER = new com.google.protobuf.AbstractParser<ListPolicyPResponse>() {
    public ListPolicyPResponse parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new ListPolicyPResponse(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<ListPolicyPResponse> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<ListPolicyPResponse> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.ListPolicyPResponse getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

