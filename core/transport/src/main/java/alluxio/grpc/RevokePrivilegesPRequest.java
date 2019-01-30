// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/privilege_master.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.privilege.RevokePrivilegesPRequest}
 */
public  final class RevokePrivilegesPRequest extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.privilege.RevokePrivilegesPRequest)
    RevokePrivilegesPRequestOrBuilder {
private static final long serialVersionUID = 0L;
  // Use RevokePrivilegesPRequest.newBuilder() to construct.
  private RevokePrivilegesPRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private RevokePrivilegesPRequest() {
    group_ = "";
    privileges_ = java.util.Collections.emptyList();
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private RevokePrivilegesPRequest(
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
            group_ = bs;
            break;
          }
          case 16: {
            int rawValue = input.readEnum();
            alluxio.grpc.PPrivilege value = alluxio.grpc.PPrivilege.valueOf(rawValue);
            if (value == null) {
              unknownFields.mergeVarintField(2, rawValue);
            } else {
              if (!((mutable_bitField0_ & 0x00000002) == 0x00000002)) {
                privileges_ = new java.util.ArrayList<java.lang.Integer>();
                mutable_bitField0_ |= 0x00000002;
              }
              privileges_.add(rawValue);
            }
            break;
          }
          case 18: {
            int length = input.readRawVarint32();
            int oldLimit = input.pushLimit(length);
            while(input.getBytesUntilLimit() > 0) {
              int rawValue = input.readEnum();
              alluxio.grpc.PPrivilege value = alluxio.grpc.PPrivilege.valueOf(rawValue);
              if (value == null) {
                unknownFields.mergeVarintField(2, rawValue);
              } else {
                if (!((mutable_bitField0_ & 0x00000002) == 0x00000002)) {
                  privileges_ = new java.util.ArrayList<java.lang.Integer>();
                  mutable_bitField0_ |= 0x00000002;
                }
                privileges_.add(rawValue);
              }
            }
            input.popLimit(oldLimit);
            break;
          }
          case 26: {
            alluxio.grpc.RevokePrivilegesPOptions.Builder subBuilder = null;
            if (((bitField0_ & 0x00000002) == 0x00000002)) {
              subBuilder = options_.toBuilder();
            }
            options_ = input.readMessage(alluxio.grpc.RevokePrivilegesPOptions.PARSER, extensionRegistry);
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
      if (((mutable_bitField0_ & 0x00000002) == 0x00000002)) {
        privileges_ = java.util.Collections.unmodifiableList(privileges_);
      }
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return alluxio.grpc.PrivilegeMasterProto.internal_static_alluxio_grpc_privilege_RevokePrivilegesPRequest_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.PrivilegeMasterProto.internal_static_alluxio_grpc_privilege_RevokePrivilegesPRequest_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.RevokePrivilegesPRequest.class, alluxio.grpc.RevokePrivilegesPRequest.Builder.class);
  }

  private int bitField0_;
  public static final int GROUP_FIELD_NUMBER = 1;
  private volatile java.lang.Object group_;
  /**
   * <code>optional string group = 1;</code>
   */
  public boolean hasGroup() {
    return ((bitField0_ & 0x00000001) == 0x00000001);
  }
  /**
   * <code>optional string group = 1;</code>
   */
  public java.lang.String getGroup() {
    java.lang.Object ref = group_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (bs.isValidUtf8()) {
        group_ = s;
      }
      return s;
    }
  }
  /**
   * <code>optional string group = 1;</code>
   */
  public com.google.protobuf.ByteString
      getGroupBytes() {
    java.lang.Object ref = group_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      group_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int PRIVILEGES_FIELD_NUMBER = 2;
  private java.util.List<java.lang.Integer> privileges_;
  private static final com.google.protobuf.Internal.ListAdapter.Converter<
      java.lang.Integer, alluxio.grpc.PPrivilege> privileges_converter_ =
          new com.google.protobuf.Internal.ListAdapter.Converter<
              java.lang.Integer, alluxio.grpc.PPrivilege>() {
            public alluxio.grpc.PPrivilege convert(java.lang.Integer from) {
              alluxio.grpc.PPrivilege result = alluxio.grpc.PPrivilege.valueOf(from);
              return result == null ? alluxio.grpc.PPrivilege.FREE : result;
            }
          };
  /**
   * <code>repeated .alluxio.grpc.privilege.PPrivilege privileges = 2;</code>
   */
  public java.util.List<alluxio.grpc.PPrivilege> getPrivilegesList() {
    return new com.google.protobuf.Internal.ListAdapter<
        java.lang.Integer, alluxio.grpc.PPrivilege>(privileges_, privileges_converter_);
  }
  /**
   * <code>repeated .alluxio.grpc.privilege.PPrivilege privileges = 2;</code>
   */
  public int getPrivilegesCount() {
    return privileges_.size();
  }
  /**
   * <code>repeated .alluxio.grpc.privilege.PPrivilege privileges = 2;</code>
   */
  public alluxio.grpc.PPrivilege getPrivileges(int index) {
    return privileges_converter_.convert(privileges_.get(index));
  }

  public static final int OPTIONS_FIELD_NUMBER = 3;
  private alluxio.grpc.RevokePrivilegesPOptions options_;
  /**
   * <code>optional .alluxio.grpc.privilege.RevokePrivilegesPOptions options = 3;</code>
   */
  public boolean hasOptions() {
    return ((bitField0_ & 0x00000002) == 0x00000002);
  }
  /**
   * <code>optional .alluxio.grpc.privilege.RevokePrivilegesPOptions options = 3;</code>
   */
  public alluxio.grpc.RevokePrivilegesPOptions getOptions() {
    return options_ == null ? alluxio.grpc.RevokePrivilegesPOptions.getDefaultInstance() : options_;
  }
  /**
   * <code>optional .alluxio.grpc.privilege.RevokePrivilegesPOptions options = 3;</code>
   */
  public alluxio.grpc.RevokePrivilegesPOptionsOrBuilder getOptionsOrBuilder() {
    return options_ == null ? alluxio.grpc.RevokePrivilegesPOptions.getDefaultInstance() : options_;
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
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, group_);
    }
    for (int i = 0; i < privileges_.size(); i++) {
      output.writeEnum(2, privileges_.get(i));
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      output.writeMessage(3, getOptions());
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, group_);
    }
    {
      int dataSize = 0;
      for (int i = 0; i < privileges_.size(); i++) {
        dataSize += com.google.protobuf.CodedOutputStream
          .computeEnumSizeNoTag(privileges_.get(i));
      }
      size += dataSize;
      size += 1 * privileges_.size();
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(3, getOptions());
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
    if (!(obj instanceof alluxio.grpc.RevokePrivilegesPRequest)) {
      return super.equals(obj);
    }
    alluxio.grpc.RevokePrivilegesPRequest other = (alluxio.grpc.RevokePrivilegesPRequest) obj;

    boolean result = true;
    result = result && (hasGroup() == other.hasGroup());
    if (hasGroup()) {
      result = result && getGroup()
          .equals(other.getGroup());
    }
    result = result && privileges_.equals(other.privileges_);
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
    if (hasGroup()) {
      hash = (37 * hash) + GROUP_FIELD_NUMBER;
      hash = (53 * hash) + getGroup().hashCode();
    }
    if (getPrivilegesCount() > 0) {
      hash = (37 * hash) + PRIVILEGES_FIELD_NUMBER;
      hash = (53 * hash) + privileges_.hashCode();
    }
    if (hasOptions()) {
      hash = (37 * hash) + OPTIONS_FIELD_NUMBER;
      hash = (53 * hash) + getOptions().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.RevokePrivilegesPRequest parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.RevokePrivilegesPRequest parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.RevokePrivilegesPRequest parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.RevokePrivilegesPRequest parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.RevokePrivilegesPRequest parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.RevokePrivilegesPRequest parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.RevokePrivilegesPRequest parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.RevokePrivilegesPRequest parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.RevokePrivilegesPRequest parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.RevokePrivilegesPRequest parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.RevokePrivilegesPRequest parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.RevokePrivilegesPRequest parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.RevokePrivilegesPRequest prototype) {
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
   * Protobuf type {@code alluxio.grpc.privilege.RevokePrivilegesPRequest}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.privilege.RevokePrivilegesPRequest)
      alluxio.grpc.RevokePrivilegesPRequestOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.PrivilegeMasterProto.internal_static_alluxio_grpc_privilege_RevokePrivilegesPRequest_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.PrivilegeMasterProto.internal_static_alluxio_grpc_privilege_RevokePrivilegesPRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.RevokePrivilegesPRequest.class, alluxio.grpc.RevokePrivilegesPRequest.Builder.class);
    }

    // Construct using alluxio.grpc.RevokePrivilegesPRequest.newBuilder()
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
      group_ = "";
      bitField0_ = (bitField0_ & ~0x00000001);
      privileges_ = java.util.Collections.emptyList();
      bitField0_ = (bitField0_ & ~0x00000002);
      if (optionsBuilder_ == null) {
        options_ = null;
      } else {
        optionsBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000004);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.PrivilegeMasterProto.internal_static_alluxio_grpc_privilege_RevokePrivilegesPRequest_descriptor;
    }

    public alluxio.grpc.RevokePrivilegesPRequest getDefaultInstanceForType() {
      return alluxio.grpc.RevokePrivilegesPRequest.getDefaultInstance();
    }

    public alluxio.grpc.RevokePrivilegesPRequest build() {
      alluxio.grpc.RevokePrivilegesPRequest result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.RevokePrivilegesPRequest buildPartial() {
      alluxio.grpc.RevokePrivilegesPRequest result = new alluxio.grpc.RevokePrivilegesPRequest(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
        to_bitField0_ |= 0x00000001;
      }
      result.group_ = group_;
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        privileges_ = java.util.Collections.unmodifiableList(privileges_);
        bitField0_ = (bitField0_ & ~0x00000002);
      }
      result.privileges_ = privileges_;
      if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
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
      if (other instanceof alluxio.grpc.RevokePrivilegesPRequest) {
        return mergeFrom((alluxio.grpc.RevokePrivilegesPRequest)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.RevokePrivilegesPRequest other) {
      if (other == alluxio.grpc.RevokePrivilegesPRequest.getDefaultInstance()) return this;
      if (other.hasGroup()) {
        bitField0_ |= 0x00000001;
        group_ = other.group_;
        onChanged();
      }
      if (!other.privileges_.isEmpty()) {
        if (privileges_.isEmpty()) {
          privileges_ = other.privileges_;
          bitField0_ = (bitField0_ & ~0x00000002);
        } else {
          ensurePrivilegesIsMutable();
          privileges_.addAll(other.privileges_);
        }
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
      alluxio.grpc.RevokePrivilegesPRequest parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.RevokePrivilegesPRequest) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private java.lang.Object group_ = "";
    /**
     * <code>optional string group = 1;</code>
     */
    public boolean hasGroup() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional string group = 1;</code>
     */
    public java.lang.String getGroup() {
      java.lang.Object ref = group_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          group_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string group = 1;</code>
     */
    public com.google.protobuf.ByteString
        getGroupBytes() {
      java.lang.Object ref = group_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        group_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string group = 1;</code>
     */
    public Builder setGroup(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
      group_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string group = 1;</code>
     */
    public Builder clearGroup() {
      bitField0_ = (bitField0_ & ~0x00000001);
      group_ = getDefaultInstance().getGroup();
      onChanged();
      return this;
    }
    /**
     * <code>optional string group = 1;</code>
     */
    public Builder setGroupBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
      group_ = value;
      onChanged();
      return this;
    }

    private java.util.List<java.lang.Integer> privileges_ =
      java.util.Collections.emptyList();
    private void ensurePrivilegesIsMutable() {
      if (!((bitField0_ & 0x00000002) == 0x00000002)) {
        privileges_ = new java.util.ArrayList<java.lang.Integer>(privileges_);
        bitField0_ |= 0x00000002;
      }
    }
    /**
     * <code>repeated .alluxio.grpc.privilege.PPrivilege privileges = 2;</code>
     */
    public java.util.List<alluxio.grpc.PPrivilege> getPrivilegesList() {
      return new com.google.protobuf.Internal.ListAdapter<
          java.lang.Integer, alluxio.grpc.PPrivilege>(privileges_, privileges_converter_);
    }
    /**
     * <code>repeated .alluxio.grpc.privilege.PPrivilege privileges = 2;</code>
     */
    public int getPrivilegesCount() {
      return privileges_.size();
    }
    /**
     * <code>repeated .alluxio.grpc.privilege.PPrivilege privileges = 2;</code>
     */
    public alluxio.grpc.PPrivilege getPrivileges(int index) {
      return privileges_converter_.convert(privileges_.get(index));
    }
    /**
     * <code>repeated .alluxio.grpc.privilege.PPrivilege privileges = 2;</code>
     */
    public Builder setPrivileges(
        int index, alluxio.grpc.PPrivilege value) {
      if (value == null) {
        throw new NullPointerException();
      }
      ensurePrivilegesIsMutable();
      privileges_.set(index, value.getNumber());
      onChanged();
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.privilege.PPrivilege privileges = 2;</code>
     */
    public Builder addPrivileges(alluxio.grpc.PPrivilege value) {
      if (value == null) {
        throw new NullPointerException();
      }
      ensurePrivilegesIsMutable();
      privileges_.add(value.getNumber());
      onChanged();
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.privilege.PPrivilege privileges = 2;</code>
     */
    public Builder addAllPrivileges(
        java.lang.Iterable<? extends alluxio.grpc.PPrivilege> values) {
      ensurePrivilegesIsMutable();
      for (alluxio.grpc.PPrivilege value : values) {
        privileges_.add(value.getNumber());
      }
      onChanged();
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.privilege.PPrivilege privileges = 2;</code>
     */
    public Builder clearPrivileges() {
      privileges_ = java.util.Collections.emptyList();
      bitField0_ = (bitField0_ & ~0x00000002);
      onChanged();
      return this;
    }

    private alluxio.grpc.RevokePrivilegesPOptions options_ = null;
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.grpc.RevokePrivilegesPOptions, alluxio.grpc.RevokePrivilegesPOptions.Builder, alluxio.grpc.RevokePrivilegesPOptionsOrBuilder> optionsBuilder_;
    /**
     * <code>optional .alluxio.grpc.privilege.RevokePrivilegesPOptions options = 3;</code>
     */
    public boolean hasOptions() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional .alluxio.grpc.privilege.RevokePrivilegesPOptions options = 3;</code>
     */
    public alluxio.grpc.RevokePrivilegesPOptions getOptions() {
      if (optionsBuilder_ == null) {
        return options_ == null ? alluxio.grpc.RevokePrivilegesPOptions.getDefaultInstance() : options_;
      } else {
        return optionsBuilder_.getMessage();
      }
    }
    /**
     * <code>optional .alluxio.grpc.privilege.RevokePrivilegesPOptions options = 3;</code>
     */
    public Builder setOptions(alluxio.grpc.RevokePrivilegesPOptions value) {
      if (optionsBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        options_ = value;
        onChanged();
      } else {
        optionsBuilder_.setMessage(value);
      }
      bitField0_ |= 0x00000004;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.privilege.RevokePrivilegesPOptions options = 3;</code>
     */
    public Builder setOptions(
        alluxio.grpc.RevokePrivilegesPOptions.Builder builderForValue) {
      if (optionsBuilder_ == null) {
        options_ = builderForValue.build();
        onChanged();
      } else {
        optionsBuilder_.setMessage(builderForValue.build());
      }
      bitField0_ |= 0x00000004;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.privilege.RevokePrivilegesPOptions options = 3;</code>
     */
    public Builder mergeOptions(alluxio.grpc.RevokePrivilegesPOptions value) {
      if (optionsBuilder_ == null) {
        if (((bitField0_ & 0x00000004) == 0x00000004) &&
            options_ != null &&
            options_ != alluxio.grpc.RevokePrivilegesPOptions.getDefaultInstance()) {
          options_ =
            alluxio.grpc.RevokePrivilegesPOptions.newBuilder(options_).mergeFrom(value).buildPartial();
        } else {
          options_ = value;
        }
        onChanged();
      } else {
        optionsBuilder_.mergeFrom(value);
      }
      bitField0_ |= 0x00000004;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.privilege.RevokePrivilegesPOptions options = 3;</code>
     */
    public Builder clearOptions() {
      if (optionsBuilder_ == null) {
        options_ = null;
        onChanged();
      } else {
        optionsBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000004);
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.privilege.RevokePrivilegesPOptions options = 3;</code>
     */
    public alluxio.grpc.RevokePrivilegesPOptions.Builder getOptionsBuilder() {
      bitField0_ |= 0x00000004;
      onChanged();
      return getOptionsFieldBuilder().getBuilder();
    }
    /**
     * <code>optional .alluxio.grpc.privilege.RevokePrivilegesPOptions options = 3;</code>
     */
    public alluxio.grpc.RevokePrivilegesPOptionsOrBuilder getOptionsOrBuilder() {
      if (optionsBuilder_ != null) {
        return optionsBuilder_.getMessageOrBuilder();
      } else {
        return options_ == null ?
            alluxio.grpc.RevokePrivilegesPOptions.getDefaultInstance() : options_;
      }
    }
    /**
     * <code>optional .alluxio.grpc.privilege.RevokePrivilegesPOptions options = 3;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.grpc.RevokePrivilegesPOptions, alluxio.grpc.RevokePrivilegesPOptions.Builder, alluxio.grpc.RevokePrivilegesPOptionsOrBuilder> 
        getOptionsFieldBuilder() {
      if (optionsBuilder_ == null) {
        optionsBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            alluxio.grpc.RevokePrivilegesPOptions, alluxio.grpc.RevokePrivilegesPOptions.Builder, alluxio.grpc.RevokePrivilegesPOptionsOrBuilder>(
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


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.privilege.RevokePrivilegesPRequest)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.privilege.RevokePrivilegesPRequest)
  private static final alluxio.grpc.RevokePrivilegesPRequest DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.RevokePrivilegesPRequest();
  }

  public static alluxio.grpc.RevokePrivilegesPRequest getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<RevokePrivilegesPRequest>
      PARSER = new com.google.protobuf.AbstractParser<RevokePrivilegesPRequest>() {
    public RevokePrivilegesPRequest parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new RevokePrivilegesPRequest(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<RevokePrivilegesPRequest> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<RevokePrivilegesPRequest> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.RevokePrivilegesPRequest getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

