// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

/**
 * <pre>
 * ALLUXIO CS ADD
 * </pre>
 *
 * Protobuf type {@code alluxio.grpc.file.DelegationTokenIdentifier}
 */
public  final class DelegationTokenIdentifier extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.file.DelegationTokenIdentifier)
    DelegationTokenIdentifierOrBuilder {
private static final long serialVersionUID = 0L;
  // Use DelegationTokenIdentifier.newBuilder() to construct.
  private DelegationTokenIdentifier(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private DelegationTokenIdentifier() {
    owner_ = "";
    renewer_ = "";
    realUser_ = "";
    issueDate_ = 0L;
    maxDate_ = 0L;
    sequenceNumber_ = 0L;
    masterKeyId_ = 0L;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private DelegationTokenIdentifier(
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
            owner_ = bs;
            break;
          }
          case 18: {
            com.google.protobuf.ByteString bs = input.readBytes();
            bitField0_ |= 0x00000002;
            renewer_ = bs;
            break;
          }
          case 26: {
            com.google.protobuf.ByteString bs = input.readBytes();
            bitField0_ |= 0x00000004;
            realUser_ = bs;
            break;
          }
          case 32: {
            bitField0_ |= 0x00000008;
            issueDate_ = input.readInt64();
            break;
          }
          case 40: {
            bitField0_ |= 0x00000010;
            maxDate_ = input.readInt64();
            break;
          }
          case 48: {
            bitField0_ |= 0x00000020;
            sequenceNumber_ = input.readInt64();
            break;
          }
          case 56: {
            bitField0_ |= 0x00000040;
            masterKeyId_ = input.readInt64();
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
    return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_DelegationTokenIdentifier_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_DelegationTokenIdentifier_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.DelegationTokenIdentifier.class, alluxio.grpc.DelegationTokenIdentifier.Builder.class);
  }

  private int bitField0_;
  public static final int OWNER_FIELD_NUMBER = 1;
  private volatile java.lang.Object owner_;
  /**
   * <code>optional string owner = 1;</code>
   */
  public boolean hasOwner() {
    return ((bitField0_ & 0x00000001) == 0x00000001);
  }
  /**
   * <code>optional string owner = 1;</code>
   */
  public java.lang.String getOwner() {
    java.lang.Object ref = owner_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (bs.isValidUtf8()) {
        owner_ = s;
      }
      return s;
    }
  }
  /**
   * <code>optional string owner = 1;</code>
   */
  public com.google.protobuf.ByteString
      getOwnerBytes() {
    java.lang.Object ref = owner_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      owner_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int RENEWER_FIELD_NUMBER = 2;
  private volatile java.lang.Object renewer_;
  /**
   * <code>optional string renewer = 2;</code>
   */
  public boolean hasRenewer() {
    return ((bitField0_ & 0x00000002) == 0x00000002);
  }
  /**
   * <code>optional string renewer = 2;</code>
   */
  public java.lang.String getRenewer() {
    java.lang.Object ref = renewer_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (bs.isValidUtf8()) {
        renewer_ = s;
      }
      return s;
    }
  }
  /**
   * <code>optional string renewer = 2;</code>
   */
  public com.google.protobuf.ByteString
      getRenewerBytes() {
    java.lang.Object ref = renewer_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      renewer_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int REALUSER_FIELD_NUMBER = 3;
  private volatile java.lang.Object realUser_;
  /**
   * <code>optional string realUser = 3;</code>
   */
  public boolean hasRealUser() {
    return ((bitField0_ & 0x00000004) == 0x00000004);
  }
  /**
   * <code>optional string realUser = 3;</code>
   */
  public java.lang.String getRealUser() {
    java.lang.Object ref = realUser_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (bs.isValidUtf8()) {
        realUser_ = s;
      }
      return s;
    }
  }
  /**
   * <code>optional string realUser = 3;</code>
   */
  public com.google.protobuf.ByteString
      getRealUserBytes() {
    java.lang.Object ref = realUser_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      realUser_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int ISSUEDATE_FIELD_NUMBER = 4;
  private long issueDate_;
  /**
   * <code>optional int64 issueDate = 4;</code>
   */
  public boolean hasIssueDate() {
    return ((bitField0_ & 0x00000008) == 0x00000008);
  }
  /**
   * <code>optional int64 issueDate = 4;</code>
   */
  public long getIssueDate() {
    return issueDate_;
  }

  public static final int MAXDATE_FIELD_NUMBER = 5;
  private long maxDate_;
  /**
   * <code>optional int64 maxDate = 5;</code>
   */
  public boolean hasMaxDate() {
    return ((bitField0_ & 0x00000010) == 0x00000010);
  }
  /**
   * <code>optional int64 maxDate = 5;</code>
   */
  public long getMaxDate() {
    return maxDate_;
  }

  public static final int SEQUENCENUMBER_FIELD_NUMBER = 6;
  private long sequenceNumber_;
  /**
   * <code>optional int64 sequenceNumber = 6;</code>
   */
  public boolean hasSequenceNumber() {
    return ((bitField0_ & 0x00000020) == 0x00000020);
  }
  /**
   * <code>optional int64 sequenceNumber = 6;</code>
   */
  public long getSequenceNumber() {
    return sequenceNumber_;
  }

  public static final int MASTERKEYID_FIELD_NUMBER = 7;
  private long masterKeyId_;
  /**
   * <code>optional int64 masterKeyId = 7;</code>
   */
  public boolean hasMasterKeyId() {
    return ((bitField0_ & 0x00000040) == 0x00000040);
  }
  /**
   * <code>optional int64 masterKeyId = 7;</code>
   */
  public long getMasterKeyId() {
    return masterKeyId_;
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
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, owner_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 2, renewer_);
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 3, realUser_);
    }
    if (((bitField0_ & 0x00000008) == 0x00000008)) {
      output.writeInt64(4, issueDate_);
    }
    if (((bitField0_ & 0x00000010) == 0x00000010)) {
      output.writeInt64(5, maxDate_);
    }
    if (((bitField0_ & 0x00000020) == 0x00000020)) {
      output.writeInt64(6, sequenceNumber_);
    }
    if (((bitField0_ & 0x00000040) == 0x00000040)) {
      output.writeInt64(7, masterKeyId_);
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, owner_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, renewer_);
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(3, realUser_);
    }
    if (((bitField0_ & 0x00000008) == 0x00000008)) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(4, issueDate_);
    }
    if (((bitField0_ & 0x00000010) == 0x00000010)) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(5, maxDate_);
    }
    if (((bitField0_ & 0x00000020) == 0x00000020)) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(6, sequenceNumber_);
    }
    if (((bitField0_ & 0x00000040) == 0x00000040)) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(7, masterKeyId_);
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
    if (!(obj instanceof alluxio.grpc.DelegationTokenIdentifier)) {
      return super.equals(obj);
    }
    alluxio.grpc.DelegationTokenIdentifier other = (alluxio.grpc.DelegationTokenIdentifier) obj;

    boolean result = true;
    result = result && (hasOwner() == other.hasOwner());
    if (hasOwner()) {
      result = result && getOwner()
          .equals(other.getOwner());
    }
    result = result && (hasRenewer() == other.hasRenewer());
    if (hasRenewer()) {
      result = result && getRenewer()
          .equals(other.getRenewer());
    }
    result = result && (hasRealUser() == other.hasRealUser());
    if (hasRealUser()) {
      result = result && getRealUser()
          .equals(other.getRealUser());
    }
    result = result && (hasIssueDate() == other.hasIssueDate());
    if (hasIssueDate()) {
      result = result && (getIssueDate()
          == other.getIssueDate());
    }
    result = result && (hasMaxDate() == other.hasMaxDate());
    if (hasMaxDate()) {
      result = result && (getMaxDate()
          == other.getMaxDate());
    }
    result = result && (hasSequenceNumber() == other.hasSequenceNumber());
    if (hasSequenceNumber()) {
      result = result && (getSequenceNumber()
          == other.getSequenceNumber());
    }
    result = result && (hasMasterKeyId() == other.hasMasterKeyId());
    if (hasMasterKeyId()) {
      result = result && (getMasterKeyId()
          == other.getMasterKeyId());
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
    if (hasOwner()) {
      hash = (37 * hash) + OWNER_FIELD_NUMBER;
      hash = (53 * hash) + getOwner().hashCode();
    }
    if (hasRenewer()) {
      hash = (37 * hash) + RENEWER_FIELD_NUMBER;
      hash = (53 * hash) + getRenewer().hashCode();
    }
    if (hasRealUser()) {
      hash = (37 * hash) + REALUSER_FIELD_NUMBER;
      hash = (53 * hash) + getRealUser().hashCode();
    }
    if (hasIssueDate()) {
      hash = (37 * hash) + ISSUEDATE_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
          getIssueDate());
    }
    if (hasMaxDate()) {
      hash = (37 * hash) + MAXDATE_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
          getMaxDate());
    }
    if (hasSequenceNumber()) {
      hash = (37 * hash) + SEQUENCENUMBER_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
          getSequenceNumber());
    }
    if (hasMasterKeyId()) {
      hash = (37 * hash) + MASTERKEYID_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
          getMasterKeyId());
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.DelegationTokenIdentifier parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.DelegationTokenIdentifier parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.DelegationTokenIdentifier parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.DelegationTokenIdentifier parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.DelegationTokenIdentifier parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.DelegationTokenIdentifier parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.DelegationTokenIdentifier parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.DelegationTokenIdentifier parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.DelegationTokenIdentifier parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.DelegationTokenIdentifier parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.DelegationTokenIdentifier parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.DelegationTokenIdentifier parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.DelegationTokenIdentifier prototype) {
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
   * ALLUXIO CS ADD
   * </pre>
   *
   * Protobuf type {@code alluxio.grpc.file.DelegationTokenIdentifier}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.file.DelegationTokenIdentifier)
      alluxio.grpc.DelegationTokenIdentifierOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_DelegationTokenIdentifier_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_DelegationTokenIdentifier_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.DelegationTokenIdentifier.class, alluxio.grpc.DelegationTokenIdentifier.Builder.class);
    }

    // Construct using alluxio.grpc.DelegationTokenIdentifier.newBuilder()
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
      owner_ = "";
      bitField0_ = (bitField0_ & ~0x00000001);
      renewer_ = "";
      bitField0_ = (bitField0_ & ~0x00000002);
      realUser_ = "";
      bitField0_ = (bitField0_ & ~0x00000004);
      issueDate_ = 0L;
      bitField0_ = (bitField0_ & ~0x00000008);
      maxDate_ = 0L;
      bitField0_ = (bitField0_ & ~0x00000010);
      sequenceNumber_ = 0L;
      bitField0_ = (bitField0_ & ~0x00000020);
      masterKeyId_ = 0L;
      bitField0_ = (bitField0_ & ~0x00000040);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_DelegationTokenIdentifier_descriptor;
    }

    public alluxio.grpc.DelegationTokenIdentifier getDefaultInstanceForType() {
      return alluxio.grpc.DelegationTokenIdentifier.getDefaultInstance();
    }

    public alluxio.grpc.DelegationTokenIdentifier build() {
      alluxio.grpc.DelegationTokenIdentifier result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.DelegationTokenIdentifier buildPartial() {
      alluxio.grpc.DelegationTokenIdentifier result = new alluxio.grpc.DelegationTokenIdentifier(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
        to_bitField0_ |= 0x00000001;
      }
      result.owner_ = owner_;
      if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
        to_bitField0_ |= 0x00000002;
      }
      result.renewer_ = renewer_;
      if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
        to_bitField0_ |= 0x00000004;
      }
      result.realUser_ = realUser_;
      if (((from_bitField0_ & 0x00000008) == 0x00000008)) {
        to_bitField0_ |= 0x00000008;
      }
      result.issueDate_ = issueDate_;
      if (((from_bitField0_ & 0x00000010) == 0x00000010)) {
        to_bitField0_ |= 0x00000010;
      }
      result.maxDate_ = maxDate_;
      if (((from_bitField0_ & 0x00000020) == 0x00000020)) {
        to_bitField0_ |= 0x00000020;
      }
      result.sequenceNumber_ = sequenceNumber_;
      if (((from_bitField0_ & 0x00000040) == 0x00000040)) {
        to_bitField0_ |= 0x00000040;
      }
      result.masterKeyId_ = masterKeyId_;
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
      if (other instanceof alluxio.grpc.DelegationTokenIdentifier) {
        return mergeFrom((alluxio.grpc.DelegationTokenIdentifier)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.DelegationTokenIdentifier other) {
      if (other == alluxio.grpc.DelegationTokenIdentifier.getDefaultInstance()) return this;
      if (other.hasOwner()) {
        bitField0_ |= 0x00000001;
        owner_ = other.owner_;
        onChanged();
      }
      if (other.hasRenewer()) {
        bitField0_ |= 0x00000002;
        renewer_ = other.renewer_;
        onChanged();
      }
      if (other.hasRealUser()) {
        bitField0_ |= 0x00000004;
        realUser_ = other.realUser_;
        onChanged();
      }
      if (other.hasIssueDate()) {
        setIssueDate(other.getIssueDate());
      }
      if (other.hasMaxDate()) {
        setMaxDate(other.getMaxDate());
      }
      if (other.hasSequenceNumber()) {
        setSequenceNumber(other.getSequenceNumber());
      }
      if (other.hasMasterKeyId()) {
        setMasterKeyId(other.getMasterKeyId());
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
      alluxio.grpc.DelegationTokenIdentifier parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.DelegationTokenIdentifier) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private java.lang.Object owner_ = "";
    /**
     * <code>optional string owner = 1;</code>
     */
    public boolean hasOwner() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional string owner = 1;</code>
     */
    public java.lang.String getOwner() {
      java.lang.Object ref = owner_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          owner_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string owner = 1;</code>
     */
    public com.google.protobuf.ByteString
        getOwnerBytes() {
      java.lang.Object ref = owner_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        owner_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string owner = 1;</code>
     */
    public Builder setOwner(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
      owner_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string owner = 1;</code>
     */
    public Builder clearOwner() {
      bitField0_ = (bitField0_ & ~0x00000001);
      owner_ = getDefaultInstance().getOwner();
      onChanged();
      return this;
    }
    /**
     * <code>optional string owner = 1;</code>
     */
    public Builder setOwnerBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
      owner_ = value;
      onChanged();
      return this;
    }

    private java.lang.Object renewer_ = "";
    /**
     * <code>optional string renewer = 2;</code>
     */
    public boolean hasRenewer() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional string renewer = 2;</code>
     */
    public java.lang.String getRenewer() {
      java.lang.Object ref = renewer_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          renewer_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string renewer = 2;</code>
     */
    public com.google.protobuf.ByteString
        getRenewerBytes() {
      java.lang.Object ref = renewer_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        renewer_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string renewer = 2;</code>
     */
    public Builder setRenewer(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
      renewer_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string renewer = 2;</code>
     */
    public Builder clearRenewer() {
      bitField0_ = (bitField0_ & ~0x00000002);
      renewer_ = getDefaultInstance().getRenewer();
      onChanged();
      return this;
    }
    /**
     * <code>optional string renewer = 2;</code>
     */
    public Builder setRenewerBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
      renewer_ = value;
      onChanged();
      return this;
    }

    private java.lang.Object realUser_ = "";
    /**
     * <code>optional string realUser = 3;</code>
     */
    public boolean hasRealUser() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional string realUser = 3;</code>
     */
    public java.lang.String getRealUser() {
      java.lang.Object ref = realUser_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          realUser_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string realUser = 3;</code>
     */
    public com.google.protobuf.ByteString
        getRealUserBytes() {
      java.lang.Object ref = realUser_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        realUser_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string realUser = 3;</code>
     */
    public Builder setRealUser(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
      realUser_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string realUser = 3;</code>
     */
    public Builder clearRealUser() {
      bitField0_ = (bitField0_ & ~0x00000004);
      realUser_ = getDefaultInstance().getRealUser();
      onChanged();
      return this;
    }
    /**
     * <code>optional string realUser = 3;</code>
     */
    public Builder setRealUserBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
      realUser_ = value;
      onChanged();
      return this;
    }

    private long issueDate_ ;
    /**
     * <code>optional int64 issueDate = 4;</code>
     */
    public boolean hasIssueDate() {
      return ((bitField0_ & 0x00000008) == 0x00000008);
    }
    /**
     * <code>optional int64 issueDate = 4;</code>
     */
    public long getIssueDate() {
      return issueDate_;
    }
    /**
     * <code>optional int64 issueDate = 4;</code>
     */
    public Builder setIssueDate(long value) {
      bitField0_ |= 0x00000008;
      issueDate_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int64 issueDate = 4;</code>
     */
    public Builder clearIssueDate() {
      bitField0_ = (bitField0_ & ~0x00000008);
      issueDate_ = 0L;
      onChanged();
      return this;
    }

    private long maxDate_ ;
    /**
     * <code>optional int64 maxDate = 5;</code>
     */
    public boolean hasMaxDate() {
      return ((bitField0_ & 0x00000010) == 0x00000010);
    }
    /**
     * <code>optional int64 maxDate = 5;</code>
     */
    public long getMaxDate() {
      return maxDate_;
    }
    /**
     * <code>optional int64 maxDate = 5;</code>
     */
    public Builder setMaxDate(long value) {
      bitField0_ |= 0x00000010;
      maxDate_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int64 maxDate = 5;</code>
     */
    public Builder clearMaxDate() {
      bitField0_ = (bitField0_ & ~0x00000010);
      maxDate_ = 0L;
      onChanged();
      return this;
    }

    private long sequenceNumber_ ;
    /**
     * <code>optional int64 sequenceNumber = 6;</code>
     */
    public boolean hasSequenceNumber() {
      return ((bitField0_ & 0x00000020) == 0x00000020);
    }
    /**
     * <code>optional int64 sequenceNumber = 6;</code>
     */
    public long getSequenceNumber() {
      return sequenceNumber_;
    }
    /**
     * <code>optional int64 sequenceNumber = 6;</code>
     */
    public Builder setSequenceNumber(long value) {
      bitField0_ |= 0x00000020;
      sequenceNumber_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int64 sequenceNumber = 6;</code>
     */
    public Builder clearSequenceNumber() {
      bitField0_ = (bitField0_ & ~0x00000020);
      sequenceNumber_ = 0L;
      onChanged();
      return this;
    }

    private long masterKeyId_ ;
    /**
     * <code>optional int64 masterKeyId = 7;</code>
     */
    public boolean hasMasterKeyId() {
      return ((bitField0_ & 0x00000040) == 0x00000040);
    }
    /**
     * <code>optional int64 masterKeyId = 7;</code>
     */
    public long getMasterKeyId() {
      return masterKeyId_;
    }
    /**
     * <code>optional int64 masterKeyId = 7;</code>
     */
    public Builder setMasterKeyId(long value) {
      bitField0_ |= 0x00000040;
      masterKeyId_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int64 masterKeyId = 7;</code>
     */
    public Builder clearMasterKeyId() {
      bitField0_ = (bitField0_ & ~0x00000040);
      masterKeyId_ = 0L;
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


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.file.DelegationTokenIdentifier)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.file.DelegationTokenIdentifier)
  private static final alluxio.grpc.DelegationTokenIdentifier DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.DelegationTokenIdentifier();
  }

  public static alluxio.grpc.DelegationTokenIdentifier getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<DelegationTokenIdentifier>
      PARSER = new com.google.protobuf.AbstractParser<DelegationTokenIdentifier>() {
    public DelegationTokenIdentifier parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new DelegationTokenIdentifier(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<DelegationTokenIdentifier> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<DelegationTokenIdentifier> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.DelegationTokenIdentifier getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

