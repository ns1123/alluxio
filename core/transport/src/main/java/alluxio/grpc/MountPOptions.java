// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.file.MountPOptions}
 */
public  final class MountPOptions extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.file.MountPOptions)
    MountPOptionsOrBuilder {
private static final long serialVersionUID = 0L;
  // Use MountPOptions.newBuilder() to construct.
  private MountPOptions(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private MountPOptions() {
    readOnly_ = false;
    shared_ = false;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private MountPOptions(
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
            readOnly_ = input.readBool();
            break;
          }
          case 18: {
            if (!((mutable_bitField0_ & 0x00000002) == 0x00000002)) {
              properties_ = com.google.protobuf.MapField.newMapField(
                  PropertiesDefaultEntryHolder.defaultEntry);
              mutable_bitField0_ |= 0x00000002;
            }
            com.google.protobuf.MapEntry<java.lang.String, java.lang.String>
            properties__ = input.readMessage(
                PropertiesDefaultEntryHolder.defaultEntry.getParserForType(), extensionRegistry);
            properties_.getMutableMap().put(
                properties__.getKey(), properties__.getValue());
            break;
          }
          case 24: {
            bitField0_ |= 0x00000002;
            shared_ = input.readBool();
            break;
          }
          case 34: {
            alluxio.grpc.FileSystemMasterCommonPOptions.Builder subBuilder = null;
            if (((bitField0_ & 0x00000004) == 0x00000004)) {
              subBuilder = commonOptions_.toBuilder();
            }
            commonOptions_ = input.readMessage(alluxio.grpc.FileSystemMasterCommonPOptions.PARSER, extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom(commonOptions_);
              commonOptions_ = subBuilder.buildPartial();
            }
            bitField0_ |= 0x00000004;
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
    return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_MountPOptions_descriptor;
  }

  @SuppressWarnings({"rawtypes"})
  protected com.google.protobuf.MapField internalGetMapField(
      int number) {
    switch (number) {
      case 2:
        return internalGetProperties();
      default:
        throw new RuntimeException(
            "Invalid map field number: " + number);
    }
  }
  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_MountPOptions_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.MountPOptions.class, alluxio.grpc.MountPOptions.Builder.class);
  }

  private int bitField0_;
  public static final int READONLY_FIELD_NUMBER = 1;
  private boolean readOnly_;
  /**
   * <code>optional bool readOnly = 1;</code>
   */
  public boolean hasReadOnly() {
    return ((bitField0_ & 0x00000001) == 0x00000001);
  }
  /**
   * <code>optional bool readOnly = 1;</code>
   */
  public boolean getReadOnly() {
    return readOnly_;
  }

  public static final int PROPERTIES_FIELD_NUMBER = 2;
  private static final class PropertiesDefaultEntryHolder {
    static final com.google.protobuf.MapEntry<
        java.lang.String, java.lang.String> defaultEntry =
            com.google.protobuf.MapEntry
            .<java.lang.String, java.lang.String>newDefaultInstance(
                alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_MountPOptions_PropertiesEntry_descriptor, 
                com.google.protobuf.WireFormat.FieldType.STRING,
                "",
                com.google.protobuf.WireFormat.FieldType.STRING,
                "");
  }
  private com.google.protobuf.MapField<
      java.lang.String, java.lang.String> properties_;
  private com.google.protobuf.MapField<java.lang.String, java.lang.String>
  internalGetProperties() {
    if (properties_ == null) {
      return com.google.protobuf.MapField.emptyMapField(
          PropertiesDefaultEntryHolder.defaultEntry);
    }
    return properties_;
  }

  public int getPropertiesCount() {
    return internalGetProperties().getMap().size();
  }
  /**
   * <code>map&lt;string, string&gt; properties = 2;</code>
   */

  public boolean containsProperties(
      java.lang.String key) {
    if (key == null) { throw new java.lang.NullPointerException(); }
    return internalGetProperties().getMap().containsKey(key);
  }
  /**
   * Use {@link #getPropertiesMap()} instead.
   */
  @java.lang.Deprecated
  public java.util.Map<java.lang.String, java.lang.String> getProperties() {
    return getPropertiesMap();
  }
  /**
   * <code>map&lt;string, string&gt; properties = 2;</code>
   */

  public java.util.Map<java.lang.String, java.lang.String> getPropertiesMap() {
    return internalGetProperties().getMap();
  }
  /**
   * <code>map&lt;string, string&gt; properties = 2;</code>
   */

  public java.lang.String getPropertiesOrDefault(
      java.lang.String key,
      java.lang.String defaultValue) {
    if (key == null) { throw new java.lang.NullPointerException(); }
    java.util.Map<java.lang.String, java.lang.String> map =
        internalGetProperties().getMap();
    return map.containsKey(key) ? map.get(key) : defaultValue;
  }
  /**
   * <code>map&lt;string, string&gt; properties = 2;</code>
   */

  public java.lang.String getPropertiesOrThrow(
      java.lang.String key) {
    if (key == null) { throw new java.lang.NullPointerException(); }
    java.util.Map<java.lang.String, java.lang.String> map =
        internalGetProperties().getMap();
    if (!map.containsKey(key)) {
      throw new java.lang.IllegalArgumentException();
    }
    return map.get(key);
  }

  public static final int SHARED_FIELD_NUMBER = 3;
  private boolean shared_;
  /**
   * <code>optional bool shared = 3;</code>
   */
  public boolean hasShared() {
    return ((bitField0_ & 0x00000002) == 0x00000002);
  }
  /**
   * <code>optional bool shared = 3;</code>
   */
  public boolean getShared() {
    return shared_;
  }

  public static final int COMMONOPTIONS_FIELD_NUMBER = 4;
  private alluxio.grpc.FileSystemMasterCommonPOptions commonOptions_;
  /**
   * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 4;</code>
   */
  public boolean hasCommonOptions() {
    return ((bitField0_ & 0x00000004) == 0x00000004);
  }
  /**
   * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 4;</code>
   */
  public alluxio.grpc.FileSystemMasterCommonPOptions getCommonOptions() {
    return commonOptions_ == null ? alluxio.grpc.FileSystemMasterCommonPOptions.getDefaultInstance() : commonOptions_;
  }
  /**
   * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 4;</code>
   */
  public alluxio.grpc.FileSystemMasterCommonPOptionsOrBuilder getCommonOptionsOrBuilder() {
    return commonOptions_ == null ? alluxio.grpc.FileSystemMasterCommonPOptions.getDefaultInstance() : commonOptions_;
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
      output.writeBool(1, readOnly_);
    }
    com.google.protobuf.GeneratedMessageV3
      .serializeStringMapTo(
        output,
        internalGetProperties(),
        PropertiesDefaultEntryHolder.defaultEntry,
        2);
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      output.writeBool(3, shared_);
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      output.writeMessage(4, getCommonOptions());
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      size += com.google.protobuf.CodedOutputStream
        .computeBoolSize(1, readOnly_);
    }
    for (java.util.Map.Entry<java.lang.String, java.lang.String> entry
         : internalGetProperties().getMap().entrySet()) {
      com.google.protobuf.MapEntry<java.lang.String, java.lang.String>
      properties__ = PropertiesDefaultEntryHolder.defaultEntry.newBuilderForType()
          .setKey(entry.getKey())
          .setValue(entry.getValue())
          .build();
      size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(2, properties__);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      size += com.google.protobuf.CodedOutputStream
        .computeBoolSize(3, shared_);
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(4, getCommonOptions());
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
    if (!(obj instanceof alluxio.grpc.MountPOptions)) {
      return super.equals(obj);
    }
    alluxio.grpc.MountPOptions other = (alluxio.grpc.MountPOptions) obj;

    boolean result = true;
    result = result && (hasReadOnly() == other.hasReadOnly());
    if (hasReadOnly()) {
      result = result && (getReadOnly()
          == other.getReadOnly());
    }
    result = result && internalGetProperties().equals(
        other.internalGetProperties());
    result = result && (hasShared() == other.hasShared());
    if (hasShared()) {
      result = result && (getShared()
          == other.getShared());
    }
    result = result && (hasCommonOptions() == other.hasCommonOptions());
    if (hasCommonOptions()) {
      result = result && getCommonOptions()
          .equals(other.getCommonOptions());
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
    if (hasReadOnly()) {
      hash = (37 * hash) + READONLY_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
          getReadOnly());
    }
    if (!internalGetProperties().getMap().isEmpty()) {
      hash = (37 * hash) + PROPERTIES_FIELD_NUMBER;
      hash = (53 * hash) + internalGetProperties().hashCode();
    }
    if (hasShared()) {
      hash = (37 * hash) + SHARED_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
          getShared());
    }
    if (hasCommonOptions()) {
      hash = (37 * hash) + COMMONOPTIONS_FIELD_NUMBER;
      hash = (53 * hash) + getCommonOptions().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.MountPOptions parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.MountPOptions parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.MountPOptions parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.MountPOptions parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.MountPOptions parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.MountPOptions parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.MountPOptions parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.MountPOptions parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.MountPOptions parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.MountPOptions parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.MountPOptions parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.MountPOptions parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.MountPOptions prototype) {
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
   * Protobuf type {@code alluxio.grpc.file.MountPOptions}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.file.MountPOptions)
      alluxio.grpc.MountPOptionsOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_MountPOptions_descriptor;
    }

    @SuppressWarnings({"rawtypes"})
    protected com.google.protobuf.MapField internalGetMapField(
        int number) {
      switch (number) {
        case 2:
          return internalGetProperties();
        default:
          throw new RuntimeException(
              "Invalid map field number: " + number);
      }
    }
    @SuppressWarnings({"rawtypes"})
    protected com.google.protobuf.MapField internalGetMutableMapField(
        int number) {
      switch (number) {
        case 2:
          return internalGetMutableProperties();
        default:
          throw new RuntimeException(
              "Invalid map field number: " + number);
      }
    }
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_MountPOptions_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.MountPOptions.class, alluxio.grpc.MountPOptions.Builder.class);
    }

    // Construct using alluxio.grpc.MountPOptions.newBuilder()
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
        getCommonOptionsFieldBuilder();
      }
    }
    public Builder clear() {
      super.clear();
      readOnly_ = false;
      bitField0_ = (bitField0_ & ~0x00000001);
      internalGetMutableProperties().clear();
      shared_ = false;
      bitField0_ = (bitField0_ & ~0x00000004);
      if (commonOptionsBuilder_ == null) {
        commonOptions_ = null;
      } else {
        commonOptionsBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000008);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_MountPOptions_descriptor;
    }

    public alluxio.grpc.MountPOptions getDefaultInstanceForType() {
      return alluxio.grpc.MountPOptions.getDefaultInstance();
    }

    public alluxio.grpc.MountPOptions build() {
      alluxio.grpc.MountPOptions result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.MountPOptions buildPartial() {
      alluxio.grpc.MountPOptions result = new alluxio.grpc.MountPOptions(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
        to_bitField0_ |= 0x00000001;
      }
      result.readOnly_ = readOnly_;
      result.properties_ = internalGetProperties();
      result.properties_.makeImmutable();
      if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
        to_bitField0_ |= 0x00000002;
      }
      result.shared_ = shared_;
      if (((from_bitField0_ & 0x00000008) == 0x00000008)) {
        to_bitField0_ |= 0x00000004;
      }
      if (commonOptionsBuilder_ == null) {
        result.commonOptions_ = commonOptions_;
      } else {
        result.commonOptions_ = commonOptionsBuilder_.build();
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
      if (other instanceof alluxio.grpc.MountPOptions) {
        return mergeFrom((alluxio.grpc.MountPOptions)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.MountPOptions other) {
      if (other == alluxio.grpc.MountPOptions.getDefaultInstance()) return this;
      if (other.hasReadOnly()) {
        setReadOnly(other.getReadOnly());
      }
      internalGetMutableProperties().mergeFrom(
          other.internalGetProperties());
      if (other.hasShared()) {
        setShared(other.getShared());
      }
      if (other.hasCommonOptions()) {
        mergeCommonOptions(other.getCommonOptions());
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
      alluxio.grpc.MountPOptions parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.MountPOptions) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private boolean readOnly_ ;
    /**
     * <code>optional bool readOnly = 1;</code>
     */
    public boolean hasReadOnly() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional bool readOnly = 1;</code>
     */
    public boolean getReadOnly() {
      return readOnly_;
    }
    /**
     * <code>optional bool readOnly = 1;</code>
     */
    public Builder setReadOnly(boolean value) {
      bitField0_ |= 0x00000001;
      readOnly_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional bool readOnly = 1;</code>
     */
    public Builder clearReadOnly() {
      bitField0_ = (bitField0_ & ~0x00000001);
      readOnly_ = false;
      onChanged();
      return this;
    }

    private com.google.protobuf.MapField<
        java.lang.String, java.lang.String> properties_;
    private com.google.protobuf.MapField<java.lang.String, java.lang.String>
    internalGetProperties() {
      if (properties_ == null) {
        return com.google.protobuf.MapField.emptyMapField(
            PropertiesDefaultEntryHolder.defaultEntry);
      }
      return properties_;
    }
    private com.google.protobuf.MapField<java.lang.String, java.lang.String>
    internalGetMutableProperties() {
      onChanged();;
      if (properties_ == null) {
        properties_ = com.google.protobuf.MapField.newMapField(
            PropertiesDefaultEntryHolder.defaultEntry);
      }
      if (!properties_.isMutable()) {
        properties_ = properties_.copy();
      }
      return properties_;
    }

    public int getPropertiesCount() {
      return internalGetProperties().getMap().size();
    }
    /**
     * <code>map&lt;string, string&gt; properties = 2;</code>
     */

    public boolean containsProperties(
        java.lang.String key) {
      if (key == null) { throw new java.lang.NullPointerException(); }
      return internalGetProperties().getMap().containsKey(key);
    }
    /**
     * Use {@link #getPropertiesMap()} instead.
     */
    @java.lang.Deprecated
    public java.util.Map<java.lang.String, java.lang.String> getProperties() {
      return getPropertiesMap();
    }
    /**
     * <code>map&lt;string, string&gt; properties = 2;</code>
     */

    public java.util.Map<java.lang.String, java.lang.String> getPropertiesMap() {
      return internalGetProperties().getMap();
    }
    /**
     * <code>map&lt;string, string&gt; properties = 2;</code>
     */

    public java.lang.String getPropertiesOrDefault(
        java.lang.String key,
        java.lang.String defaultValue) {
      if (key == null) { throw new java.lang.NullPointerException(); }
      java.util.Map<java.lang.String, java.lang.String> map =
          internalGetProperties().getMap();
      return map.containsKey(key) ? map.get(key) : defaultValue;
    }
    /**
     * <code>map&lt;string, string&gt; properties = 2;</code>
     */

    public java.lang.String getPropertiesOrThrow(
        java.lang.String key) {
      if (key == null) { throw new java.lang.NullPointerException(); }
      java.util.Map<java.lang.String, java.lang.String> map =
          internalGetProperties().getMap();
      if (!map.containsKey(key)) {
        throw new java.lang.IllegalArgumentException();
      }
      return map.get(key);
    }

    public Builder clearProperties() {
      internalGetMutableProperties().getMutableMap()
          .clear();
      return this;
    }
    /**
     * <code>map&lt;string, string&gt; properties = 2;</code>
     */

    public Builder removeProperties(
        java.lang.String key) {
      if (key == null) { throw new java.lang.NullPointerException(); }
      internalGetMutableProperties().getMutableMap()
          .remove(key);
      return this;
    }
    /**
     * Use alternate mutation accessors instead.
     */
    @java.lang.Deprecated
    public java.util.Map<java.lang.String, java.lang.String>
    getMutableProperties() {
      return internalGetMutableProperties().getMutableMap();
    }
    /**
     * <code>map&lt;string, string&gt; properties = 2;</code>
     */
    public Builder putProperties(
        java.lang.String key,
        java.lang.String value) {
      if (key == null) { throw new java.lang.NullPointerException(); }
      if (value == null) { throw new java.lang.NullPointerException(); }
      internalGetMutableProperties().getMutableMap()
          .put(key, value);
      return this;
    }
    /**
     * <code>map&lt;string, string&gt; properties = 2;</code>
     */

    public Builder putAllProperties(
        java.util.Map<java.lang.String, java.lang.String> values) {
      internalGetMutableProperties().getMutableMap()
          .putAll(values);
      return this;
    }

    private boolean shared_ ;
    /**
     * <code>optional bool shared = 3;</code>
     */
    public boolean hasShared() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional bool shared = 3;</code>
     */
    public boolean getShared() {
      return shared_;
    }
    /**
     * <code>optional bool shared = 3;</code>
     */
    public Builder setShared(boolean value) {
      bitField0_ |= 0x00000004;
      shared_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional bool shared = 3;</code>
     */
    public Builder clearShared() {
      bitField0_ = (bitField0_ & ~0x00000004);
      shared_ = false;
      onChanged();
      return this;
    }

    private alluxio.grpc.FileSystemMasterCommonPOptions commonOptions_ = null;
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.grpc.FileSystemMasterCommonPOptions, alluxio.grpc.FileSystemMasterCommonPOptions.Builder, alluxio.grpc.FileSystemMasterCommonPOptionsOrBuilder> commonOptionsBuilder_;
    /**
     * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 4;</code>
     */
    public boolean hasCommonOptions() {
      return ((bitField0_ & 0x00000008) == 0x00000008);
    }
    /**
     * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 4;</code>
     */
    public alluxio.grpc.FileSystemMasterCommonPOptions getCommonOptions() {
      if (commonOptionsBuilder_ == null) {
        return commonOptions_ == null ? alluxio.grpc.FileSystemMasterCommonPOptions.getDefaultInstance() : commonOptions_;
      } else {
        return commonOptionsBuilder_.getMessage();
      }
    }
    /**
     * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 4;</code>
     */
    public Builder setCommonOptions(alluxio.grpc.FileSystemMasterCommonPOptions value) {
      if (commonOptionsBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        commonOptions_ = value;
        onChanged();
      } else {
        commonOptionsBuilder_.setMessage(value);
      }
      bitField0_ |= 0x00000008;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 4;</code>
     */
    public Builder setCommonOptions(
        alluxio.grpc.FileSystemMasterCommonPOptions.Builder builderForValue) {
      if (commonOptionsBuilder_ == null) {
        commonOptions_ = builderForValue.build();
        onChanged();
      } else {
        commonOptionsBuilder_.setMessage(builderForValue.build());
      }
      bitField0_ |= 0x00000008;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 4;</code>
     */
    public Builder mergeCommonOptions(alluxio.grpc.FileSystemMasterCommonPOptions value) {
      if (commonOptionsBuilder_ == null) {
        if (((bitField0_ & 0x00000008) == 0x00000008) &&
            commonOptions_ != null &&
            commonOptions_ != alluxio.grpc.FileSystemMasterCommonPOptions.getDefaultInstance()) {
          commonOptions_ =
            alluxio.grpc.FileSystemMasterCommonPOptions.newBuilder(commonOptions_).mergeFrom(value).buildPartial();
        } else {
          commonOptions_ = value;
        }
        onChanged();
      } else {
        commonOptionsBuilder_.mergeFrom(value);
      }
      bitField0_ |= 0x00000008;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 4;</code>
     */
    public Builder clearCommonOptions() {
      if (commonOptionsBuilder_ == null) {
        commonOptions_ = null;
        onChanged();
      } else {
        commonOptionsBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000008);
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 4;</code>
     */
    public alluxio.grpc.FileSystemMasterCommonPOptions.Builder getCommonOptionsBuilder() {
      bitField0_ |= 0x00000008;
      onChanged();
      return getCommonOptionsFieldBuilder().getBuilder();
    }
    /**
     * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 4;</code>
     */
    public alluxio.grpc.FileSystemMasterCommonPOptionsOrBuilder getCommonOptionsOrBuilder() {
      if (commonOptionsBuilder_ != null) {
        return commonOptionsBuilder_.getMessageOrBuilder();
      } else {
        return commonOptions_ == null ?
            alluxio.grpc.FileSystemMasterCommonPOptions.getDefaultInstance() : commonOptions_;
      }
    }
    /**
     * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 4;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.grpc.FileSystemMasterCommonPOptions, alluxio.grpc.FileSystemMasterCommonPOptions.Builder, alluxio.grpc.FileSystemMasterCommonPOptionsOrBuilder> 
        getCommonOptionsFieldBuilder() {
      if (commonOptionsBuilder_ == null) {
        commonOptionsBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            alluxio.grpc.FileSystemMasterCommonPOptions, alluxio.grpc.FileSystemMasterCommonPOptions.Builder, alluxio.grpc.FileSystemMasterCommonPOptionsOrBuilder>(
                getCommonOptions(),
                getParentForChildren(),
                isClean());
        commonOptions_ = null;
      }
      return commonOptionsBuilder_;
    }
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.file.MountPOptions)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.file.MountPOptions)
  private static final alluxio.grpc.MountPOptions DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.MountPOptions();
  }

  public static alluxio.grpc.MountPOptions getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<MountPOptions>
      PARSER = new com.google.protobuf.AbstractParser<MountPOptions>() {
    public MountPOptions parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new MountPOptions(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<MountPOptions> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<MountPOptions> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.MountPOptions getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

