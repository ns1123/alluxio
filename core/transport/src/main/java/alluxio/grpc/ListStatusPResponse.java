// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.file.ListStatusPResponse}
 */
public  final class ListStatusPResponse extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.file.ListStatusPResponse)
    ListStatusPResponseOrBuilder {
private static final long serialVersionUID = 0L;
  // Use ListStatusPResponse.newBuilder() to construct.
  private ListStatusPResponse(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private ListStatusPResponse() {
    fileInfos_ = java.util.Collections.emptyList();
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private ListStatusPResponse(
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
              fileInfos_ = new java.util.ArrayList<alluxio.grpc.FileInfo>();
              mutable_bitField0_ |= 0x00000001;
            }
            fileInfos_.add(
                input.readMessage(alluxio.grpc.FileInfo.PARSER, extensionRegistry));
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
        fileInfos_ = java.util.Collections.unmodifiableList(fileInfos_);
      }
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_ListStatusPResponse_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_ListStatusPResponse_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.ListStatusPResponse.class, alluxio.grpc.ListStatusPResponse.Builder.class);
  }

  public static final int FILEINFOS_FIELD_NUMBER = 1;
  private java.util.List<alluxio.grpc.FileInfo> fileInfos_;
  /**
   * <code>repeated .alluxio.grpc.file.FileInfo fileInfos = 1;</code>
   */
  public java.util.List<alluxio.grpc.FileInfo> getFileInfosList() {
    return fileInfos_;
  }
  /**
   * <code>repeated .alluxio.grpc.file.FileInfo fileInfos = 1;</code>
   */
  public java.util.List<? extends alluxio.grpc.FileInfoOrBuilder> 
      getFileInfosOrBuilderList() {
    return fileInfos_;
  }
  /**
   * <code>repeated .alluxio.grpc.file.FileInfo fileInfos = 1;</code>
   */
  public int getFileInfosCount() {
    return fileInfos_.size();
  }
  /**
   * <code>repeated .alluxio.grpc.file.FileInfo fileInfos = 1;</code>
   */
  public alluxio.grpc.FileInfo getFileInfos(int index) {
    return fileInfos_.get(index);
  }
  /**
   * <code>repeated .alluxio.grpc.file.FileInfo fileInfos = 1;</code>
   */
  public alluxio.grpc.FileInfoOrBuilder getFileInfosOrBuilder(
      int index) {
    return fileInfos_.get(index);
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
    for (int i = 0; i < fileInfos_.size(); i++) {
      output.writeMessage(1, fileInfos_.get(i));
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    for (int i = 0; i < fileInfos_.size(); i++) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(1, fileInfos_.get(i));
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
    if (!(obj instanceof alluxio.grpc.ListStatusPResponse)) {
      return super.equals(obj);
    }
    alluxio.grpc.ListStatusPResponse other = (alluxio.grpc.ListStatusPResponse) obj;

    boolean result = true;
    result = result && getFileInfosList()
        .equals(other.getFileInfosList());
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
    if (getFileInfosCount() > 0) {
      hash = (37 * hash) + FILEINFOS_FIELD_NUMBER;
      hash = (53 * hash) + getFileInfosList().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.ListStatusPResponse parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.ListStatusPResponse parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.ListStatusPResponse parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.ListStatusPResponse parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.ListStatusPResponse parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.ListStatusPResponse parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.ListStatusPResponse parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.ListStatusPResponse parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.ListStatusPResponse parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.ListStatusPResponse parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.ListStatusPResponse parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.ListStatusPResponse parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.ListStatusPResponse prototype) {
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
   * Protobuf type {@code alluxio.grpc.file.ListStatusPResponse}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.file.ListStatusPResponse)
      alluxio.grpc.ListStatusPResponseOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_ListStatusPResponse_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_ListStatusPResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.ListStatusPResponse.class, alluxio.grpc.ListStatusPResponse.Builder.class);
    }

    // Construct using alluxio.grpc.ListStatusPResponse.newBuilder()
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
        getFileInfosFieldBuilder();
      }
    }
    public Builder clear() {
      super.clear();
      if (fileInfosBuilder_ == null) {
        fileInfos_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000001);
      } else {
        fileInfosBuilder_.clear();
      }
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_ListStatusPResponse_descriptor;
    }

    public alluxio.grpc.ListStatusPResponse getDefaultInstanceForType() {
      return alluxio.grpc.ListStatusPResponse.getDefaultInstance();
    }

    public alluxio.grpc.ListStatusPResponse build() {
      alluxio.grpc.ListStatusPResponse result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.ListStatusPResponse buildPartial() {
      alluxio.grpc.ListStatusPResponse result = new alluxio.grpc.ListStatusPResponse(this);
      int from_bitField0_ = bitField0_;
      if (fileInfosBuilder_ == null) {
        if (((bitField0_ & 0x00000001) == 0x00000001)) {
          fileInfos_ = java.util.Collections.unmodifiableList(fileInfos_);
          bitField0_ = (bitField0_ & ~0x00000001);
        }
        result.fileInfos_ = fileInfos_;
      } else {
        result.fileInfos_ = fileInfosBuilder_.build();
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
      if (other instanceof alluxio.grpc.ListStatusPResponse) {
        return mergeFrom((alluxio.grpc.ListStatusPResponse)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.ListStatusPResponse other) {
      if (other == alluxio.grpc.ListStatusPResponse.getDefaultInstance()) return this;
      if (fileInfosBuilder_ == null) {
        if (!other.fileInfos_.isEmpty()) {
          if (fileInfos_.isEmpty()) {
            fileInfos_ = other.fileInfos_;
            bitField0_ = (bitField0_ & ~0x00000001);
          } else {
            ensureFileInfosIsMutable();
            fileInfos_.addAll(other.fileInfos_);
          }
          onChanged();
        }
      } else {
        if (!other.fileInfos_.isEmpty()) {
          if (fileInfosBuilder_.isEmpty()) {
            fileInfosBuilder_.dispose();
            fileInfosBuilder_ = null;
            fileInfos_ = other.fileInfos_;
            bitField0_ = (bitField0_ & ~0x00000001);
            fileInfosBuilder_ = 
              com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders ?
                 getFileInfosFieldBuilder() : null;
          } else {
            fileInfosBuilder_.addAllMessages(other.fileInfos_);
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
      alluxio.grpc.ListStatusPResponse parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.ListStatusPResponse) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private java.util.List<alluxio.grpc.FileInfo> fileInfos_ =
      java.util.Collections.emptyList();
    private void ensureFileInfosIsMutable() {
      if (!((bitField0_ & 0x00000001) == 0x00000001)) {
        fileInfos_ = new java.util.ArrayList<alluxio.grpc.FileInfo>(fileInfos_);
        bitField0_ |= 0x00000001;
       }
    }

    private com.google.protobuf.RepeatedFieldBuilderV3<
        alluxio.grpc.FileInfo, alluxio.grpc.FileInfo.Builder, alluxio.grpc.FileInfoOrBuilder> fileInfosBuilder_;

    /**
     * <code>repeated .alluxio.grpc.file.FileInfo fileInfos = 1;</code>
     */
    public java.util.List<alluxio.grpc.FileInfo> getFileInfosList() {
      if (fileInfosBuilder_ == null) {
        return java.util.Collections.unmodifiableList(fileInfos_);
      } else {
        return fileInfosBuilder_.getMessageList();
      }
    }
    /**
     * <code>repeated .alluxio.grpc.file.FileInfo fileInfos = 1;</code>
     */
    public int getFileInfosCount() {
      if (fileInfosBuilder_ == null) {
        return fileInfos_.size();
      } else {
        return fileInfosBuilder_.getCount();
      }
    }
    /**
     * <code>repeated .alluxio.grpc.file.FileInfo fileInfos = 1;</code>
     */
    public alluxio.grpc.FileInfo getFileInfos(int index) {
      if (fileInfosBuilder_ == null) {
        return fileInfos_.get(index);
      } else {
        return fileInfosBuilder_.getMessage(index);
      }
    }
    /**
     * <code>repeated .alluxio.grpc.file.FileInfo fileInfos = 1;</code>
     */
    public Builder setFileInfos(
        int index, alluxio.grpc.FileInfo value) {
      if (fileInfosBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureFileInfosIsMutable();
        fileInfos_.set(index, value);
        onChanged();
      } else {
        fileInfosBuilder_.setMessage(index, value);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.file.FileInfo fileInfos = 1;</code>
     */
    public Builder setFileInfos(
        int index, alluxio.grpc.FileInfo.Builder builderForValue) {
      if (fileInfosBuilder_ == null) {
        ensureFileInfosIsMutable();
        fileInfos_.set(index, builderForValue.build());
        onChanged();
      } else {
        fileInfosBuilder_.setMessage(index, builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.file.FileInfo fileInfos = 1;</code>
     */
    public Builder addFileInfos(alluxio.grpc.FileInfo value) {
      if (fileInfosBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureFileInfosIsMutable();
        fileInfos_.add(value);
        onChanged();
      } else {
        fileInfosBuilder_.addMessage(value);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.file.FileInfo fileInfos = 1;</code>
     */
    public Builder addFileInfos(
        int index, alluxio.grpc.FileInfo value) {
      if (fileInfosBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureFileInfosIsMutable();
        fileInfos_.add(index, value);
        onChanged();
      } else {
        fileInfosBuilder_.addMessage(index, value);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.file.FileInfo fileInfos = 1;</code>
     */
    public Builder addFileInfos(
        alluxio.grpc.FileInfo.Builder builderForValue) {
      if (fileInfosBuilder_ == null) {
        ensureFileInfosIsMutable();
        fileInfos_.add(builderForValue.build());
        onChanged();
      } else {
        fileInfosBuilder_.addMessage(builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.file.FileInfo fileInfos = 1;</code>
     */
    public Builder addFileInfos(
        int index, alluxio.grpc.FileInfo.Builder builderForValue) {
      if (fileInfosBuilder_ == null) {
        ensureFileInfosIsMutable();
        fileInfos_.add(index, builderForValue.build());
        onChanged();
      } else {
        fileInfosBuilder_.addMessage(index, builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.file.FileInfo fileInfos = 1;</code>
     */
    public Builder addAllFileInfos(
        java.lang.Iterable<? extends alluxio.grpc.FileInfo> values) {
      if (fileInfosBuilder_ == null) {
        ensureFileInfosIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
            values, fileInfos_);
        onChanged();
      } else {
        fileInfosBuilder_.addAllMessages(values);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.file.FileInfo fileInfos = 1;</code>
     */
    public Builder clearFileInfos() {
      if (fileInfosBuilder_ == null) {
        fileInfos_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000001);
        onChanged();
      } else {
        fileInfosBuilder_.clear();
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.file.FileInfo fileInfos = 1;</code>
     */
    public Builder removeFileInfos(int index) {
      if (fileInfosBuilder_ == null) {
        ensureFileInfosIsMutable();
        fileInfos_.remove(index);
        onChanged();
      } else {
        fileInfosBuilder_.remove(index);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.file.FileInfo fileInfos = 1;</code>
     */
    public alluxio.grpc.FileInfo.Builder getFileInfosBuilder(
        int index) {
      return getFileInfosFieldBuilder().getBuilder(index);
    }
    /**
     * <code>repeated .alluxio.grpc.file.FileInfo fileInfos = 1;</code>
     */
    public alluxio.grpc.FileInfoOrBuilder getFileInfosOrBuilder(
        int index) {
      if (fileInfosBuilder_ == null) {
        return fileInfos_.get(index);  } else {
        return fileInfosBuilder_.getMessageOrBuilder(index);
      }
    }
    /**
     * <code>repeated .alluxio.grpc.file.FileInfo fileInfos = 1;</code>
     */
    public java.util.List<? extends alluxio.grpc.FileInfoOrBuilder> 
         getFileInfosOrBuilderList() {
      if (fileInfosBuilder_ != null) {
        return fileInfosBuilder_.getMessageOrBuilderList();
      } else {
        return java.util.Collections.unmodifiableList(fileInfos_);
      }
    }
    /**
     * <code>repeated .alluxio.grpc.file.FileInfo fileInfos = 1;</code>
     */
    public alluxio.grpc.FileInfo.Builder addFileInfosBuilder() {
      return getFileInfosFieldBuilder().addBuilder(
          alluxio.grpc.FileInfo.getDefaultInstance());
    }
    /**
     * <code>repeated .alluxio.grpc.file.FileInfo fileInfos = 1;</code>
     */
    public alluxio.grpc.FileInfo.Builder addFileInfosBuilder(
        int index) {
      return getFileInfosFieldBuilder().addBuilder(
          index, alluxio.grpc.FileInfo.getDefaultInstance());
    }
    /**
     * <code>repeated .alluxio.grpc.file.FileInfo fileInfos = 1;</code>
     */
    public java.util.List<alluxio.grpc.FileInfo.Builder> 
         getFileInfosBuilderList() {
      return getFileInfosFieldBuilder().getBuilderList();
    }
    private com.google.protobuf.RepeatedFieldBuilderV3<
        alluxio.grpc.FileInfo, alluxio.grpc.FileInfo.Builder, alluxio.grpc.FileInfoOrBuilder> 
        getFileInfosFieldBuilder() {
      if (fileInfosBuilder_ == null) {
        fileInfosBuilder_ = new com.google.protobuf.RepeatedFieldBuilderV3<
            alluxio.grpc.FileInfo, alluxio.grpc.FileInfo.Builder, alluxio.grpc.FileInfoOrBuilder>(
                fileInfos_,
                ((bitField0_ & 0x00000001) == 0x00000001),
                getParentForChildren(),
                isClean());
        fileInfos_ = null;
      }
      return fileInfosBuilder_;
    }
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.file.ListStatusPResponse)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.file.ListStatusPResponse)
  private static final alluxio.grpc.ListStatusPResponse DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.ListStatusPResponse();
  }

  public static alluxio.grpc.ListStatusPResponse getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<ListStatusPResponse>
      PARSER = new com.google.protobuf.AbstractParser<ListStatusPResponse>() {
    public ListStatusPResponse parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new ListStatusPResponse(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<ListStatusPResponse> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<ListStatusPResponse> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.ListStatusPResponse getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

