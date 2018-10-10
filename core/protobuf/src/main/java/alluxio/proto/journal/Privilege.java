// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: journal/privilege.proto

package alluxio.proto.journal;

public final class Privilege {
  private Privilege() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  /**
   * Protobuf enum {@code alluxio.proto.journal.PPrivilege}
   *
   * <pre>
   * next available id: 4
   * The name "Privilege" is already taken by this file.
   * </pre>
   */
  public enum PPrivilege
      implements com.google.protobuf.ProtocolMessageEnum {
    /**
     * <code>FREE_PRIVILEGE = 0;</code>
     *
     * <pre>
     * Append "PRIVILEGE" to prevent collisions with other enums.
     * </pre>
     */
    FREE_PRIVILEGE(0, 0),
    /**
     * <code>PIN_PRIVILEGE = 1;</code>
     */
    PIN_PRIVILEGE(1, 1),
    /**
     * <code>REPLICATION_PRIVILEGE = 2;</code>
     */
    REPLICATION_PRIVILEGE(2, 2),
    /**
     * <code>TTL_PRIVILEGE = 3;</code>
     */
    TTL_PRIVILEGE(3, 3),
    ;

    /**
     * <code>FREE_PRIVILEGE = 0;</code>
     *
     * <pre>
     * Append "PRIVILEGE" to prevent collisions with other enums.
     * </pre>
     */
    public static final int FREE_PRIVILEGE_VALUE = 0;
    /**
     * <code>PIN_PRIVILEGE = 1;</code>
     */
    public static final int PIN_PRIVILEGE_VALUE = 1;
    /**
     * <code>REPLICATION_PRIVILEGE = 2;</code>
     */
    public static final int REPLICATION_PRIVILEGE_VALUE = 2;
    /**
     * <code>TTL_PRIVILEGE = 3;</code>
     */
    public static final int TTL_PRIVILEGE_VALUE = 3;


    public final int getNumber() { return value; }

    public static PPrivilege valueOf(int value) {
      switch (value) {
        case 0: return FREE_PRIVILEGE;
        case 1: return PIN_PRIVILEGE;
        case 2: return REPLICATION_PRIVILEGE;
        case 3: return TTL_PRIVILEGE;
        default: return null;
      }
    }

    public static com.google.protobuf.Internal.EnumLiteMap<PPrivilege>
        internalGetValueMap() {
      return internalValueMap;
    }
    private static com.google.protobuf.Internal.EnumLiteMap<PPrivilege>
        internalValueMap =
          new com.google.protobuf.Internal.EnumLiteMap<PPrivilege>() {
            public PPrivilege findValueByNumber(int number) {
              return PPrivilege.valueOf(number);
            }
          };

    public final com.google.protobuf.Descriptors.EnumValueDescriptor
        getValueDescriptor() {
      return getDescriptor().getValues().get(index);
    }
    public final com.google.protobuf.Descriptors.EnumDescriptor
        getDescriptorForType() {
      return getDescriptor();
    }
    public static final com.google.protobuf.Descriptors.EnumDescriptor
        getDescriptor() {
      return alluxio.proto.journal.Privilege.getDescriptor().getEnumTypes().get(0);
    }

    private static final PPrivilege[] VALUES = values();

    public static PPrivilege valueOf(
        com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
      if (desc.getType() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "EnumValueDescriptor is not for this type.");
      }
      return VALUES[desc.getIndex()];
    }

    private final int index;
    private final int value;

    private PPrivilege(int index, int value) {
      this.index = index;
      this.value = value;
    }

    // @@protoc_insertion_point(enum_scope:alluxio.proto.journal.PPrivilege)
  }

  public interface PrivilegeUpdateEntryOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // optional string group = 1;
    /**
     * <code>optional string group = 1;</code>
     */
    boolean hasGroup();
    /**
     * <code>optional string group = 1;</code>
     */
    java.lang.String getGroup();
    /**
     * <code>optional string group = 1;</code>
     */
    com.google.protobuf.ByteString
        getGroupBytes();

    // optional bool grant = 2;
    /**
     * <code>optional bool grant = 2;</code>
     */
    boolean hasGrant();
    /**
     * <code>optional bool grant = 2;</code>
     */
    boolean getGrant();

    // repeated .alluxio.proto.journal.PPrivilege privilege = 3;
    /**
     * <code>repeated .alluxio.proto.journal.PPrivilege privilege = 3;</code>
     */
    java.util.List<alluxio.proto.journal.Privilege.PPrivilege> getPrivilegeList();
    /**
     * <code>repeated .alluxio.proto.journal.PPrivilege privilege = 3;</code>
     */
    int getPrivilegeCount();
    /**
     * <code>repeated .alluxio.proto.journal.PPrivilege privilege = 3;</code>
     */
    alluxio.proto.journal.Privilege.PPrivilege getPrivilege(int index);
  }
  /**
   * Protobuf type {@code alluxio.proto.journal.PrivilegeUpdateEntry}
   *
   * <pre>
   * next available id: 4
   * </pre>
   */
  public static final class PrivilegeUpdateEntry extends
      com.google.protobuf.GeneratedMessage
      implements PrivilegeUpdateEntryOrBuilder {
    // Use PrivilegeUpdateEntry.newBuilder() to construct.
    private PrivilegeUpdateEntry(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private PrivilegeUpdateEntry(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final PrivilegeUpdateEntry defaultInstance;
    public static PrivilegeUpdateEntry getDefaultInstance() {
      return defaultInstance;
    }

    public PrivilegeUpdateEntry getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private PrivilegeUpdateEntry(
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
            case 10: {
              bitField0_ |= 0x00000001;
              group_ = input.readBytes();
              break;
            }
            case 16: {
              bitField0_ |= 0x00000002;
              grant_ = input.readBool();
              break;
            }
            case 24: {
              int rawValue = input.readEnum();
              alluxio.proto.journal.Privilege.PPrivilege value = alluxio.proto.journal.Privilege.PPrivilege.valueOf(rawValue);
              if (value == null) {
                unknownFields.mergeVarintField(3, rawValue);
              } else {
                if (!((mutable_bitField0_ & 0x00000004) == 0x00000004)) {
                  privilege_ = new java.util.ArrayList<alluxio.proto.journal.Privilege.PPrivilege>();
                  mutable_bitField0_ |= 0x00000004;
                }
                privilege_.add(value);
              }
              break;
            }
            case 26: {
              int length = input.readRawVarint32();
              int oldLimit = input.pushLimit(length);
              while(input.getBytesUntilLimit() > 0) {
                int rawValue = input.readEnum();
                alluxio.proto.journal.Privilege.PPrivilege value = alluxio.proto.journal.Privilege.PPrivilege.valueOf(rawValue);
                if (value == null) {
                  unknownFields.mergeVarintField(3, rawValue);
                } else {
                  if (!((mutable_bitField0_ & 0x00000004) == 0x00000004)) {
                    privilege_ = new java.util.ArrayList<alluxio.proto.journal.Privilege.PPrivilege>();
                    mutable_bitField0_ |= 0x00000004;
                  }
                  privilege_.add(value);
                }
              }
              input.popLimit(oldLimit);
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
        if (((mutable_bitField0_ & 0x00000004) == 0x00000004)) {
          privilege_ = java.util.Collections.unmodifiableList(privilege_);
        }
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.proto.journal.Privilege.internal_static_alluxio_proto_journal_PrivilegeUpdateEntry_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.proto.journal.Privilege.internal_static_alluxio_proto_journal_PrivilegeUpdateEntry_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.proto.journal.Privilege.PrivilegeUpdateEntry.class, alluxio.proto.journal.Privilege.PrivilegeUpdateEntry.Builder.class);
    }

    public static com.google.protobuf.Parser<PrivilegeUpdateEntry> PARSER =
        new com.google.protobuf.AbstractParser<PrivilegeUpdateEntry>() {
      public PrivilegeUpdateEntry parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PrivilegeUpdateEntry(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<PrivilegeUpdateEntry> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // optional string group = 1;
    public static final int GROUP_FIELD_NUMBER = 1;
    private java.lang.Object group_;
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

    // optional bool grant = 2;
    public static final int GRANT_FIELD_NUMBER = 2;
    private boolean grant_;
    /**
     * <code>optional bool grant = 2;</code>
     */
    public boolean hasGrant() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional bool grant = 2;</code>
     */
    public boolean getGrant() {
      return grant_;
    }

    // repeated .alluxio.proto.journal.PPrivilege privilege = 3;
    public static final int PRIVILEGE_FIELD_NUMBER = 3;
    private java.util.List<alluxio.proto.journal.Privilege.PPrivilege> privilege_;
    /**
     * <code>repeated .alluxio.proto.journal.PPrivilege privilege = 3;</code>
     */
    public java.util.List<alluxio.proto.journal.Privilege.PPrivilege> getPrivilegeList() {
      return privilege_;
    }
    /**
     * <code>repeated .alluxio.proto.journal.PPrivilege privilege = 3;</code>
     */
    public int getPrivilegeCount() {
      return privilege_.size();
    }
    /**
     * <code>repeated .alluxio.proto.journal.PPrivilege privilege = 3;</code>
     */
    public alluxio.proto.journal.Privilege.PPrivilege getPrivilege(int index) {
      return privilege_.get(index);
    }

    private void initFields() {
      group_ = "";
      grant_ = false;
      privilege_ = java.util.Collections.emptyList();
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
        output.writeBytes(1, getGroupBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeBool(2, grant_);
      }
      for (int i = 0; i < privilege_.size(); i++) {
        output.writeEnum(3, privilege_.get(i).getNumber());
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
          .computeBytesSize(1, getGroupBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBoolSize(2, grant_);
      }
      {
        int dataSize = 0;
        for (int i = 0; i < privilege_.size(); i++) {
          dataSize += com.google.protobuf.CodedOutputStream
            .computeEnumSizeNoTag(privilege_.get(i).getNumber());
        }
        size += dataSize;
        size += 1 * privilege_.size();
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

    public static alluxio.proto.journal.Privilege.PrivilegeUpdateEntry parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static alluxio.proto.journal.Privilege.PrivilegeUpdateEntry parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static alluxio.proto.journal.Privilege.PrivilegeUpdateEntry parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static alluxio.proto.journal.Privilege.PrivilegeUpdateEntry parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static alluxio.proto.journal.Privilege.PrivilegeUpdateEntry parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static alluxio.proto.journal.Privilege.PrivilegeUpdateEntry parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static alluxio.proto.journal.Privilege.PrivilegeUpdateEntry parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static alluxio.proto.journal.Privilege.PrivilegeUpdateEntry parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static alluxio.proto.journal.Privilege.PrivilegeUpdateEntry parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static alluxio.proto.journal.Privilege.PrivilegeUpdateEntry parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(alluxio.proto.journal.Privilege.PrivilegeUpdateEntry prototype) {
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
     * Protobuf type {@code alluxio.proto.journal.PrivilegeUpdateEntry}
     *
     * <pre>
     * next available id: 4
     * </pre>
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements alluxio.proto.journal.Privilege.PrivilegeUpdateEntryOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return alluxio.proto.journal.Privilege.internal_static_alluxio_proto_journal_PrivilegeUpdateEntry_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return alluxio.proto.journal.Privilege.internal_static_alluxio_proto_journal_PrivilegeUpdateEntry_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                alluxio.proto.journal.Privilege.PrivilegeUpdateEntry.class, alluxio.proto.journal.Privilege.PrivilegeUpdateEntry.Builder.class);
      }

      // Construct using alluxio.proto.journal.Privilege.PrivilegeUpdateEntry.newBuilder()
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
        group_ = "";
        bitField0_ = (bitField0_ & ~0x00000001);
        grant_ = false;
        bitField0_ = (bitField0_ & ~0x00000002);
        privilege_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000004);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return alluxio.proto.journal.Privilege.internal_static_alluxio_proto_journal_PrivilegeUpdateEntry_descriptor;
      }

      public alluxio.proto.journal.Privilege.PrivilegeUpdateEntry getDefaultInstanceForType() {
        return alluxio.proto.journal.Privilege.PrivilegeUpdateEntry.getDefaultInstance();
      }

      public alluxio.proto.journal.Privilege.PrivilegeUpdateEntry build() {
        alluxio.proto.journal.Privilege.PrivilegeUpdateEntry result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public alluxio.proto.journal.Privilege.PrivilegeUpdateEntry buildPartial() {
        alluxio.proto.journal.Privilege.PrivilegeUpdateEntry result = new alluxio.proto.journal.Privilege.PrivilegeUpdateEntry(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.group_ = group_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.grant_ = grant_;
        if (((bitField0_ & 0x00000004) == 0x00000004)) {
          privilege_ = java.util.Collections.unmodifiableList(privilege_);
          bitField0_ = (bitField0_ & ~0x00000004);
        }
        result.privilege_ = privilege_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof alluxio.proto.journal.Privilege.PrivilegeUpdateEntry) {
          return mergeFrom((alluxio.proto.journal.Privilege.PrivilegeUpdateEntry)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(alluxio.proto.journal.Privilege.PrivilegeUpdateEntry other) {
        if (other == alluxio.proto.journal.Privilege.PrivilegeUpdateEntry.getDefaultInstance()) return this;
        if (other.hasGroup()) {
          bitField0_ |= 0x00000001;
          group_ = other.group_;
          onChanged();
        }
        if (other.hasGrant()) {
          setGrant(other.getGrant());
        }
        if (!other.privilege_.isEmpty()) {
          if (privilege_.isEmpty()) {
            privilege_ = other.privilege_;
            bitField0_ = (bitField0_ & ~0x00000004);
          } else {
            ensurePrivilegeIsMutable();
            privilege_.addAll(other.privilege_);
          }
          onChanged();
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
        alluxio.proto.journal.Privilege.PrivilegeUpdateEntry parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (alluxio.proto.journal.Privilege.PrivilegeUpdateEntry) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // optional string group = 1;
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
          java.lang.String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          group_ = s;
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

      // optional bool grant = 2;
      private boolean grant_ ;
      /**
       * <code>optional bool grant = 2;</code>
       */
      public boolean hasGrant() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>optional bool grant = 2;</code>
       */
      public boolean getGrant() {
        return grant_;
      }
      /**
       * <code>optional bool grant = 2;</code>
       */
      public Builder setGrant(boolean value) {
        bitField0_ |= 0x00000002;
        grant_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bool grant = 2;</code>
       */
      public Builder clearGrant() {
        bitField0_ = (bitField0_ & ~0x00000002);
        grant_ = false;
        onChanged();
        return this;
      }

      // repeated .alluxio.proto.journal.PPrivilege privilege = 3;
      private java.util.List<alluxio.proto.journal.Privilege.PPrivilege> privilege_ =
        java.util.Collections.emptyList();
      private void ensurePrivilegeIsMutable() {
        if (!((bitField0_ & 0x00000004) == 0x00000004)) {
          privilege_ = new java.util.ArrayList<alluxio.proto.journal.Privilege.PPrivilege>(privilege_);
          bitField0_ |= 0x00000004;
        }
      }
      /**
       * <code>repeated .alluxio.proto.journal.PPrivilege privilege = 3;</code>
       */
      public java.util.List<alluxio.proto.journal.Privilege.PPrivilege> getPrivilegeList() {
        return java.util.Collections.unmodifiableList(privilege_);
      }
      /**
       * <code>repeated .alluxio.proto.journal.PPrivilege privilege = 3;</code>
       */
      public int getPrivilegeCount() {
        return privilege_.size();
      }
      /**
       * <code>repeated .alluxio.proto.journal.PPrivilege privilege = 3;</code>
       */
      public alluxio.proto.journal.Privilege.PPrivilege getPrivilege(int index) {
        return privilege_.get(index);
      }
      /**
       * <code>repeated .alluxio.proto.journal.PPrivilege privilege = 3;</code>
       */
      public Builder setPrivilege(
          int index, alluxio.proto.journal.Privilege.PPrivilege value) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensurePrivilegeIsMutable();
        privilege_.set(index, value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated .alluxio.proto.journal.PPrivilege privilege = 3;</code>
       */
      public Builder addPrivilege(alluxio.proto.journal.Privilege.PPrivilege value) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensurePrivilegeIsMutable();
        privilege_.add(value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated .alluxio.proto.journal.PPrivilege privilege = 3;</code>
       */
      public Builder addAllPrivilege(
          java.lang.Iterable<? extends alluxio.proto.journal.Privilege.PPrivilege> values) {
        ensurePrivilegeIsMutable();
        super.addAll(values, privilege_);
        onChanged();
        return this;
      }
      /**
       * <code>repeated .alluxio.proto.journal.PPrivilege privilege = 3;</code>
       */
      public Builder clearPrivilege() {
        privilege_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000004);
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:alluxio.proto.journal.PrivilegeUpdateEntry)
    }

    static {
      defaultInstance = new PrivilegeUpdateEntry(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:alluxio.proto.journal.PrivilegeUpdateEntry)
  }

  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_proto_journal_PrivilegeUpdateEntry_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_alluxio_proto_journal_PrivilegeUpdateEntry_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\027journal/privilege.proto\022\025alluxio.proto" +
      ".journal\"j\n\024PrivilegeUpdateEntry\022\r\n\005grou" +
      "p\030\001 \001(\t\022\r\n\005grant\030\002 \001(\010\0224\n\tprivilege\030\003 \003(" +
      "\0162!.alluxio.proto.journal.PPrivilege*a\n\n" +
      "PPrivilege\022\022\n\016FREE_PRIVILEGE\020\000\022\021\n\rPIN_PR" +
      "IVILEGE\020\001\022\031\n\025REPLICATION_PRIVILEGE\020\002\022\021\n\r" +
      "TTL_PRIVILEGE\020\003"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_alluxio_proto_journal_PrivilegeUpdateEntry_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_alluxio_proto_journal_PrivilegeUpdateEntry_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_alluxio_proto_journal_PrivilegeUpdateEntry_descriptor,
              new java.lang.String[] { "Group", "Grant", "Privilege", });
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