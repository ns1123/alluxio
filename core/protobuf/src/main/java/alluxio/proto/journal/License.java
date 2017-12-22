// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: journal/license.proto

package alluxio.proto.journal;

public final class License {
  private License() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface LicenseCheckEntryOrBuilder extends
      // @@protoc_insertion_point(interface_extends:alluxio.proto.journal.LicenseCheckEntry)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>optional int64 time_ms = 1;</code>
     */
    boolean hasTimeMs();
    /**
     * <code>optional int64 time_ms = 1;</code>
     */
    long getTimeMs();
  }
  /**
   * Protobuf type {@code alluxio.proto.journal.LicenseCheckEntry}
   *
   * <pre>
   * next available id: 2
   * </pre>
   */
  public static final class LicenseCheckEntry extends
      com.google.protobuf.GeneratedMessage implements
      // @@protoc_insertion_point(message_implements:alluxio.proto.journal.LicenseCheckEntry)
      LicenseCheckEntryOrBuilder {
    // Use LicenseCheckEntry.newBuilder() to construct.
    private LicenseCheckEntry(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private LicenseCheckEntry(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final LicenseCheckEntry defaultInstance;
    public static LicenseCheckEntry getDefaultInstance() {
      return defaultInstance;
    }

    public LicenseCheckEntry getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private LicenseCheckEntry(
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
            case 8: {
              bitField0_ |= 0x00000001;
              timeMs_ = input.readInt64();
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
      return alluxio.proto.journal.License.internal_static_alluxio_proto_journal_LicenseCheckEntry_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.proto.journal.License.internal_static_alluxio_proto_journal_LicenseCheckEntry_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.proto.journal.License.LicenseCheckEntry.class, alluxio.proto.journal.License.LicenseCheckEntry.Builder.class);
    }

    public static com.google.protobuf.Parser<LicenseCheckEntry> PARSER =
        new com.google.protobuf.AbstractParser<LicenseCheckEntry>() {
      public LicenseCheckEntry parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new LicenseCheckEntry(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<LicenseCheckEntry> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    public static final int TIME_MS_FIELD_NUMBER = 1;
    private long timeMs_;
    /**
     * <code>optional int64 time_ms = 1;</code>
     */
    public boolean hasTimeMs() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional int64 time_ms = 1;</code>
     */
    public long getTimeMs() {
      return timeMs_;
    }

    private void initFields() {
      timeMs_ = 0L;
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
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeInt64(1, timeMs_);
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
          .computeInt64Size(1, timeMs_);
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

    public static alluxio.proto.journal.License.LicenseCheckEntry parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static alluxio.proto.journal.License.LicenseCheckEntry parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static alluxio.proto.journal.License.LicenseCheckEntry parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static alluxio.proto.journal.License.LicenseCheckEntry parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static alluxio.proto.journal.License.LicenseCheckEntry parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static alluxio.proto.journal.License.LicenseCheckEntry parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static alluxio.proto.journal.License.LicenseCheckEntry parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static alluxio.proto.journal.License.LicenseCheckEntry parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static alluxio.proto.journal.License.LicenseCheckEntry parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static alluxio.proto.journal.License.LicenseCheckEntry parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(alluxio.proto.journal.License.LicenseCheckEntry prototype) {
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
     * Protobuf type {@code alluxio.proto.journal.LicenseCheckEntry}
     *
     * <pre>
     * next available id: 2
     * </pre>
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:alluxio.proto.journal.LicenseCheckEntry)
        alluxio.proto.journal.License.LicenseCheckEntryOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return alluxio.proto.journal.License.internal_static_alluxio_proto_journal_LicenseCheckEntry_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return alluxio.proto.journal.License.internal_static_alluxio_proto_journal_LicenseCheckEntry_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                alluxio.proto.journal.License.LicenseCheckEntry.class, alluxio.proto.journal.License.LicenseCheckEntry.Builder.class);
      }

      // Construct using alluxio.proto.journal.License.LicenseCheckEntry.newBuilder()
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
        timeMs_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return alluxio.proto.journal.License.internal_static_alluxio_proto_journal_LicenseCheckEntry_descriptor;
      }

      public alluxio.proto.journal.License.LicenseCheckEntry getDefaultInstanceForType() {
        return alluxio.proto.journal.License.LicenseCheckEntry.getDefaultInstance();
      }

      public alluxio.proto.journal.License.LicenseCheckEntry build() {
        alluxio.proto.journal.License.LicenseCheckEntry result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public alluxio.proto.journal.License.LicenseCheckEntry buildPartial() {
        alluxio.proto.journal.License.LicenseCheckEntry result = new alluxio.proto.journal.License.LicenseCheckEntry(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.timeMs_ = timeMs_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof alluxio.proto.journal.License.LicenseCheckEntry) {
          return mergeFrom((alluxio.proto.journal.License.LicenseCheckEntry)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(alluxio.proto.journal.License.LicenseCheckEntry other) {
        if (other == alluxio.proto.journal.License.LicenseCheckEntry.getDefaultInstance()) return this;
        if (other.hasTimeMs()) {
          setTimeMs(other.getTimeMs());
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
        alluxio.proto.journal.License.LicenseCheckEntry parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (alluxio.proto.journal.License.LicenseCheckEntry) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private long timeMs_ ;
      /**
       * <code>optional int64 time_ms = 1;</code>
       */
      public boolean hasTimeMs() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>optional int64 time_ms = 1;</code>
       */
      public long getTimeMs() {
        return timeMs_;
      }
      /**
       * <code>optional int64 time_ms = 1;</code>
       */
      public Builder setTimeMs(long value) {
        bitField0_ |= 0x00000001;
        timeMs_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int64 time_ms = 1;</code>
       */
      public Builder clearTimeMs() {
        bitField0_ = (bitField0_ & ~0x00000001);
        timeMs_ = 0L;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:alluxio.proto.journal.LicenseCheckEntry)
    }

    static {
      defaultInstance = new LicenseCheckEntry(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:alluxio.proto.journal.LicenseCheckEntry)
  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_proto_journal_LicenseCheckEntry_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_alluxio_proto_journal_LicenseCheckEntry_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\025journal/license.proto\022\025alluxio.proto.j" +
      "ournal\"$\n\021LicenseCheckEntry\022\017\n\007time_ms\030\001" +
      " \001(\003"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
        new com.google.protobuf.Descriptors.FileDescriptor.    InternalDescriptorAssigner() {
          public com.google.protobuf.ExtensionRegistry assignDescriptors(
              com.google.protobuf.Descriptors.FileDescriptor root) {
            descriptor = root;
            return null;
          }
        };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
    internal_static_alluxio_proto_journal_LicenseCheckEntry_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_alluxio_proto_journal_LicenseCheckEntry_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessage.FieldAccessorTable(
        internal_static_alluxio_proto_journal_LicenseCheckEntry_descriptor,
        new java.lang.String[] { "TimeMs", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
