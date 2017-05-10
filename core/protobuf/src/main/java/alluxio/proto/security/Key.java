// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: key.proto

package alluxio.proto.security;

public final class Key {
  private Key() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  /**
   * Protobuf enum {@code alluxio.proto.security.KeyType}
   */
  public enum KeyType
      implements com.google.protobuf.ProtocolMessageEnum {
    /**
     * <code>CAPABILITY = 0;</code>
     */
    CAPABILITY(0, 0),
    ;

    /**
     * <code>CAPABILITY = 0;</code>
     */
    public static final int CAPABILITY_VALUE = 0;


    public final int getNumber() { return value; }

    public static KeyType valueOf(int value) {
      switch (value) {
        case 0: return CAPABILITY;
        default: return null;
      }
    }

    public static com.google.protobuf.Internal.EnumLiteMap<KeyType>
        internalGetValueMap() {
      return internalValueMap;
    }
    private static com.google.protobuf.Internal.EnumLiteMap<KeyType>
        internalValueMap =
          new com.google.protobuf.Internal.EnumLiteMap<KeyType>() {
            public KeyType findValueByNumber(int number) {
              return KeyType.valueOf(number);
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
      return alluxio.proto.security.Key.getDescriptor().getEnumTypes().get(0);
    }

    private static final KeyType[] VALUES = values();

    public static KeyType valueOf(
        com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
      if (desc.getType() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "EnumValueDescriptor is not for this type.");
      }
      return VALUES[desc.getIndex()];
    }

    private final int index;
    private final int value;

    private KeyType(int index, int value) {
      this.index = index;
      this.value = value;
    }

    // @@protoc_insertion_point(enum_scope:alluxio.proto.security.KeyType)
  }

  public interface SecretKeyOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // optional .alluxio.proto.security.KeyType key_type = 1;
    /**
     * <code>optional .alluxio.proto.security.KeyType key_type = 1;</code>
     */
    boolean hasKeyType();
    /**
     * <code>optional .alluxio.proto.security.KeyType key_type = 1;</code>
     */
    alluxio.proto.security.Key.KeyType getKeyType();

    // optional int64 key_id = 2;
    /**
     * <code>optional int64 key_id = 2;</code>
     */
    boolean hasKeyId();
    /**
     * <code>optional int64 key_id = 2;</code>
     */
    long getKeyId();

    // optional int64 expiration_time_ms = 3;
    /**
     * <code>optional int64 expiration_time_ms = 3;</code>
     */
    boolean hasExpirationTimeMs();
    /**
     * <code>optional int64 expiration_time_ms = 3;</code>
     */
    long getExpirationTimeMs();

    // optional bytes secret_key = 4;
    /**
     * <code>optional bytes secret_key = 4;</code>
     */
    boolean hasSecretKey();
    /**
     * <code>optional bytes secret_key = 4;</code>
     */
    com.google.protobuf.ByteString getSecretKey();
  }
  /**
   * Protobuf type {@code alluxio.proto.security.SecretKey}
   */
  public static final class SecretKey extends
      com.google.protobuf.GeneratedMessage
      implements SecretKeyOrBuilder {
    // Use SecretKey.newBuilder() to construct.
    private SecretKey(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private SecretKey(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final SecretKey defaultInstance;
    public static SecretKey getDefaultInstance() {
      return defaultInstance;
    }

    public SecretKey getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private SecretKey(
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
              int rawValue = input.readEnum();
              alluxio.proto.security.Key.KeyType value = alluxio.proto.security.Key.KeyType.valueOf(rawValue);
              if (value == null) {
                unknownFields.mergeVarintField(1, rawValue);
              } else {
                bitField0_ |= 0x00000001;
                keyType_ = value;
              }
              break;
            }
            case 16: {
              bitField0_ |= 0x00000002;
              keyId_ = input.readInt64();
              break;
            }
            case 24: {
              bitField0_ |= 0x00000004;
              expirationTimeMs_ = input.readInt64();
              break;
            }
            case 34: {
              bitField0_ |= 0x00000008;
              secretKey_ = input.readBytes();
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
      return alluxio.proto.security.Key.internal_static_alluxio_proto_security_SecretKey_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.proto.security.Key.internal_static_alluxio_proto_security_SecretKey_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.proto.security.Key.SecretKey.class, alluxio.proto.security.Key.SecretKey.Builder.class);
    }

    public static com.google.protobuf.Parser<SecretKey> PARSER =
        new com.google.protobuf.AbstractParser<SecretKey>() {
      public SecretKey parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new SecretKey(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<SecretKey> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // optional .alluxio.proto.security.KeyType key_type = 1;
    public static final int KEY_TYPE_FIELD_NUMBER = 1;
    private alluxio.proto.security.Key.KeyType keyType_;
    /**
     * <code>optional .alluxio.proto.security.KeyType key_type = 1;</code>
     */
    public boolean hasKeyType() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional .alluxio.proto.security.KeyType key_type = 1;</code>
     */
    public alluxio.proto.security.Key.KeyType getKeyType() {
      return keyType_;
    }

    // optional int64 key_id = 2;
    public static final int KEY_ID_FIELD_NUMBER = 2;
    private long keyId_;
    /**
     * <code>optional int64 key_id = 2;</code>
     */
    public boolean hasKeyId() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional int64 key_id = 2;</code>
     */
    public long getKeyId() {
      return keyId_;
    }

    // optional int64 expiration_time_ms = 3;
    public static final int EXPIRATION_TIME_MS_FIELD_NUMBER = 3;
    private long expirationTimeMs_;
    /**
     * <code>optional int64 expiration_time_ms = 3;</code>
     */
    public boolean hasExpirationTimeMs() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional int64 expiration_time_ms = 3;</code>
     */
    public long getExpirationTimeMs() {
      return expirationTimeMs_;
    }

    // optional bytes secret_key = 4;
    public static final int SECRET_KEY_FIELD_NUMBER = 4;
    private com.google.protobuf.ByteString secretKey_;
    /**
     * <code>optional bytes secret_key = 4;</code>
     */
    public boolean hasSecretKey() {
      return ((bitField0_ & 0x00000008) == 0x00000008);
    }
    /**
     * <code>optional bytes secret_key = 4;</code>
     */
    public com.google.protobuf.ByteString getSecretKey() {
      return secretKey_;
    }

    private void initFields() {
      keyType_ = alluxio.proto.security.Key.KeyType.CAPABILITY;
      keyId_ = 0L;
      expirationTimeMs_ = 0L;
      secretKey_ = com.google.protobuf.ByteString.EMPTY;
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
        output.writeEnum(1, keyType_.getNumber());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeInt64(2, keyId_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        output.writeInt64(3, expirationTimeMs_);
      }
      if (((bitField0_ & 0x00000008) == 0x00000008)) {
        output.writeBytes(4, secretKey_);
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
          .computeEnumSize(1, keyType_.getNumber());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(2, keyId_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(3, expirationTimeMs_);
      }
      if (((bitField0_ & 0x00000008) == 0x00000008)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(4, secretKey_);
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

    public static alluxio.proto.security.Key.SecretKey parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static alluxio.proto.security.Key.SecretKey parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static alluxio.proto.security.Key.SecretKey parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static alluxio.proto.security.Key.SecretKey parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static alluxio.proto.security.Key.SecretKey parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static alluxio.proto.security.Key.SecretKey parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static alluxio.proto.security.Key.SecretKey parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static alluxio.proto.security.Key.SecretKey parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static alluxio.proto.security.Key.SecretKey parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static alluxio.proto.security.Key.SecretKey parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(alluxio.proto.security.Key.SecretKey prototype) {
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
     * Protobuf type {@code alluxio.proto.security.SecretKey}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements alluxio.proto.security.Key.SecretKeyOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return alluxio.proto.security.Key.internal_static_alluxio_proto_security_SecretKey_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return alluxio.proto.security.Key.internal_static_alluxio_proto_security_SecretKey_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                alluxio.proto.security.Key.SecretKey.class, alluxio.proto.security.Key.SecretKey.Builder.class);
      }

      // Construct using alluxio.proto.security.Key.SecretKey.newBuilder()
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
        keyType_ = alluxio.proto.security.Key.KeyType.CAPABILITY;
        bitField0_ = (bitField0_ & ~0x00000001);
        keyId_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000002);
        expirationTimeMs_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000004);
        secretKey_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000008);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return alluxio.proto.security.Key.internal_static_alluxio_proto_security_SecretKey_descriptor;
      }

      public alluxio.proto.security.Key.SecretKey getDefaultInstanceForType() {
        return alluxio.proto.security.Key.SecretKey.getDefaultInstance();
      }

      public alluxio.proto.security.Key.SecretKey build() {
        alluxio.proto.security.Key.SecretKey result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public alluxio.proto.security.Key.SecretKey buildPartial() {
        alluxio.proto.security.Key.SecretKey result = new alluxio.proto.security.Key.SecretKey(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.keyType_ = keyType_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.keyId_ = keyId_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        result.expirationTimeMs_ = expirationTimeMs_;
        if (((from_bitField0_ & 0x00000008) == 0x00000008)) {
          to_bitField0_ |= 0x00000008;
        }
        result.secretKey_ = secretKey_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof alluxio.proto.security.Key.SecretKey) {
          return mergeFrom((alluxio.proto.security.Key.SecretKey)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(alluxio.proto.security.Key.SecretKey other) {
        if (other == alluxio.proto.security.Key.SecretKey.getDefaultInstance()) return this;
        if (other.hasKeyType()) {
          setKeyType(other.getKeyType());
        }
        if (other.hasKeyId()) {
          setKeyId(other.getKeyId());
        }
        if (other.hasExpirationTimeMs()) {
          setExpirationTimeMs(other.getExpirationTimeMs());
        }
        if (other.hasSecretKey()) {
          setSecretKey(other.getSecretKey());
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
        alluxio.proto.security.Key.SecretKey parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (alluxio.proto.security.Key.SecretKey) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // optional .alluxio.proto.security.KeyType key_type = 1;
      private alluxio.proto.security.Key.KeyType keyType_ = alluxio.proto.security.Key.KeyType.CAPABILITY;
      /**
       * <code>optional .alluxio.proto.security.KeyType key_type = 1;</code>
       */
      public boolean hasKeyType() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>optional .alluxio.proto.security.KeyType key_type = 1;</code>
       */
      public alluxio.proto.security.Key.KeyType getKeyType() {
        return keyType_;
      }
      /**
       * <code>optional .alluxio.proto.security.KeyType key_type = 1;</code>
       */
      public Builder setKeyType(alluxio.proto.security.Key.KeyType value) {
        if (value == null) {
          throw new NullPointerException();
        }
        bitField0_ |= 0x00000001;
        keyType_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional .alluxio.proto.security.KeyType key_type = 1;</code>
       */
      public Builder clearKeyType() {
        bitField0_ = (bitField0_ & ~0x00000001);
        keyType_ = alluxio.proto.security.Key.KeyType.CAPABILITY;
        onChanged();
        return this;
      }

      // optional int64 key_id = 2;
      private long keyId_ ;
      /**
       * <code>optional int64 key_id = 2;</code>
       */
      public boolean hasKeyId() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>optional int64 key_id = 2;</code>
       */
      public long getKeyId() {
        return keyId_;
      }
      /**
       * <code>optional int64 key_id = 2;</code>
       */
      public Builder setKeyId(long value) {
        bitField0_ |= 0x00000002;
        keyId_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int64 key_id = 2;</code>
       */
      public Builder clearKeyId() {
        bitField0_ = (bitField0_ & ~0x00000002);
        keyId_ = 0L;
        onChanged();
        return this;
      }

      // optional int64 expiration_time_ms = 3;
      private long expirationTimeMs_ ;
      /**
       * <code>optional int64 expiration_time_ms = 3;</code>
       */
      public boolean hasExpirationTimeMs() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <code>optional int64 expiration_time_ms = 3;</code>
       */
      public long getExpirationTimeMs() {
        return expirationTimeMs_;
      }
      /**
       * <code>optional int64 expiration_time_ms = 3;</code>
       */
      public Builder setExpirationTimeMs(long value) {
        bitField0_ |= 0x00000004;
        expirationTimeMs_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int64 expiration_time_ms = 3;</code>
       */
      public Builder clearExpirationTimeMs() {
        bitField0_ = (bitField0_ & ~0x00000004);
        expirationTimeMs_ = 0L;
        onChanged();
        return this;
      }

      // optional bytes secret_key = 4;
      private com.google.protobuf.ByteString secretKey_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>optional bytes secret_key = 4;</code>
       */
      public boolean hasSecretKey() {
        return ((bitField0_ & 0x00000008) == 0x00000008);
      }
      /**
       * <code>optional bytes secret_key = 4;</code>
       */
      public com.google.protobuf.ByteString getSecretKey() {
        return secretKey_;
      }
      /**
       * <code>optional bytes secret_key = 4;</code>
       */
      public Builder setSecretKey(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000008;
        secretKey_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bytes secret_key = 4;</code>
       */
      public Builder clearSecretKey() {
        bitField0_ = (bitField0_ & ~0x00000008);
        secretKey_ = getDefaultInstance().getSecretKey();
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:alluxio.proto.security.SecretKey)
    }

    static {
      defaultInstance = new SecretKey(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:alluxio.proto.security.SecretKey)
  }

  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_proto_security_SecretKey_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_alluxio_proto_security_SecretKey_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\tkey.proto\022\026alluxio.proto.security\"~\n\tS" +
      "ecretKey\0221\n\010key_type\030\001 \001(\0162\037.alluxio.pro" +
      "to.security.KeyType\022\016\n\006key_id\030\002 \001(\003\022\032\n\022e" +
      "xpiration_time_ms\030\003 \001(\003\022\022\n\nsecret_key\030\004 " +
      "\001(\014*\031\n\007KeyType\022\016\n\nCAPABILITY\020\000"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_alluxio_proto_security_SecretKey_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_alluxio_proto_security_SecretKey_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_alluxio_proto_security_SecretKey_descriptor,
              new java.lang.String[] { "KeyType", "KeyId", "ExpirationTimeMs", "SecretKey", });
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
