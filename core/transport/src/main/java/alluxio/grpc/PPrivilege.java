// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/privilege_master.proto

package alluxio.grpc;

/**
 * Protobuf enum {@code alluxio.grpc.privilege.PPrivilege}
 */
public enum PPrivilege
    implements com.google.protobuf.ProtocolMessageEnum {
  /**
   * <code>FREE = 1;</code>
   */
  FREE(1),
  /**
   * <code>PIN = 2;</code>
   */
  PIN(2),
  /**
   * <code>REPLICATION = 3;</code>
   */
  REPLICATION(3),
  /**
   * <code>TTL = 4;</code>
   */
  TTL(4),
  ;

  /**
   * <code>FREE = 1;</code>
   */
  public static final int FREE_VALUE = 1;
  /**
   * <code>PIN = 2;</code>
   */
  public static final int PIN_VALUE = 2;
  /**
   * <code>REPLICATION = 3;</code>
   */
  public static final int REPLICATION_VALUE = 3;
  /**
   * <code>TTL = 4;</code>
   */
  public static final int TTL_VALUE = 4;


  public final int getNumber() {
    return value;
  }

  /**
   * @deprecated Use {@link #forNumber(int)} instead.
   */
  @java.lang.Deprecated
  public static PPrivilege valueOf(int value) {
    return forNumber(value);
  }

  public static PPrivilege forNumber(int value) {
    switch (value) {
      case 1: return FREE;
      case 2: return PIN;
      case 3: return REPLICATION;
      case 4: return TTL;
      default: return null;
    }
  }

  public static com.google.protobuf.Internal.EnumLiteMap<PPrivilege>
      internalGetValueMap() {
    return internalValueMap;
  }
  private static final com.google.protobuf.Internal.EnumLiteMap<
      PPrivilege> internalValueMap =
        new com.google.protobuf.Internal.EnumLiteMap<PPrivilege>() {
          public PPrivilege findValueByNumber(int number) {
            return PPrivilege.forNumber(number);
          }
        };

  public final com.google.protobuf.Descriptors.EnumValueDescriptor
      getValueDescriptor() {
    return getDescriptor().getValues().get(ordinal());
  }
  public final com.google.protobuf.Descriptors.EnumDescriptor
      getDescriptorForType() {
    return getDescriptor();
  }
  public static final com.google.protobuf.Descriptors.EnumDescriptor
      getDescriptor() {
    return alluxio.grpc.PrivilegeMasterProto.getDescriptor().getEnumTypes().get(0);
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

  private final int value;

  private PPrivilege(int value) {
    this.value = value;
  }

  // @@protoc_insertion_point(enum_scope:alluxio.grpc.privilege.PPrivilege)
}

