// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/fscommon.proto

package alluxio.grpc;

public final class FsCommonProto {
  private FsCommonProto() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_grpc_fscommon_Capability_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_alluxio_grpc_fscommon_Capability_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\023grpc/fscommon.proto\022\025alluxio.grpc.fsco" +
      "mmon\"C\n\nCapability\022\017\n\007content\030\001 \001(\014\022\025\n\ra" +
      "uthenticator\030\002 \001(\014\022\r\n\005keyId\030\003 \001(\003*1\n\023Loa" +
      "dDescendantPType\022\010\n\004NONE\020\000\022\007\n\003ONE\020\001\022\007\n\003A" +
      "LL\020\002B\037\n\014alluxio.grpcB\rFsCommonProtoP\001"
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
    internal_static_alluxio_grpc_fscommon_Capability_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_alluxio_grpc_fscommon_Capability_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_alluxio_grpc_fscommon_Capability_descriptor,
        new java.lang.String[] { "Content", "Authenticator", "KeyId", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
