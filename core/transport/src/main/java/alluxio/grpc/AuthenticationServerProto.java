// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/sasl_server.proto

package alluxio.grpc;

public final class AuthenticationServerProto {
  private AuthenticationServerProto() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_grpc_sasl_SaslMessage_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_alluxio_grpc_sasl_SaslMessage_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\026grpc/sasl_server.proto\022\021alluxio.grpc.s" +
      "asl\"\205\001\n\013SaslMessage\0227\n\013messageType\030\001 \001(\016" +
      "2\".alluxio.grpc.sasl.SaslMessageType\022\017\n\007" +
      "message\030\002 \001(\014\022\020\n\010clientId\030\003 \001(\t\022\032\n\022authe" +
      "nticationName\030\004 \001(\t*-\n\017SaslMessageType\022\r" +
      "\n\tCHALLENGE\020\000\022\013\n\007SUCCESS\020\0012o\n\031SaslAuthen" +
      "ticationService\022R\n\014authenticate\022\036.alluxi" +
      "o.grpc.sasl.SaslMessage\032\036.alluxio.grpc.s" +
      "asl.SaslMessage(\0010\001B+\n\014alluxio.grpcB\031Aut" +
      "henticationServerProtoP\001"
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
    internal_static_alluxio_grpc_sasl_SaslMessage_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_alluxio_grpc_sasl_SaslMessage_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_alluxio_grpc_sasl_SaslMessage_descriptor,
        new java.lang.String[] { "MessageType", "Message", "ClientId", "AuthenticationName", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
