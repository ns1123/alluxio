// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

public interface CancelDelegationTokenPRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.file.CancelDelegationTokenPRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional .alluxio.grpc.file.DelegationToken token = 1;</code>
   */
  boolean hasToken();
  /**
   * <code>optional .alluxio.grpc.file.DelegationToken token = 1;</code>
   */
  alluxio.grpc.DelegationToken getToken();
  /**
   * <code>optional .alluxio.grpc.file.DelegationToken token = 1;</code>
   */
  alluxio.grpc.DelegationTokenOrBuilder getTokenOrBuilder();
}
