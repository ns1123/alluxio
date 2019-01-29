// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

public interface DelegationTokenIdentifierOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.file.DelegationTokenIdentifier)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional string owner = 1;</code>
   */
  boolean hasOwner();
  /**
   * <code>optional string owner = 1;</code>
   */
  java.lang.String getOwner();
  /**
   * <code>optional string owner = 1;</code>
   */
  com.google.protobuf.ByteString
      getOwnerBytes();

  /**
   * <code>optional string renewer = 2;</code>
   */
  boolean hasRenewer();
  /**
   * <code>optional string renewer = 2;</code>
   */
  java.lang.String getRenewer();
  /**
   * <code>optional string renewer = 2;</code>
   */
  com.google.protobuf.ByteString
      getRenewerBytes();

  /**
   * <code>optional string realUser = 3;</code>
   */
  boolean hasRealUser();
  /**
   * <code>optional string realUser = 3;</code>
   */
  java.lang.String getRealUser();
  /**
   * <code>optional string realUser = 3;</code>
   */
  com.google.protobuf.ByteString
      getRealUserBytes();

  /**
   * <code>optional int64 issueDate = 4;</code>
   */
  boolean hasIssueDate();
  /**
   * <code>optional int64 issueDate = 4;</code>
   */
  long getIssueDate();

  /**
   * <code>optional int64 maxDate = 5;</code>
   */
  boolean hasMaxDate();
  /**
   * <code>optional int64 maxDate = 5;</code>
   */
  long getMaxDate();

  /**
   * <code>optional int64 sequenceNumber = 6;</code>
   */
  boolean hasSequenceNumber();
  /**
   * <code>optional int64 sequenceNumber = 6;</code>
   */
  long getSequenceNumber();

  /**
   * <code>optional int64 masterKeyId = 7;</code>
   */
  boolean hasMasterKeyId();
  /**
   * <code>optional int64 masterKeyId = 7;</code>
   */
  long getMasterKeyId();
}
