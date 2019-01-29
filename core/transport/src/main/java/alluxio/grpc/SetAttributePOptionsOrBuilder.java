// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

public interface SetAttributePOptionsOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.file.SetAttributePOptions)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional bool pinned = 1;</code>
   */
  boolean hasPinned();
  /**
   * <code>optional bool pinned = 1;</code>
   */
  boolean getPinned();

  /**
   * <code>optional int64 ttl = 2;</code>
   */
  boolean hasTtl();
  /**
   * <code>optional int64 ttl = 2;</code>
   */
  long getTtl();

  /**
   * <code>optional .alluxio.grpc.TtlAction ttlAction = 3;</code>
   */
  boolean hasTtlAction();
  /**
   * <code>optional .alluxio.grpc.TtlAction ttlAction = 3;</code>
   */
  alluxio.grpc.TtlAction getTtlAction();

  /**
   * <code>optional bool persisted = 4;</code>
   */
  boolean hasPersisted();
  /**
   * <code>optional bool persisted = 4;</code>
   */
  boolean getPersisted();

  /**
   * <code>optional string owner = 5;</code>
   */
  boolean hasOwner();
  /**
   * <code>optional string owner = 5;</code>
   */
  java.lang.String getOwner();
  /**
   * <code>optional string owner = 5;</code>
   */
  com.google.protobuf.ByteString
      getOwnerBytes();

  /**
   * <code>optional string group = 6;</code>
   */
  boolean hasGroup();
  /**
   * <code>optional string group = 6;</code>
   */
  java.lang.String getGroup();
  /**
   * <code>optional string group = 6;</code>
   */
  com.google.protobuf.ByteString
      getGroupBytes();

  /**
   * <code>optional .alluxio.grpc.PMode mode = 7;</code>
   */
  boolean hasMode();
  /**
   * <code>optional .alluxio.grpc.PMode mode = 7;</code>
   */
  alluxio.grpc.PMode getMode();
  /**
   * <code>optional .alluxio.grpc.PMode mode = 7;</code>
   */
  alluxio.grpc.PModeOrBuilder getModeOrBuilder();

  /**
   * <code>optional bool recursive = 8;</code>
   */
  boolean hasRecursive();
  /**
   * <code>optional bool recursive = 8;</code>
   */
  boolean getRecursive();

  /**
   * <code>optional int32 replicationMax = 9;</code>
   */
  boolean hasReplicationMax();
  /**
   * <code>optional int32 replicationMax = 9;</code>
   */
  int getReplicationMax();

  /**
   * <code>optional int32 replicationMin = 10;</code>
   */
  boolean hasReplicationMin();
  /**
   * <code>optional int32 replicationMin = 10;</code>
   */
  int getReplicationMin();

  /**
   * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 11;</code>
   */
  boolean hasCommonOptions();
  /**
   * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 11;</code>
   */
  alluxio.grpc.FileSystemMasterCommonPOptions getCommonOptions();
  /**
   * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 11;</code>
   */
  alluxio.grpc.FileSystemMasterCommonPOptionsOrBuilder getCommonOptionsOrBuilder();
}
