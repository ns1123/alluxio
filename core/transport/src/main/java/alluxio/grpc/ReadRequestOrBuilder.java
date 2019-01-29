// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/block_worker.proto

package alluxio.grpc;

public interface ReadRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.block.ReadRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional int64 block_id = 1;</code>
   */
  boolean hasBlockId();
  /**
   * <code>optional int64 block_id = 1;</code>
   */
  long getBlockId();

  /**
   * <code>optional int64 offset = 2;</code>
   */
  boolean hasOffset();
  /**
   * <code>optional int64 offset = 2;</code>
   */
  long getOffset();

  /**
   * <code>optional int64 length = 3;</code>
   */
  boolean hasLength();
  /**
   * <code>optional int64 length = 3;</code>
   */
  long getLength();

  /**
   * <pre>
   * Whether the block should be promoted before reading
   * </pre>
   *
   * <code>optional bool promote = 4;</code>
   */
  boolean hasPromote();
  /**
   * <pre>
   * Whether the block should be promoted before reading
   * </pre>
   *
   * <code>optional bool promote = 4;</code>
   */
  boolean getPromote();

  /**
   * <code>optional int64 chunk_size = 5;</code>
   */
  boolean hasChunkSize();
  /**
   * <code>optional int64 chunk_size = 5;</code>
   */
  long getChunkSize();

  /**
   * <pre>
   * This is only set for UFS block read.
   * </pre>
   *
   * <code>optional .alluxio.proto.dataserver.OpenUfsBlockOptions open_ufs_block_options = 6;</code>
   */
  boolean hasOpenUfsBlockOptions();
  /**
   * <pre>
   * This is only set for UFS block read.
   * </pre>
   *
   * <code>optional .alluxio.proto.dataserver.OpenUfsBlockOptions open_ufs_block_options = 6;</code>
   */
  alluxio.proto.dataserver.Protocol.OpenUfsBlockOptions getOpenUfsBlockOptions();
  /**
   * <pre>
   * This is only set for UFS block read.
   * </pre>
   *
   * <code>optional .alluxio.proto.dataserver.OpenUfsBlockOptions open_ufs_block_options = 6;</code>
   */
  alluxio.proto.dataserver.Protocol.OpenUfsBlockOptionsOrBuilder getOpenUfsBlockOptionsOrBuilder();

  /**
   * <pre>
   * ALLUXIO CS ADD
   * </pre>
   *
   * <code>optional .alluxio.proto.security.Capability capability = 1000;</code>
   */
  boolean hasCapability();
  /**
   * <pre>
   * ALLUXIO CS ADD
   * </pre>
   *
   * <code>optional .alluxio.proto.security.Capability capability = 1000;</code>
   */
  alluxio.proto.security.CapabilityProto.Capability getCapability();
  /**
   * <pre>
   * ALLUXIO CS ADD
   * </pre>
   *
   * <code>optional .alluxio.proto.security.Capability capability = 1000;</code>
   */
  alluxio.proto.security.CapabilityProto.CapabilityOrBuilder getCapabilityOrBuilder();
}
