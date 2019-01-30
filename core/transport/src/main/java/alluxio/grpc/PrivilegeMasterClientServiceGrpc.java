package alluxio.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 * <pre>
 **
 * This interface contains privilege master service endpoints for Alluxio clients.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.17.1)",
    comments = "Source: grpc/privilege_master.proto")
public final class PrivilegeMasterClientServiceGrpc {

  private PrivilegeMasterClientServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.privilege.PrivilegeMasterClientService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetGroupPrivilegesPRequest,
      alluxio.grpc.GetGroupPrivilegesPResponse> getGetGroupPrivilegesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetGroupPrivileges",
      requestType = alluxio.grpc.GetGroupPrivilegesPRequest.class,
      responseType = alluxio.grpc.GetGroupPrivilegesPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetGroupPrivilegesPRequest,
      alluxio.grpc.GetGroupPrivilegesPResponse> getGetGroupPrivilegesMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetGroupPrivilegesPRequest, alluxio.grpc.GetGroupPrivilegesPResponse> getGetGroupPrivilegesMethod;
    if ((getGetGroupPrivilegesMethod = PrivilegeMasterClientServiceGrpc.getGetGroupPrivilegesMethod) == null) {
      synchronized (PrivilegeMasterClientServiceGrpc.class) {
        if ((getGetGroupPrivilegesMethod = PrivilegeMasterClientServiceGrpc.getGetGroupPrivilegesMethod) == null) {
          PrivilegeMasterClientServiceGrpc.getGetGroupPrivilegesMethod = getGetGroupPrivilegesMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetGroupPrivilegesPRequest, alluxio.grpc.GetGroupPrivilegesPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.privilege.PrivilegeMasterClientService", "GetGroupPrivileges"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetGroupPrivilegesPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetGroupPrivilegesPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new PrivilegeMasterClientServiceMethodDescriptorSupplier("GetGroupPrivileges"))
                  .build();
          }
        }
     }
     return getGetGroupPrivilegesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetUserPrivilegesPRequest,
      alluxio.grpc.GetUserPrivilegesPResponse> getGetUserPrivilegesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetUserPrivileges",
      requestType = alluxio.grpc.GetUserPrivilegesPRequest.class,
      responseType = alluxio.grpc.GetUserPrivilegesPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetUserPrivilegesPRequest,
      alluxio.grpc.GetUserPrivilegesPResponse> getGetUserPrivilegesMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetUserPrivilegesPRequest, alluxio.grpc.GetUserPrivilegesPResponse> getGetUserPrivilegesMethod;
    if ((getGetUserPrivilegesMethod = PrivilegeMasterClientServiceGrpc.getGetUserPrivilegesMethod) == null) {
      synchronized (PrivilegeMasterClientServiceGrpc.class) {
        if ((getGetUserPrivilegesMethod = PrivilegeMasterClientServiceGrpc.getGetUserPrivilegesMethod) == null) {
          PrivilegeMasterClientServiceGrpc.getGetUserPrivilegesMethod = getGetUserPrivilegesMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetUserPrivilegesPRequest, alluxio.grpc.GetUserPrivilegesPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.privilege.PrivilegeMasterClientService", "GetUserPrivileges"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetUserPrivilegesPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetUserPrivilegesPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new PrivilegeMasterClientServiceMethodDescriptorSupplier("GetUserPrivileges"))
                  .build();
          }
        }
     }
     return getGetUserPrivilegesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetGroupToPrivilegesMappingPRequest,
      alluxio.grpc.GetGroupToPrivilegesMappingPResponse> getGetGroupToPrivilegesMappingMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetGroupToPrivilegesMapping",
      requestType = alluxio.grpc.GetGroupToPrivilegesMappingPRequest.class,
      responseType = alluxio.grpc.GetGroupToPrivilegesMappingPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetGroupToPrivilegesMappingPRequest,
      alluxio.grpc.GetGroupToPrivilegesMappingPResponse> getGetGroupToPrivilegesMappingMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetGroupToPrivilegesMappingPRequest, alluxio.grpc.GetGroupToPrivilegesMappingPResponse> getGetGroupToPrivilegesMappingMethod;
    if ((getGetGroupToPrivilegesMappingMethod = PrivilegeMasterClientServiceGrpc.getGetGroupToPrivilegesMappingMethod) == null) {
      synchronized (PrivilegeMasterClientServiceGrpc.class) {
        if ((getGetGroupToPrivilegesMappingMethod = PrivilegeMasterClientServiceGrpc.getGetGroupToPrivilegesMappingMethod) == null) {
          PrivilegeMasterClientServiceGrpc.getGetGroupToPrivilegesMappingMethod = getGetGroupToPrivilegesMappingMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetGroupToPrivilegesMappingPRequest, alluxio.grpc.GetGroupToPrivilegesMappingPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.privilege.PrivilegeMasterClientService", "GetGroupToPrivilegesMapping"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetGroupToPrivilegesMappingPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetGroupToPrivilegesMappingPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new PrivilegeMasterClientServiceMethodDescriptorSupplier("GetGroupToPrivilegesMapping"))
                  .build();
          }
        }
     }
     return getGetGroupToPrivilegesMappingMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GrantPrivilegesPRequest,
      alluxio.grpc.GrantPrivilegesPResponse> getGrantPrivilegesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GrantPrivileges",
      requestType = alluxio.grpc.GrantPrivilegesPRequest.class,
      responseType = alluxio.grpc.GrantPrivilegesPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GrantPrivilegesPRequest,
      alluxio.grpc.GrantPrivilegesPResponse> getGrantPrivilegesMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GrantPrivilegesPRequest, alluxio.grpc.GrantPrivilegesPResponse> getGrantPrivilegesMethod;
    if ((getGrantPrivilegesMethod = PrivilegeMasterClientServiceGrpc.getGrantPrivilegesMethod) == null) {
      synchronized (PrivilegeMasterClientServiceGrpc.class) {
        if ((getGrantPrivilegesMethod = PrivilegeMasterClientServiceGrpc.getGrantPrivilegesMethod) == null) {
          PrivilegeMasterClientServiceGrpc.getGrantPrivilegesMethod = getGrantPrivilegesMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GrantPrivilegesPRequest, alluxio.grpc.GrantPrivilegesPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.privilege.PrivilegeMasterClientService", "GrantPrivileges"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GrantPrivilegesPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GrantPrivilegesPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new PrivilegeMasterClientServiceMethodDescriptorSupplier("GrantPrivileges"))
                  .build();
          }
        }
     }
     return getGrantPrivilegesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.RevokePrivilegesPRequest,
      alluxio.grpc.RevokePrivilegesPResponse> getRevokePrivilegesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RevokePrivileges",
      requestType = alluxio.grpc.RevokePrivilegesPRequest.class,
      responseType = alluxio.grpc.RevokePrivilegesPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.RevokePrivilegesPRequest,
      alluxio.grpc.RevokePrivilegesPResponse> getRevokePrivilegesMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.RevokePrivilegesPRequest, alluxio.grpc.RevokePrivilegesPResponse> getRevokePrivilegesMethod;
    if ((getRevokePrivilegesMethod = PrivilegeMasterClientServiceGrpc.getRevokePrivilegesMethod) == null) {
      synchronized (PrivilegeMasterClientServiceGrpc.class) {
        if ((getRevokePrivilegesMethod = PrivilegeMasterClientServiceGrpc.getRevokePrivilegesMethod) == null) {
          PrivilegeMasterClientServiceGrpc.getRevokePrivilegesMethod = getRevokePrivilegesMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.RevokePrivilegesPRequest, alluxio.grpc.RevokePrivilegesPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.privilege.PrivilegeMasterClientService", "RevokePrivileges"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RevokePrivilegesPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RevokePrivilegesPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new PrivilegeMasterClientServiceMethodDescriptorSupplier("RevokePrivileges"))
                  .build();
          }
        }
     }
     return getRevokePrivilegesMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static PrivilegeMasterClientServiceStub newStub(io.grpc.Channel channel) {
    return new PrivilegeMasterClientServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static PrivilegeMasterClientServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new PrivilegeMasterClientServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static PrivilegeMasterClientServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new PrivilegeMasterClientServiceFutureStub(channel);
  }

  /**
   * <pre>
   **
   * This interface contains privilege master service endpoints for Alluxio clients.
   * </pre>
   */
  public static abstract class PrivilegeMasterClientServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     **
     * Returns the privilege information for the given group.
     * </pre>
     */
    public void getGroupPrivileges(alluxio.grpc.GetGroupPrivilegesPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetGroupPrivilegesPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetGroupPrivilegesMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the privilege information for the given user.
     * </pre>
     */
    public void getUserPrivileges(alluxio.grpc.GetUserPrivilegesPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetUserPrivilegesPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetUserPrivilegesMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the mapping from groups to privileges.
     * </pre>
     */
    public void getGroupToPrivilegesMapping(alluxio.grpc.GetGroupToPrivilegesMappingPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetGroupToPrivilegesMappingPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetGroupToPrivilegesMappingMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Grants the given privileges to the given group, returning the updated privileges for the group.
     * </pre>
     */
    public void grantPrivileges(alluxio.grpc.GrantPrivilegesPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GrantPrivilegesPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGrantPrivilegesMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Removes the given privileges from the given group, returning the updated privileges for the group.
     * </pre>
     */
    public void revokePrivileges(alluxio.grpc.RevokePrivilegesPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RevokePrivilegesPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRevokePrivilegesMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetGroupPrivilegesMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetGroupPrivilegesPRequest,
                alluxio.grpc.GetGroupPrivilegesPResponse>(
                  this, METHODID_GET_GROUP_PRIVILEGES)))
          .addMethod(
            getGetUserPrivilegesMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetUserPrivilegesPRequest,
                alluxio.grpc.GetUserPrivilegesPResponse>(
                  this, METHODID_GET_USER_PRIVILEGES)))
          .addMethod(
            getGetGroupToPrivilegesMappingMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetGroupToPrivilegesMappingPRequest,
                alluxio.grpc.GetGroupToPrivilegesMappingPResponse>(
                  this, METHODID_GET_GROUP_TO_PRIVILEGES_MAPPING)))
          .addMethod(
            getGrantPrivilegesMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GrantPrivilegesPRequest,
                alluxio.grpc.GrantPrivilegesPResponse>(
                  this, METHODID_GRANT_PRIVILEGES)))
          .addMethod(
            getRevokePrivilegesMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.RevokePrivilegesPRequest,
                alluxio.grpc.RevokePrivilegesPResponse>(
                  this, METHODID_REVOKE_PRIVILEGES)))
          .build();
    }
  }

  /**
   * <pre>
   **
   * This interface contains privilege master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class PrivilegeMasterClientServiceStub extends io.grpc.stub.AbstractStub<PrivilegeMasterClientServiceStub> {
    private PrivilegeMasterClientServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private PrivilegeMasterClientServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PrivilegeMasterClientServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new PrivilegeMasterClientServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns the privilege information for the given group.
     * </pre>
     */
    public void getGroupPrivileges(alluxio.grpc.GetGroupPrivilegesPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetGroupPrivilegesPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetGroupPrivilegesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the privilege information for the given user.
     * </pre>
     */
    public void getUserPrivileges(alluxio.grpc.GetUserPrivilegesPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetUserPrivilegesPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetUserPrivilegesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the mapping from groups to privileges.
     * </pre>
     */
    public void getGroupToPrivilegesMapping(alluxio.grpc.GetGroupToPrivilegesMappingPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetGroupToPrivilegesMappingPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetGroupToPrivilegesMappingMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Grants the given privileges to the given group, returning the updated privileges for the group.
     * </pre>
     */
    public void grantPrivileges(alluxio.grpc.GrantPrivilegesPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GrantPrivilegesPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGrantPrivilegesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Removes the given privileges from the given group, returning the updated privileges for the group.
     * </pre>
     */
    public void revokePrivileges(alluxio.grpc.RevokePrivilegesPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RevokePrivilegesPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRevokePrivilegesMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains privilege master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class PrivilegeMasterClientServiceBlockingStub extends io.grpc.stub.AbstractStub<PrivilegeMasterClientServiceBlockingStub> {
    private PrivilegeMasterClientServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private PrivilegeMasterClientServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PrivilegeMasterClientServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new PrivilegeMasterClientServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns the privilege information for the given group.
     * </pre>
     */
    public alluxio.grpc.GetGroupPrivilegesPResponse getGroupPrivileges(alluxio.grpc.GetGroupPrivilegesPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetGroupPrivilegesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns the privilege information for the given user.
     * </pre>
     */
    public alluxio.grpc.GetUserPrivilegesPResponse getUserPrivileges(alluxio.grpc.GetUserPrivilegesPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetUserPrivilegesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns the mapping from groups to privileges.
     * </pre>
     */
    public alluxio.grpc.GetGroupToPrivilegesMappingPResponse getGroupToPrivilegesMapping(alluxio.grpc.GetGroupToPrivilegesMappingPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetGroupToPrivilegesMappingMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Grants the given privileges to the given group, returning the updated privileges for the group.
     * </pre>
     */
    public alluxio.grpc.GrantPrivilegesPResponse grantPrivileges(alluxio.grpc.GrantPrivilegesPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGrantPrivilegesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Removes the given privileges from the given group, returning the updated privileges for the group.
     * </pre>
     */
    public alluxio.grpc.RevokePrivilegesPResponse revokePrivileges(alluxio.grpc.RevokePrivilegesPRequest request) {
      return blockingUnaryCall(
          getChannel(), getRevokePrivilegesMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains privilege master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class PrivilegeMasterClientServiceFutureStub extends io.grpc.stub.AbstractStub<PrivilegeMasterClientServiceFutureStub> {
    private PrivilegeMasterClientServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private PrivilegeMasterClientServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PrivilegeMasterClientServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new PrivilegeMasterClientServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns the privilege information for the given group.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetGroupPrivilegesPResponse> getGroupPrivileges(
        alluxio.grpc.GetGroupPrivilegesPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetGroupPrivilegesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns the privilege information for the given user.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetUserPrivilegesPResponse> getUserPrivileges(
        alluxio.grpc.GetUserPrivilegesPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetUserPrivilegesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns the mapping from groups to privileges.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetGroupToPrivilegesMappingPResponse> getGroupToPrivilegesMapping(
        alluxio.grpc.GetGroupToPrivilegesMappingPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetGroupToPrivilegesMappingMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Grants the given privileges to the given group, returning the updated privileges for the group.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GrantPrivilegesPResponse> grantPrivileges(
        alluxio.grpc.GrantPrivilegesPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGrantPrivilegesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Removes the given privileges from the given group, returning the updated privileges for the group.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.RevokePrivilegesPResponse> revokePrivileges(
        alluxio.grpc.RevokePrivilegesPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRevokePrivilegesMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_GROUP_PRIVILEGES = 0;
  private static final int METHODID_GET_USER_PRIVILEGES = 1;
  private static final int METHODID_GET_GROUP_TO_PRIVILEGES_MAPPING = 2;
  private static final int METHODID_GRANT_PRIVILEGES = 3;
  private static final int METHODID_REVOKE_PRIVILEGES = 4;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final PrivilegeMasterClientServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(PrivilegeMasterClientServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_GROUP_PRIVILEGES:
          serviceImpl.getGroupPrivileges((alluxio.grpc.GetGroupPrivilegesPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetGroupPrivilegesPResponse>) responseObserver);
          break;
        case METHODID_GET_USER_PRIVILEGES:
          serviceImpl.getUserPrivileges((alluxio.grpc.GetUserPrivilegesPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetUserPrivilegesPResponse>) responseObserver);
          break;
        case METHODID_GET_GROUP_TO_PRIVILEGES_MAPPING:
          serviceImpl.getGroupToPrivilegesMapping((alluxio.grpc.GetGroupToPrivilegesMappingPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetGroupToPrivilegesMappingPResponse>) responseObserver);
          break;
        case METHODID_GRANT_PRIVILEGES:
          serviceImpl.grantPrivileges((alluxio.grpc.GrantPrivilegesPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GrantPrivilegesPResponse>) responseObserver);
          break;
        case METHODID_REVOKE_PRIVILEGES:
          serviceImpl.revokePrivileges((alluxio.grpc.RevokePrivilegesPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.RevokePrivilegesPResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class PrivilegeMasterClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    PrivilegeMasterClientServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.PrivilegeMasterProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("PrivilegeMasterClientService");
    }
  }

  private static final class PrivilegeMasterClientServiceFileDescriptorSupplier
      extends PrivilegeMasterClientServiceBaseDescriptorSupplier {
    PrivilegeMasterClientServiceFileDescriptorSupplier() {}
  }

  private static final class PrivilegeMasterClientServiceMethodDescriptorSupplier
      extends PrivilegeMasterClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    PrivilegeMasterClientServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (PrivilegeMasterClientServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new PrivilegeMasterClientServiceFileDescriptorSupplier())
              .addMethod(getGetGroupPrivilegesMethod())
              .addMethod(getGetUserPrivilegesMethod())
              .addMethod(getGetGroupToPrivilegesMappingMethod())
              .addMethod(getGrantPrivilegesMethod())
              .addMethod(getRevokePrivilegesMethod())
              .build();
        }
      }
    }
    return result;
  }
}
