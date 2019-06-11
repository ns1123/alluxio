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
 * This interface contains policy master service endpoints for Alluxio clients.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.17.1)",
    comments = "Source: grpc/policy_master.proto")
public final class PolicyMasterClientServiceGrpc {

  private PolicyMasterClientServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.policy.PolicyMasterClientService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.ListPolicyPRequest,
      alluxio.grpc.ListPolicyPResponse> getListPolicyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListPolicy",
      requestType = alluxio.grpc.ListPolicyPRequest.class,
      responseType = alluxio.grpc.ListPolicyPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.ListPolicyPRequest,
      alluxio.grpc.ListPolicyPResponse> getListPolicyMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.ListPolicyPRequest, alluxio.grpc.ListPolicyPResponse> getListPolicyMethod;
    if ((getListPolicyMethod = PolicyMasterClientServiceGrpc.getListPolicyMethod) == null) {
      synchronized (PolicyMasterClientServiceGrpc.class) {
        if ((getListPolicyMethod = PolicyMasterClientServiceGrpc.getListPolicyMethod) == null) {
          PolicyMasterClientServiceGrpc.getListPolicyMethod = getListPolicyMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.ListPolicyPRequest, alluxio.grpc.ListPolicyPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.policy.PolicyMasterClientService", "ListPolicy"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.ListPolicyPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.ListPolicyPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new PolicyMasterClientServiceMethodDescriptorSupplier("ListPolicy"))
                  .build();
          }
        }
     }
     return getListPolicyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.AddPolicyPRequest,
      alluxio.grpc.AddPolicyPResponse> getAddPolicyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AddPolicy",
      requestType = alluxio.grpc.AddPolicyPRequest.class,
      responseType = alluxio.grpc.AddPolicyPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.AddPolicyPRequest,
      alluxio.grpc.AddPolicyPResponse> getAddPolicyMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.AddPolicyPRequest, alluxio.grpc.AddPolicyPResponse> getAddPolicyMethod;
    if ((getAddPolicyMethod = PolicyMasterClientServiceGrpc.getAddPolicyMethod) == null) {
      synchronized (PolicyMasterClientServiceGrpc.class) {
        if ((getAddPolicyMethod = PolicyMasterClientServiceGrpc.getAddPolicyMethod) == null) {
          PolicyMasterClientServiceGrpc.getAddPolicyMethod = getAddPolicyMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.AddPolicyPRequest, alluxio.grpc.AddPolicyPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.policy.PolicyMasterClientService", "AddPolicy"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.AddPolicyPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.AddPolicyPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new PolicyMasterClientServiceMethodDescriptorSupplier("AddPolicy"))
                  .build();
          }
        }
     }
     return getAddPolicyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.RemovePolicyPRequest,
      alluxio.grpc.RemovePolicyPResponse> getRemovePolicyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RemovePolicy",
      requestType = alluxio.grpc.RemovePolicyPRequest.class,
      responseType = alluxio.grpc.RemovePolicyPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.RemovePolicyPRequest,
      alluxio.grpc.RemovePolicyPResponse> getRemovePolicyMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.RemovePolicyPRequest, alluxio.grpc.RemovePolicyPResponse> getRemovePolicyMethod;
    if ((getRemovePolicyMethod = PolicyMasterClientServiceGrpc.getRemovePolicyMethod) == null) {
      synchronized (PolicyMasterClientServiceGrpc.class) {
        if ((getRemovePolicyMethod = PolicyMasterClientServiceGrpc.getRemovePolicyMethod) == null) {
          PolicyMasterClientServiceGrpc.getRemovePolicyMethod = getRemovePolicyMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.RemovePolicyPRequest, alluxio.grpc.RemovePolicyPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.policy.PolicyMasterClientService", "RemovePolicy"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RemovePolicyPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RemovePolicyPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new PolicyMasterClientServiceMethodDescriptorSupplier("RemovePolicy"))
                  .build();
          }
        }
     }
     return getRemovePolicyMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static PolicyMasterClientServiceStub newStub(io.grpc.Channel channel) {
    return new PolicyMasterClientServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static PolicyMasterClientServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new PolicyMasterClientServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static PolicyMasterClientServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new PolicyMasterClientServiceFutureStub(channel);
  }

  /**
   * <pre>
   **
   * This interface contains policy master service endpoints for Alluxio clients.
   * </pre>
   */
  public static abstract class PolicyMasterClientServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     **
     * Returns the list of policies
     * </pre>
     */
    public void listPolicy(alluxio.grpc.ListPolicyPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.ListPolicyPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getListPolicyMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Adds a new policy definition
     * </pre>
     */
    public void addPolicy(alluxio.grpc.AddPolicyPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.AddPolicyPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getAddPolicyMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Removes a policy definition
     * </pre>
     */
    public void removePolicy(alluxio.grpc.RemovePolicyPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RemovePolicyPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRemovePolicyMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getListPolicyMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.ListPolicyPRequest,
                alluxio.grpc.ListPolicyPResponse>(
                  this, METHODID_LIST_POLICY)))
          .addMethod(
            getAddPolicyMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.AddPolicyPRequest,
                alluxio.grpc.AddPolicyPResponse>(
                  this, METHODID_ADD_POLICY)))
          .addMethod(
            getRemovePolicyMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.RemovePolicyPRequest,
                alluxio.grpc.RemovePolicyPResponse>(
                  this, METHODID_REMOVE_POLICY)))
          .build();
    }
  }

  /**
   * <pre>
   **
   * This interface contains policy master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class PolicyMasterClientServiceStub extends io.grpc.stub.AbstractStub<PolicyMasterClientServiceStub> {
    private PolicyMasterClientServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private PolicyMasterClientServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PolicyMasterClientServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new PolicyMasterClientServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns the list of policies
     * </pre>
     */
    public void listPolicy(alluxio.grpc.ListPolicyPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.ListPolicyPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getListPolicyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Adds a new policy definition
     * </pre>
     */
    public void addPolicy(alluxio.grpc.AddPolicyPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.AddPolicyPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getAddPolicyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Removes a policy definition
     * </pre>
     */
    public void removePolicy(alluxio.grpc.RemovePolicyPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RemovePolicyPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRemovePolicyMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains policy master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class PolicyMasterClientServiceBlockingStub extends io.grpc.stub.AbstractStub<PolicyMasterClientServiceBlockingStub> {
    private PolicyMasterClientServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private PolicyMasterClientServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PolicyMasterClientServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new PolicyMasterClientServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns the list of policies
     * </pre>
     */
    public alluxio.grpc.ListPolicyPResponse listPolicy(alluxio.grpc.ListPolicyPRequest request) {
      return blockingUnaryCall(
          getChannel(), getListPolicyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Adds a new policy definition
     * </pre>
     */
    public alluxio.grpc.AddPolicyPResponse addPolicy(alluxio.grpc.AddPolicyPRequest request) {
      return blockingUnaryCall(
          getChannel(), getAddPolicyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Removes a policy definition
     * </pre>
     */
    public alluxio.grpc.RemovePolicyPResponse removePolicy(alluxio.grpc.RemovePolicyPRequest request) {
      return blockingUnaryCall(
          getChannel(), getRemovePolicyMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains policy master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class PolicyMasterClientServiceFutureStub extends io.grpc.stub.AbstractStub<PolicyMasterClientServiceFutureStub> {
    private PolicyMasterClientServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private PolicyMasterClientServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PolicyMasterClientServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new PolicyMasterClientServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns the list of policies
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.ListPolicyPResponse> listPolicy(
        alluxio.grpc.ListPolicyPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getListPolicyMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Adds a new policy definition
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.AddPolicyPResponse> addPolicy(
        alluxio.grpc.AddPolicyPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getAddPolicyMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Removes a policy definition
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.RemovePolicyPResponse> removePolicy(
        alluxio.grpc.RemovePolicyPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRemovePolicyMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_LIST_POLICY = 0;
  private static final int METHODID_ADD_POLICY = 1;
  private static final int METHODID_REMOVE_POLICY = 2;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final PolicyMasterClientServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(PolicyMasterClientServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_LIST_POLICY:
          serviceImpl.listPolicy((alluxio.grpc.ListPolicyPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.ListPolicyPResponse>) responseObserver);
          break;
        case METHODID_ADD_POLICY:
          serviceImpl.addPolicy((alluxio.grpc.AddPolicyPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.AddPolicyPResponse>) responseObserver);
          break;
        case METHODID_REMOVE_POLICY:
          serviceImpl.removePolicy((alluxio.grpc.RemovePolicyPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.RemovePolicyPResponse>) responseObserver);
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

  private static abstract class PolicyMasterClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    PolicyMasterClientServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.PolicyMasterProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("PolicyMasterClientService");
    }
  }

  private static final class PolicyMasterClientServiceFileDescriptorSupplier
      extends PolicyMasterClientServiceBaseDescriptorSupplier {
    PolicyMasterClientServiceFileDescriptorSupplier() {}
  }

  private static final class PolicyMasterClientServiceMethodDescriptorSupplier
      extends PolicyMasterClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    PolicyMasterClientServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (PolicyMasterClientServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new PolicyMasterClientServiceFileDescriptorSupplier())
              .addMethod(getListPolicyMethod())
              .addMethod(getAddPolicyMethod())
              .addMethod(getRemovePolicyMethod())
              .build();
        }
      }
    }
    return result;
  }
}
