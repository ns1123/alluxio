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
 * The Secure RPC master service
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.17.1)",
    comments = "Source: grpc/secure_rpc_server.proto")
public final class SecureRpcMasterServiceGrpc {

  private SecureRpcMasterServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.securerpc.SecureRpcMasterService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.WriteSecretKeyPRequest,
      alluxio.grpc.WriteSecretKeyPResponse> getWriteSecretKeyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "WriteSecretKey",
      requestType = alluxio.grpc.WriteSecretKeyPRequest.class,
      responseType = alluxio.grpc.WriteSecretKeyPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.WriteSecretKeyPRequest,
      alluxio.grpc.WriteSecretKeyPResponse> getWriteSecretKeyMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.WriteSecretKeyPRequest, alluxio.grpc.WriteSecretKeyPResponse> getWriteSecretKeyMethod;
    if ((getWriteSecretKeyMethod = SecureRpcMasterServiceGrpc.getWriteSecretKeyMethod) == null) {
      synchronized (SecureRpcMasterServiceGrpc.class) {
        if ((getWriteSecretKeyMethod = SecureRpcMasterServiceGrpc.getWriteSecretKeyMethod) == null) {
          SecureRpcMasterServiceGrpc.getWriteSecretKeyMethod = getWriteSecretKeyMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.WriteSecretKeyPRequest, alluxio.grpc.WriteSecretKeyPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.securerpc.SecureRpcMasterService", "WriteSecretKey"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.WriteSecretKeyPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.WriteSecretKeyPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new SecureRpcMasterServiceMethodDescriptorSupplier("WriteSecretKey"))
                  .build();
          }
        }
     }
     return getWriteSecretKeyMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static SecureRpcMasterServiceStub newStub(io.grpc.Channel channel) {
    return new SecureRpcMasterServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static SecureRpcMasterServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new SecureRpcMasterServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static SecureRpcMasterServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new SecureRpcMasterServiceFutureStub(channel);
  }

  /**
   * <pre>
   * The Secure RPC master service
   * </pre>
   */
  public static abstract class SecureRpcMasterServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Writes a new secret key to server
     * </pre>
     */
    public void writeSecretKey(alluxio.grpc.WriteSecretKeyPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.WriteSecretKeyPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getWriteSecretKeyMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getWriteSecretKeyMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.WriteSecretKeyPRequest,
                alluxio.grpc.WriteSecretKeyPResponse>(
                  this, METHODID_WRITE_SECRET_KEY)))
          .build();
    }
  }

  /**
   * <pre>
   * The Secure RPC master service
   * </pre>
   */
  public static final class SecureRpcMasterServiceStub extends io.grpc.stub.AbstractStub<SecureRpcMasterServiceStub> {
    private SecureRpcMasterServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private SecureRpcMasterServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SecureRpcMasterServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new SecureRpcMasterServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Writes a new secret key to server
     * </pre>
     */
    public void writeSecretKey(alluxio.grpc.WriteSecretKeyPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.WriteSecretKeyPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getWriteSecretKeyMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * The Secure RPC master service
   * </pre>
   */
  public static final class SecureRpcMasterServiceBlockingStub extends io.grpc.stub.AbstractStub<SecureRpcMasterServiceBlockingStub> {
    private SecureRpcMasterServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private SecureRpcMasterServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SecureRpcMasterServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new SecureRpcMasterServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Writes a new secret key to server
     * </pre>
     */
    public alluxio.grpc.WriteSecretKeyPResponse writeSecretKey(alluxio.grpc.WriteSecretKeyPRequest request) {
      return blockingUnaryCall(
          getChannel(), getWriteSecretKeyMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * The Secure RPC master service
   * </pre>
   */
  public static final class SecureRpcMasterServiceFutureStub extends io.grpc.stub.AbstractStub<SecureRpcMasterServiceFutureStub> {
    private SecureRpcMasterServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private SecureRpcMasterServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SecureRpcMasterServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new SecureRpcMasterServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Writes a new secret key to server
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.WriteSecretKeyPResponse> writeSecretKey(
        alluxio.grpc.WriteSecretKeyPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getWriteSecretKeyMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_WRITE_SECRET_KEY = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final SecureRpcMasterServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(SecureRpcMasterServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_WRITE_SECRET_KEY:
          serviceImpl.writeSecretKey((alluxio.grpc.WriteSecretKeyPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.WriteSecretKeyPResponse>) responseObserver);
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

  private static abstract class SecureRpcMasterServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    SecureRpcMasterServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.SecureRpcServerProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("SecureRpcMasterService");
    }
  }

  private static final class SecureRpcMasterServiceFileDescriptorSupplier
      extends SecureRpcMasterServiceBaseDescriptorSupplier {
    SecureRpcMasterServiceFileDescriptorSupplier() {}
  }

  private static final class SecureRpcMasterServiceMethodDescriptorSupplier
      extends SecureRpcMasterServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    SecureRpcMasterServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (SecureRpcMasterServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new SecureRpcMasterServiceFileDescriptorSupplier())
              .addMethod(getWriteSecretKeyMethod())
              .build();
        }
      }
    }
    return result;
  }
}
