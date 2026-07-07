/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.mobile.ivy.demo.helloworld;

import org.apache.dubbo.common.stream.StreamObserver;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.PathResolver;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.ServerService;
import org.apache.dubbo.rpc.TriRpcStatus;
import org.apache.dubbo.rpc.model.MethodDescriptor;
import org.apache.dubbo.rpc.model.ServiceDescriptor;
import org.apache.dubbo.rpc.model.StubMethodDescriptor;
import org.apache.dubbo.rpc.model.StubServiceDescriptor;
import org.apache.dubbo.rpc.service.Destroyable;
import org.apache.dubbo.rpc.stub.BiStreamMethodHandler;
import org.apache.dubbo.rpc.stub.ServerStreamMethodHandler;
import org.apache.dubbo.rpc.stub.StubInvocationUtil;
import org.apache.dubbo.rpc.stub.StubInvoker;
import org.apache.dubbo.rpc.stub.StubMethodHandler;
import org.apache.dubbo.rpc.stub.StubSuppliers;
import org.apache.dubbo.rpc.stub.UnaryStubMethodHandler;

import com.google.protobuf.Message;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.concurrent.CompletableFuture;

public final class DubboHelloWorldServiceTriple {

    public static final String SERVICE_NAME = HelloWorldService.SERVICE_NAME;

    private static final StubServiceDescriptor serviceDescriptor = new StubServiceDescriptor(SERVICE_NAME, HelloWorldService.class);

    static {
        org.apache.dubbo.rpc.protocol.tri.service.SchemaDescriptorRegistry.addSchemaDescriptor(SERVICE_NAME, HelloWorld.getDescriptor());
        StubSuppliers.addSupplier(SERVICE_NAME, DubboHelloWorldServiceTriple::newStub);
        StubSuppliers.addSupplier(HelloWorldService.JAVA_SERVICE_NAME,  DubboHelloWorldServiceTriple::newStub);
        StubSuppliers.addDescriptor(SERVICE_NAME, serviceDescriptor);
        StubSuppliers.addDescriptor(HelloWorldService.JAVA_SERVICE_NAME, serviceDescriptor);
    }

    @SuppressWarnings("unchecked")
    public static HelloWorldService newStub(Invoker<?> invoker) {
        return new HelloWorldServiceStub((Invoker<HelloWorldService>)invoker);
    }

    private static final StubMethodDescriptor sayHelloMethod = new StubMethodDescriptor("sayHello",
    cn.mobile.ivy.demo.helloworld.HelloRequest.class, cn.mobile.ivy.demo.helloworld.HelloReply.class, MethodDescriptor.RpcType.UNARY,
    obj -> ((Message) obj).toByteArray(), obj -> ((Message) obj).toByteArray(), cn.mobile.ivy.demo.helloworld.HelloRequest::parseFrom,
    cn.mobile.ivy.demo.helloworld.HelloReply::parseFrom);

    private static final StubMethodDescriptor sayHelloAsyncMethod = new StubMethodDescriptor("sayHello",
    cn.mobile.ivy.demo.helloworld.HelloRequest.class, java.util.concurrent.CompletableFuture.class, MethodDescriptor.RpcType.UNARY,
    obj -> ((Message) obj).toByteArray(), obj -> ((Message) obj).toByteArray(), cn.mobile.ivy.demo.helloworld.HelloRequest::parseFrom,
    cn.mobile.ivy.demo.helloworld.HelloReply::parseFrom);

    private static final StubMethodDescriptor sayHelloProxyAsyncMethod = new StubMethodDescriptor("sayHelloAsync",
    cn.mobile.ivy.demo.helloworld.HelloRequest.class, cn.mobile.ivy.demo.helloworld.HelloReply.class, MethodDescriptor.RpcType.UNARY,
    obj -> ((Message) obj).toByteArray(), obj -> ((Message) obj).toByteArray(), cn.mobile.ivy.demo.helloworld.HelloRequest::parseFrom,
    cn.mobile.ivy.demo.helloworld.HelloReply::parseFrom);

    static{
        serviceDescriptor.addMethod(sayHelloMethod);
        serviceDescriptor.addMethod(sayHelloProxyAsyncMethod);
    }

    public static class HelloWorldServiceStub implements HelloWorldService, Destroyable {
        private final Invoker<HelloWorldService> invoker;

        public HelloWorldServiceStub(Invoker<HelloWorldService> invoker) {
            this.invoker = invoker;
        }

        @Override
        public void $destroy() {
              invoker.destroy();
         }

        @Override
        public cn.mobile.ivy.demo.helloworld.HelloReply sayHello(cn.mobile.ivy.demo.helloworld.HelloRequest request){
            return StubInvocationUtil.unaryCall(invoker, sayHelloMethod, request);
        }

        public CompletableFuture<cn.mobile.ivy.demo.helloworld.HelloReply> sayHelloAsync(cn.mobile.ivy.demo.helloworld.HelloRequest request){
            return StubInvocationUtil.unaryCall(invoker, sayHelloAsyncMethod, request);
        }

        public void sayHello(cn.mobile.ivy.demo.helloworld.HelloRequest request, StreamObserver<cn.mobile.ivy.demo.helloworld.HelloReply> responseObserver){
            StubInvocationUtil.unaryCall(invoker, sayHelloMethod , request, responseObserver);
        }
    }

    public static abstract class HelloWorldServiceImplBase implements HelloWorldService, ServerService<HelloWorldService> {
        private <T, R> BiConsumer<T, StreamObserver<R>> syncToAsync(java.util.function.Function<T, R> syncFun) {
            return new BiConsumer<T, StreamObserver<R>>() {
                @Override
                public void accept(T t, StreamObserver<R> observer) {
                    try {
                        R ret = syncFun.apply(t);
                        observer.onNext(ret);
                        observer.onCompleted();
                    } catch (Throwable e) {
                        observer.onError(e);
                    }
                }
            };
        }

        @Override
        public CompletableFuture<cn.mobile.ivy.demo.helloworld.HelloReply> sayHelloAsync(cn.mobile.ivy.demo.helloworld.HelloRequest request){
                return CompletableFuture.completedFuture(sayHello(request));
        }

        // This server stream type unary method is <b>only</b> used for generated stub to support async unary method.
        // It will not be called if you are NOT using Dubbo3 generated triple stub and <b>DO NOT</b> implement this method.

        public void sayHello(cn.mobile.ivy.demo.helloworld.HelloRequest request, StreamObserver<cn.mobile.ivy.demo.helloworld.HelloReply> responseObserver){
            sayHelloAsync(request).whenComplete((r, t) -> {
                if (t != null) {
                    responseObserver.onError(t);
                } else {
                    responseObserver.onNext(r);
                    responseObserver.onCompleted();
                }
            });
        }

        @Override
        public final Invoker<HelloWorldService> getInvoker(URL url) {
            PathResolver pathResolver = url.getOrDefaultFrameworkModel()
            .getExtensionLoader(PathResolver.class)
            .getDefaultExtension();
            Map<String, StubMethodHandler<?, ?>> handlers = new HashMap<>();
            pathResolver.addNativeStub( "/" + SERVICE_NAME + "/sayHello");
            pathResolver.addNativeStub( "/" + SERVICE_NAME + "/sayHelloAsync");
            // for compatibility
            pathResolver.addNativeStub( "/" + JAVA_SERVICE_NAME + "/sayHello");
            pathResolver.addNativeStub( "/" + JAVA_SERVICE_NAME + "/sayHelloAsync");
            BiConsumer<cn.mobile.ivy.demo.helloworld.HelloRequest, StreamObserver<cn.mobile.ivy.demo.helloworld.HelloReply>> sayHelloFunc = this::sayHello;
            handlers.put(sayHelloMethod.getMethodName(), new UnaryStubMethodHandler<>(sayHelloFunc));
            BiConsumer<cn.mobile.ivy.demo.helloworld.HelloRequest, StreamObserver<cn.mobile.ivy.demo.helloworld.HelloReply>> sayHelloAsyncFunc = syncToAsync(this::sayHello);
            handlers.put(sayHelloProxyAsyncMethod.getMethodName(), new UnaryStubMethodHandler<>(sayHelloAsyncFunc));

            return new StubInvoker<>(this, url, HelloWorldService.class, handlers);
        }

        @Override
        public cn.mobile.ivy.demo.helloworld.HelloReply sayHello(cn.mobile.ivy.demo.helloworld.HelloRequest request){
            throw unimplementedMethodException(sayHelloMethod);
        }

        @Override
        public final ServiceDescriptor getServiceDescriptor() {
            return serviceDescriptor;
        }
        private RpcException unimplementedMethodException(StubMethodDescriptor methodDescriptor) {
            return TriRpcStatus.UNIMPLEMENTED.withDescription(String.format("Method %s is unimplemented",
                "/" + serviceDescriptor.getInterfaceName() + "/" + methodDescriptor.getMethodName())).asException();
        }
    }
}
