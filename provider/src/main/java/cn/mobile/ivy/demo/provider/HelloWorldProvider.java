package cn.mobile.ivy.demo.provider;

import cn.mobile.ivy.demo.helloworld.DubboHelloWorldServiceTriple;
import cn.mobile.ivy.demo.helloworld.HelloReply;
import cn.mobile.ivy.demo.helloworld.HelloRequest;
import org.apache.dubbo.config.annotation.DubboService;
import org.springframework.util.StringUtils;

@DubboService
public class HelloWorldProvider extends DubboHelloWorldServiceTriple.HelloWorldServiceImplBase {

    @Override
    public HelloReply sayHello(HelloRequest request) {
        String name = StringUtils.hasText(request.getName()) ? request.getName().trim() : "world";
        return HelloReply.newBuilder()
                .setMessage("hello, " + name)
                .build();
    }
}
