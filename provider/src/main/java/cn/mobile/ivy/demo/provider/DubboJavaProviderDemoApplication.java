package cn.mobile.ivy.demo.provider;

import org.apache.dubbo.config.spring.context.annotation.EnableDubbo;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableDubbo
@SpringBootApplication
public class DubboJavaProviderDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DubboJavaProviderDemoApplication.class, args);
    }
}
