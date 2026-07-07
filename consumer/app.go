package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"dgdemo/pb"

	"dubbo.apache.org/dubbo-go/v3/client"
	"dubbo.apache.org/dubbo-go/v3/common/constant"
	dubboconfig "dubbo.apache.org/dubbo-go/v3/config"
	"dubbo.apache.org/dubbo-go/v3/global"
	_ "dubbo.apache.org/dubbo-go/v3/imports"
	_ "dubbo.apache.org/dubbo-go/v3/logger/core/zap"
	"dubbo.apache.org/dubbo-go/v3/registry"
)

func main() {
	if err := initDubboLogger(); err != nil {
		log.Fatalf("init dubbo logger: %v", err)
	}

	cli, err := newDubboClient()
	if err != nil {
		log.Fatalf("new dubbo client: %v", err)
	}

	svc, err := pb.NewHelloWorldService(
		cli,
		client.WithProtocolTriple(),
		client.WithCheck(),
	)
	fmt.Println(svc)
	if err != nil {
		log.Fatalf("new hello world service: %v", err)
	}

	//name := env("HELLO_NAME", "dubbo-go")
	//callSayHello(svc, name)

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		fmt.Println("1分钟过去了...")
		//callSayHello(svc, name)
	}
}

func callSayHello(svc pb.HelloWorldService, name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := svc.SayHello(ctx, &pb.HelloRequest{Name: name})
	if err != nil {
		log.Printf("say hello failed: %v", err)
		return
	}

	fmt.Printf("[%s] response: %s\n", time.Now().Format(time.DateTime), resp.GetMessage())
}

func newDubboClient() (*client.Client, error) {
	app := global.DefaultApplicationConfig()
	app.Name = env("DUBBO_APPLICATION_NAME", "dubbo-go-consumer-demo")
	app.Environment = env("NACOS_NAMESPACE", "test")

	return client.NewClient(
		client.SetClientApplication(app),
		client.WithClientLoadBalance(constant.LoadBalanceKeyInterleavedWeightedRoundRobin),
		client.WithClientRegistry(
			registry.WithNacos(),
			registry.WithAddress(env("NACOS_SERVER_ADDR", "120.26.172.16:8848")),
			registry.WithNamespace(env("NACOS_NAMESPACE", "test")),
			registry.WithGroup(env("NACOS_GROUP", "DEFAULT_GROUP")),
			registry.WithUsername(env("NACOS_USERNAME", "nacos")),
			registry.WithPassword(env("NACOS_PASSWORD", "nacos")),
			registry.WithRegisterInterface(),
		),
		client.WithClientRequestTimeout(5*time.Second),
	)
}

func initDubboLogger() error {
	logDir := filepath.Clean(env("DUBBO_LOG_DIR", "logs"))
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	compress := true
	cfg := &dubboconfig.LoggerConfig{
		Driver:   "zap",
		Level:    env("DUBBO_LOG_LEVEL", "info"),
		Format:   "text",
		Appender: "file",
		File: &dubboconfig.File{
			Name:       filepath.Join(logDir, "dubbo.log"),
			MaxSize:    100,
			MaxBackups: 5,
			MaxAge:     3,
			Compress:   &compress,
		},
	}
	return cfg.Init()
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
