package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

const defaultServiceName = "providers:cn.mobile.ivy.demo.helloworld.HelloWorldService::"

func main() {
	logger, closeLog, err := newLogger()
	if err != nil {
		log.Fatalf("init logger: %v", err)
	}
	defer closeLog()

	serviceName := env("NACOS_WATCH_SERVICE", defaultServiceName)
	groupName := env("NACOS_GROUP", "DEFAULT_GROUP")

	namingClient, err := clients.NewNamingClient(vo.NacosClientParam{
		ClientConfig: &constant.ClientConfig{
			NamespaceId:         env("NACOS_NAMESPACE", "test"),
			TimeoutMs:           5000,
			NotLoadCacheAtStart: envBool("NACOS_NOT_LOAD_CACHE", true),
			LogDir:              filepath.Clean(env("NACOS_SDK_LOG_DIR", "logs/nacos-sdk")),
			CacheDir:            filepath.Clean(env("NACOS_SDK_CACHE_DIR", "logs/nacos-cache")),
			Username:            env("NACOS_USERNAME", "nacos"),
			Password:            env("NACOS_PASSWORD", "nacos"),
		},
		ServerConfigs: []constant.ServerConfig{
			newServerConfig(env("NACOS_SERVER_ADDR", "120.26.172.16:8848")),
		},
	})
	if err != nil {
		logger.Fatalf("new nacos naming client: %v", err)
	}

	logger.Printf("watch service=%s group=%s namespace=%s server=%s", serviceName, groupName, env("NACOS_NAMESPACE", "test"), env("NACOS_SERVER_ADDR", "120.26.172.16:8848"))
	logger.Printf("sdk cache=%s log=%s notLoadCacheAtStart=%t", filepath.Clean(env("NACOS_SDK_CACHE_DIR", "logs/nacos-cache")), filepath.Clean(env("NACOS_SDK_LOG_DIR", "logs/nacos-sdk")), envBool("NACOS_NOT_LOAD_CACHE", true))

	var (
		pushMu       sync.Mutex
		lastPushSeen instanceSnapshot
	)
	if err := namingClient.Subscribe(&vo.SubscribeParam{
		ServiceName: serviceName,
		GroupName:   groupName,
		SubscribeCallback: func(instances []model.Instance, err error) {
			pushMu.Lock()
			defer pushMu.Unlock()

			current := newInstanceSnapshot(instances)
			logger.Printf("SDK_PUSH %s", diffInstances(lastPushSeen, current))
			lastPushSeen = current
			printInstances(logger, "SDK_PUSH_LIST", instances, err)
		},
	}); err != nil {
		logger.Fatalf("subscribe: %v", err)
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		instances, err := namingClient.SelectAllInstances(vo.SelectAllInstancesParam{
			ServiceName: serviceName,
			GroupName:   groupName,
		})
		printInstances(logger, "SDK_CACHE", instances, err)
		<-ticker.C
	}
}

func newLogger() (*log.Logger, func(), error) {
	logDir := filepath.Clean(env("NACOS_WATCH_LOG_DIR", "logs"))
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, nil, err
	}

	file, err := os.OpenFile(filepath.Join(logDir, "nacos-watch.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, nil, err
	}

	logger := log.New(file, "", log.LstdFlags)
	return logger, func() { _ = file.Close() }, nil
}

func newServerConfig(addr string) constant.ServerConfig {
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
		portText = "8848"
	}

	port, err := strconv.ParseUint(portText, 10, 64)
	if err != nil || port == 0 {
		port = 8848
	}

	return constant.ServerConfig{
		IpAddr: host,
		Port:   port,
	}
}

func printInstances(logger *log.Logger, source string, instances []model.Instance, err error) {
	if err != nil {
		logger.Printf("%s error: %v", source, err)
		return
	}

	if len(instances) == 0 {
		logger.Printf("%s instances=0 []", source)
		return
	}

	parts := make([]string, 0, len(instances))
	for _, inst := range instances {
		timestamp := inst.Metadata["timestamp"]
		side := inst.Metadata["side"]
		parts = append(parts, fmt.Sprintf("%s:%d healthy=%t enabled=%t weight=%.1f ephemeral=%t side=%s timestamp=%s", inst.Ip, inst.Port, inst.Healthy, inst.Enable, inst.Weight, inst.Ephemeral, side, timestamp))
	}
	logger.Printf("%s instances=%d [%s]", source, len(instances), strings.Join(parts, "; "))
}

type instanceSnapshot map[string]model.Instance

func newInstanceSnapshot(instances []model.Instance) instanceSnapshot {
	snapshot := make(instanceSnapshot, len(instances))
	for _, inst := range instances {
		snapshot[instanceKey(inst)] = inst
	}
	return snapshot
}

func diffInstances(previous, current instanceSnapshot) string {
	if previous == nil {
		return "initial"
	}

	added := make([]string, 0)
	removed := make([]string, 0)
	changed := make([]string, 0)

	for key, inst := range current {
		old, ok := previous[key]
		if !ok {
			added = append(added, instanceLabel(inst))
			continue
		}
		if old.Healthy != inst.Healthy || old.Enable != inst.Enable || old.Weight != inst.Weight || old.Metadata["timestamp"] != inst.Metadata["timestamp"] {
			changed = append(changed, fmt.Sprintf("%s %s -> %s", key, old.Metadata["timestamp"], inst.Metadata["timestamp"]))
		}
	}

	for key, inst := range previous {
		if _, ok := current[key]; !ok {
			removed = append(removed, instanceLabel(inst))
		}
	}

	if len(added) == 0 && len(removed) == 0 && len(changed) == 0 {
		return "unchanged"
	}

	parts := make([]string, 0, 3)
	if len(added) > 0 {
		parts = append(parts, "added=["+strings.Join(added, "; ")+"]")
	}
	if len(removed) > 0 {
		parts = append(parts, "removed=["+strings.Join(removed, "; ")+"]")
	}
	if len(changed) > 0 {
		parts = append(parts, "changed=["+strings.Join(changed, "; ")+"]")
	}
	return strings.Join(parts, " ")
}

func instanceKey(inst model.Instance) string {
	return fmt.Sprintf("%s:%d", inst.Ip, inst.Port)
}

func instanceLabel(inst model.Instance) string {
	return fmt.Sprintf("%s healthy=%t enabled=%t timestamp=%s", instanceKey(inst), inst.Healthy, inst.Enable, inst.Metadata["timestamp"])
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}
