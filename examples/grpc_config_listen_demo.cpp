#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <string>

#include "Nacos.h"
#include "constant/ConfigConstant.h"

using namespace nacos;

class GrpcConfigListener : public Listener {
public:
    GrpcConfigListener() : receivedCount(0) {}

    void receiveConfigInfo(const NacosString &configInfo) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            content = configInfo;
            ++receivedCount;
        }
        std::cout << "CONFIG_UPDATE_RECEIVED=" << configInfo << std::endl;
        condition.notify_all();
    }

    bool waitForUpdates(int expectedUpdates, int timeoutSeconds) {
        std::unique_lock<std::mutex> lock(mutex);
        return condition.wait_for(lock, std::chrono::seconds(timeoutSeconds),
                                  [this, expectedUpdates] { return receivedCount >= expectedUpdates; });
    }

    int getReceivedCount() {
        std::lock_guard<std::mutex> lock(mutex);
        return receivedCount;
    }

private:
    std::mutex mutex;
    std::condition_variable condition;
    NacosString content;
    int receivedCount;
};

int main(int argc, char **argv) {
    const NacosString dataId = argc > 1 ? argv[1] : "grpc-config-listen-test";
    const NacosString serverAddr = argc > 2 ? argv[2] : "127.0.0.1:8848";
    const int expectedUpdates = argc > 3 ? std::atoi(argv[3]) : 1;
    const int timeoutSeconds = argc > 4 ? std::atoi(argv[4]) : 45;
    const char *configuredLogPath = std::getenv("NACOS_LOG_PATH");
    const NacosString logPath = configuredLogPath == NULL ? "./logs" : configuredLogPath;
    Properties properties;
    properties[PropertyKeyConst::SERVER_ADDR] = serverAddr;
    properties[PropertyKeyConst::CONFIG_GRPC_ENABLED] = "true";
    properties[PropertyKeyConst::LOG_PATH] = logPath;
    properties[PropertyKeyConst::LOG_LEVEL] = "DEBUG";

    INacosServiceFactory *factory = NacosFactoryFactory::getNacosFactory(properties);
    ResourceGuard<INacosServiceFactory> factoryGuard(factory);
    ConfigService *configService = factory->CreateConfigService();
    ResourceGuard<ConfigService> serviceGuard(configService);

    GrpcConfigListener *listener = new GrpcConfigListener();
    configService->addListener(dataId, ConfigConstant::DEFAULT_GROUP, listener);
    std::cout << "LISTENER_READY dataId=" << dataId << " expectedUpdates=" << expectedUpdates << std::endl;

    if (!listener->waitForUpdates(expectedUpdates, timeoutSeconds)) {
        std::cerr << "CONFIG_UPDATE_TIMEOUT received=" << listener->getReceivedCount()
                  << " expected=" << expectedUpdates << std::endl;
        configService->removeListener(dataId, ConfigConstant::DEFAULT_GROUP, listener);
        return 1;
    }

    configService->removeListener(dataId, ConfigConstant::DEFAULT_GROUP, listener);
    return 0;
}
