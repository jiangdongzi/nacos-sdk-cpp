#include <iostream>
#include <memory>
#include <unistd.h>
#include "Nacos.h"

using namespace std;
using namespace nacos;

class GrpcSubscribeListener : public EventListener {
public:
    void receiveNamingInfo(const ServiceInfo &serviceInfo) override {
        cout << "=== gRPC subscribe update ===" << endl;
        cout << serviceInfo.toInstanceString() << endl;
        cout << "=============================" << endl;
    }
};

int main() {
    Properties props;
    props[PropertyKeyConst::SERVER_ADDR] = "127.0.0.1:8848";
    props[PropertyKeyConst::GRPC_KEEPALIVE_INTERVAL] = "5000";
    props[PropertyKeyConst::GRPC_HEALTHCHECK_TIMEOUT] = "2000";
    props[PropertyKeyConst::LOG_PATH] = "./logs";
    props[PropertyKeyConst::LOG_LEVEL] = "INFO";
    const static std::string CPP_LONG_LINK = "cpp_long_link";
    props[PropertyKeyConst::NAMESPACE] = CPP_LONG_LINK;

    INacosServiceFactory *factory = NacosFactoryFactory::getNacosFactory(props);
    ResourceGuard<INacosServiceFactory> factoryGuard(factory);
    NamingService *naming = factory->CreateNamingService();
    ResourceGuard<NamingService> namingGuard(naming);

    std::unique_ptr<GrpcSubscribeListener> listenerHolder(new GrpcSubscribeListener());
    GrpcSubscribeListener *listener = listenerHolder.get();
    NacosString serviceName = "DemoGrpcService";

    try {
        naming->subscribe(serviceName, listener);
        listenerHolder.release(); // ownership handled by SDK (EventDispatcher)
        cout << "Subscribed to " << serviceName << ", waiting for updates..." << endl;
        sleep(240);
        naming->unsubscribe(serviceName, listener);
        cout << "Unsubscribed from " << serviceName << endl;
    } catch (NacosException &e) {
        cout << "Subscription failed: " << e.what() << endl;
        return -1;
    }

    return 0;
}
