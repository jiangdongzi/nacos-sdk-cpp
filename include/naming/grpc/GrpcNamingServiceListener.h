#ifndef NACOS_SDK_CPP_GRPCNAMINGSERVICELISTENER_H
#define NACOS_SDK_CPP_GRPCNAMINGSERVICELISTENER_H

#include <atomic>
#include <mutex>
#include <set>
#include <string>
#include <thread>

#include "NacosString.h"
#include "nacos_grpc_service.grpc.pb.h"

namespace nacos {
class ObjectConfigData;

struct GrpcSubscriptionKey {
    NacosString serviceName;
    NacosString groupName;
    NacosString clusters;

    bool operator<(const GrpcSubscriptionKey &rhs) const {
        if (serviceName != rhs.serviceName) {
            return serviceName < rhs.serviceName;
        }
        if (groupName != rhs.groupName) {
            return groupName < rhs.groupName;
        }
        return clusters < rhs.clusters;
    }
};

class GrpcNamingServiceListener {
public:
    explicit GrpcNamingServiceListener(ObjectConfigData *objectConfigData);
    ~GrpcNamingServiceListener();

    void start();
    void stop();

    void subscribe(const NacosString &serviceName, const NacosString &groupName, const NacosString &clusters);
    void unsubscribe(const NacosString &serviceName, const NacosString &groupName, const NacosString &clusters);

private:
    ObjectConfigData *_objectConfigData;

    std::mutex subscriptionMutex;
    std::set<GrpcSubscriptionKey> subscriptions;

    std::atomic<bool> running;
    std::atomic<bool> connectionReady;
    std::thread workerThread;

    void run();
    bool establishConnection();
    void closeStream();
    void resubscribeAll();
    bool sendSubscribeRequest(const GrpcSubscriptionKey &key, bool subscribeFlag);

    bool performServerCheck(std::string &connectionId);
    bool sendConnectionSetup(const std::string &connectionId);

    void handlePayload(const ::Payload &payload);
    bool sendStreamAck(const std::string &type, const std::string &body);

    bool callUnary(const std::string &type, const std::string &body, std::string &responseBody);
    void processServiceInfoJson(const std::string &serviceInfoJson);
    void fetchServiceSnapshot(const NacosString &serviceName, const NacosString &groupName, const NacosString &clusters);

    NacosString resolveAppName() const;
    struct GrpcState;
    GrpcState *state;
    std::string clientIp;
};
}

#endif // NACOS_SDK_CPP_GRPCNAMINGSERVICELISTENER_H
