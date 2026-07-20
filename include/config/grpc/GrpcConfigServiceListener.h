#ifndef NACOS_SDK_CPP_GRPCCONFIGSERVICELISTENER_H
#define NACOS_SDK_CPP_GRPCCONFIGSERVICELISTENER_H

#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "NacosString.h"
#include "nacos_grpc_service.grpc.pb.h"

namespace nacos {
class Listener;
class ObjectConfigData;

struct GrpcConfigSubscriptionKey {
    NacosString dataId;
    NacosString group;
    NacosString tenant;

    bool operator<(const GrpcConfigSubscriptionKey &rhs) const {
        if (dataId != rhs.dataId) {
            return dataId < rhs.dataId;
        }
        if (group != rhs.group) {
            return group < rhs.group;
        }
        return tenant < rhs.tenant;
    }
};

class GrpcConfigServiceListener {
public:
    explicit GrpcConfigServiceListener(ObjectConfigData *objectConfigData);
    ~GrpcConfigServiceListener();

    void start();
    void stop();

    void addListener(const NacosString &dataId, const NacosString &group, const NacosString &tenant,
                     const NacosString &initialContent, Listener *listener);
    void removeListener(const NacosString &dataId, const NacosString &group, const NacosString &tenant,
                        Listener *listener);
    bool isConnected() const;

private:
    struct SubscriptionState {
        NacosString md5;
        std::set<Listener *> listeners;
        bool initializing;
        bool dirty;
        unsigned long generation;

        SubscriptionState() : initializing(true), dirty(true), generation(1) {}
    };

    struct ListenSnapshot {
        GrpcConfigSubscriptionKey key;
        NacosString md5;
        unsigned long generation;
    };

    ObjectConfigData *_objectConfigData;
    std::mutex subscriptionMutex;
    std::map<GrpcConfigSubscriptionKey, SubscriptionState> subscriptions;

    std::atomic<bool> running;
    std::atomic<bool> connectionReady;
    std::thread workerThread;
    std::thread healthThread;
    std::atomic<bool> stopHealth;
    std::atomic<bool> healthRunning;
    std::mutex healthMutex;
    std::condition_variable healthCv;
    std::atomic<long long> lastActiveMillis;
    std::atomic<long long> lastFullSyncMillis;
    int keepaliveIntervalMs;
    int healthTimeoutMs;

    void run();
    bool establishConnection();
    void closeStream();
    void markAllDirty();

    std::vector<ListenSnapshot> snapshotAll();
    std::vector<ListenSnapshot> snapshotDirty();
    bool sendBatchListen(const std::vector<ListenSnapshot> &snapshot, bool listen, bool processChanges);
    bool refreshConfig(const GrpcConfigSubscriptionKey &key, unsigned long expectedGeneration, bool notifyUser);
    void updateSubscriptionAfterQuery(const GrpcConfigSubscriptionKey &key, const NacosString &md5,
                                      unsigned long expectedGeneration);
    void markSnapshotClean(const ListenSnapshot &snapshot);
    bool isInitializing(const GrpcConfigSubscriptionKey &key);

    bool performServerCheck(std::string &connectionId);
    bool sendConnectionSetup(const std::string &connectionId);
    void handlePayload(const ::Payload &payload);
    bool sendStreamPayload(const std::string &type, const std::string &body);
    bool callUnary(const std::string &type, const std::string &body, std::string &responseBody,
                   int timeoutMs, bool notifyQuery = false);

    NacosString effectiveNamespace() const;
    NacosString resolveAppName() const;
    void addCommonHeaders(::Metadata *metadata);

    struct GrpcState;
    GrpcState *state;
    std::string clientIp;

    void startHealthLoop();
    void stopHealthLoop();
    void healthLoop();
    void markActivity();
    bool sendHealthCheck();
};
}

#endif // NACOS_SDK_CPP_GRPCCONFIGSERVICELISTENER_H
