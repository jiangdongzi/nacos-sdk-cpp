#include <list>
#include <stdlib.h>
#include "naming/registry/NacosDiscovery.h"
#include "naming/registry/Naming.h"
#include "naming/NamingService.h"
#include "naming/Instance.h"
#include "naming/ServiceInfo.h"
#include "naming/subscribe/EventListener.h"
#include "naming/selectors/RandomByWeightSelector.h"

namespace nacos { namespace naming {

namespace {

NacosString getMeta(const std::map<NacosString, NacosString> &metadata, const NacosString &key) {
    std::map<NacosString, NacosString>::const_iterator it = metadata.find(key);
    if (it == metadata.end()) {
        return "";
    }
    return it->second;
}

int64_t toInt64(const NacosString &str) {
    if (str.empty()) {
        return 0;
    }
    return (int64_t) strtoll(str.c_str(), NULL, 10);
}

//Build a Service from a list of instances.
//Reads the "svrid" metadata key, mirroring the Go buildServiceByInstance.
Service buildServiceByInstances(const NacosString &serviceName, const std::list<Instance> &instances) {
    Service svr;
    svr.serviceName = serviceName;
    for (std::list<Instance>::const_iterator it = instances.begin(); it != instances.end(); it++) {
        Protocol protocol = (Protocol) toInt64(getMeta(it->metadata, "protocol"));
        int64_t svrId = toInt64(getMeta(it->metadata, "svrid"));
        NacosString envBranch = getMeta(it->metadata, "env_branch");
        Endpoint ep = NewEndpoint(serviceName, it->ip, (int64_t) it->port, (int64_t) it->weight,
                                  it->enabled, protocol, svrId, envBranch);
        svr.endpoints.push_back(ep);
    }
    return svr;
}

/**
 * Bridge EventListener -> ServiceChangeCallback. The dispatcher owns instances
 * of this class once they are handed to NamingService::subscribe.
 */
class CallbackEventListener : public EventListener {
private:
    NacosString _serviceName;
    ServiceChangeCallback _callback;

public:
    CallbackEventListener(const NacosString &serviceName, const ServiceChangeCallback &callback)
            : _serviceName(serviceName), _callback(callback) {
        setListenerName("registry-callback-" + serviceName);
    }

    void receiveNamingInfo(const ServiceInfo &serviceInfo) {
        //getHosts() is a non-const accessor that returns a copy; the object is
        //not actually const here, so const_cast is safe.
        std::list<Instance> hosts = const_cast<ServiceInfo &>(serviceInfo).getHosts();
        Service svr = buildServiceByInstances(_serviceName, hosts);
        if (_callback) {
            _callback(svr);
        }
    }
};

}//anonymous namespace

NacosDiscovery::NacosDiscovery(const NacosString &namespaceId, const NacosString &addr) NACOS_THROW(NacosException) {
    _client = new NamingClient(namespaceId, addr);
    _ownClient = true;
}

NacosDiscovery::NacosDiscovery(NamingClient *client) {
    _client = client;
    _ownClient = true;
}

NacosDiscovery::~NacosDiscovery() {
    close();
}

Endpoint NacosDiscovery::getEnableEndpoint(const NacosString &serviceName) NACOS_THROW(NacosException) {
    std::list<Instance> all = _client->naming()->getAllInstances(serviceName);

    //Keep only healthy & enabled instances, then weighted-random pick one,
    //mirroring the Go SelectOneHealthyInstance behaviour.
    std::list<Instance> candidates;
    for (std::list<Instance>::const_iterator it = all.begin(); it != all.end(); it++) {
        if (it->healthy && it->enabled) {
            candidates.push_back(*it);
        }
    }

    nacos::naming::selectors::RandomByWeightSelector selector;
    std::list<Instance> picked = selector.select(candidates);
    if (picked.empty()) {
        throw NacosException(NacosException::HTTP_NOT_FOUND,
                             "nacos discovery select enable endpoint failed, no available instance for service:" + serviceName);
    }

    const Instance &inst = picked.front();
    Protocol protocol = (Protocol) toInt64(getMeta(inst.metadata, "protocol"));
    //NOTE: the Go GetEnableEndpoint reads the "svr_id" metadata key here while
    //the register layer writes "svrid"; this discrepancy is preserved on purpose
    //to keep behaviour identical to the original Go implementation.
    int64_t svrId = toInt64(getMeta(inst.metadata, "svr_id"));
    NacosString envBranch = getMeta(inst.metadata, "env_branch");

    return NewEndpoint(serviceName, inst.ip, (int64_t) inst.port, (int64_t) inst.weight,
                       inst.enabled, protocol, svrId, envBranch);
}

Service NacosDiscovery::getService(const NacosString &serviceName) NACOS_THROW(NacosException) {
    std::list<Instance> all = _client->naming()->getAllInstances(serviceName);
    return buildServiceByInstances(serviceName, all);
}

void NacosDiscovery::registerCallback(const NacosString &serviceName,
                                      const ServiceChangeCallback &callback) NACOS_THROW(NacosException) {
    EventListener *listener = new CallbackEventListener(serviceName, callback);
    _client->naming()->subscribe(serviceName, listener);
    //The listener is owned by the SDK event dispatcher from now on. We keep a
    //reference only; it is freed when the underlying client/factory is torn down.
    _listeners.push_back(listener);
}

void NacosDiscovery::close() {
    _listeners.clear();
    if (_ownClient && _client != NULL) {
        _client->close();
        delete _client;
    }
    _client = NULL;
}

Discovery *NewDiscovery(const NacosString &namespaceId, const NacosString &addr) NACOS_THROW(NacosException) {
    return new NacosDiscovery(namespaceId, addr);
}

} /*naming*/ } /*nacos*/
