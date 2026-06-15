#include "naming/registry/NacosRegister.h"
#include "naming/registry/Naming.h"
#include "naming/NamingService.h"
#include "naming/Instance.h"

namespace nacos { namespace naming {

namespace {

const char *DEFAULT_CLUSTER = "DEFAULT";

void checkEndpointLegal(const NacosString &serviceName, const NacosString &ip, int64_t port,
                        const NacosString &action) NACOS_THROW(NacosException) {
    if (serviceName.empty() || ip.empty() || port == 0) {
        throw NacosException(NacosException::INVALID_PARAM,
                             "nacos register " + action + " failed, endpoint illegal");
    }
}

}//anonymous namespace

NacosRegister::NacosRegister(const NacosString &namespaceId, const NacosString &addr,
                             ApiVersion apiVersion) NACOS_THROW(NacosException) {
    _client = new NamingClient(namespaceId, addr);
    _ownClient = true;
    _apiVersion = apiVersion;
}

NacosRegister::NacosRegister(NamingClient *client, ApiVersion apiVersion) {
    _client = client;
    _ownClient = true;
    _apiVersion = apiVersion;
}

NacosRegister::~NacosRegister() {
    close();
}

void NacosRegister::addEndpoint(const Endpoint &endpoint) NACOS_THROW(NacosException) {
    checkEndpointLegal(endpoint.serviceName, endpoint.ip, endpoint.port, "add endpoint");

    int64_t weight = endpoint.weight;
    if (weight <= 0) {
        weight = 100;
    }

    //Create an ephemeral instance; the SDK sends a heartbeat every few seconds.
    Instance instance;
    instance.ip = endpoint.ip;
    instance.port = (int) endpoint.port;
    instance.weight = (double) weight;
    instance.healthy = true;
    instance.enabled = endpoint.enable;
    instance.ephemeral = true;
    instance.clusterName = DEFAULT_CLUSTER;
    instance.metadata["protocol"] = NacosStringOps::valueOf((int64_t) endpoint.protocol);
    if (endpoint.svrId != 0) {
        instance.metadata["svrid"] = NacosStringOps::valueOf(endpoint.svrId);
    }
    if (!endpoint.envBranch.empty()) {
        instance.metadata["env_branch"] = endpoint.envBranch;
    }

    _client->naming()->registerInstance(endpoint.serviceName, instance);
}

void NacosRegister::enableEndpoint(const NacosString &serviceName, const NacosString &ip,
                                   int64_t port, Protocol protocol) NACOS_THROW(NacosException) {
    Endpoint endpoint = NewEndpoint(serviceName, ip, port, 100, true, protocol);
    updateEndpoint(endpoint);
}

void NacosRegister::disableEndpoint(const NacosString &serviceName, const NacosString &ip,
                                    int64_t port, Protocol protocol) NACOS_THROW(NacosException) {
    Endpoint endpoint = NewEndpoint(serviceName, ip, port, 100, false, protocol);
    updateEndpoint(endpoint);
}

void NacosRegister::updateEndpoint(const Endpoint &endpoint) NACOS_THROW(NacosException) {
    checkEndpointLegal(endpoint.serviceName, endpoint.ip, endpoint.port, "update endpoint");
    _client->updateEndpoint(endpoint, _apiVersion);
}

void NacosRegister::delEndpoint(const NacosString &serviceName, const NacosString &ip,
                                int64_t port, Protocol protocol) NACOS_THROW(NacosException) {
    (void) protocol;
    checkEndpointLegal(serviceName, ip, port, "del endpoint");
    _client->naming()->deregisterInstance(serviceName, ip, (int) port, DEFAULT_CLUSTER);
}

void NacosRegister::close() {
    if (_ownClient && _client != NULL) {
        _client->close();
        delete _client;
    }
    _client = NULL;
}

Register *NewRegister(const NacosString &namespaceId, const NacosString &addr,
                      ApiVersion apiVersion) NACOS_THROW(NacosException) {
    return new NacosRegister(namespaceId, addr, apiVersion);
}

} /*naming*/ } /*nacos*/
