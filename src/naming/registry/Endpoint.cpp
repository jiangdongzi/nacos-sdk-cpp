#include "naming/registry/Endpoint.h"
#include "NacosString.h"

namespace nacos { namespace naming {

Endpoint::Endpoint() {
    ip = "";
    port = 0;
    version = 0;
    serviceName = "";
    enable = false;
    weight = 0;
    protocol = ProtocolUnknown;
    svrId = 0;
    envBranch = "";
}

NacosString Endpoint::toString() const {
    return "Endpoint{serviceName:" + serviceName +
           " ip:" + ip +
           " port:" + NacosStringOps::valueOf(port) +
           " weight:" + NacosStringOps::valueOf(weight) +
           " enable:" + NacosStringOps::valueOf(enable) +
           " protocol:" + NacosStringOps::valueOf((int) protocol) +
           " svrId:" + NacosStringOps::valueOf(svrId) +
           " envBranch:" + envBranch +
           " version:" + NacosStringOps::valueOf(version) + "}";
}

Endpoint NewEndpoint(const NacosString &serviceName, const NacosString &ip,
                     int64_t port, int64_t weight, bool enable, Protocol protocol,
                     int64_t svrId, const NacosString &envBranch) {
    Endpoint ep;
    ep.serviceName = serviceName;
    ep.ip = ip;
    ep.port = port;
    ep.weight = weight;
    ep.enable = enable;
    ep.protocol = protocol;

    if (svrId != 0) {
        ep.svrId = svrId;
    }
    if (!envBranch.empty()) {
        ep.envBranch = envBranch;
    }

    return ep;
}

} /*naming*/ } /*nacos*/
