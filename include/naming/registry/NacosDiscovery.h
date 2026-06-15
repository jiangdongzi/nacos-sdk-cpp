#ifndef __NACOS_REGISTRY_NACOS_DISCOVERY_H_
#define __NACOS_REGISTRY_NACOS_DISCOVERY_H_

#include <list>
#include "NacosString.h"
#include "NacosExceptions.h"
#include "Compatibility.h"
#include "naming/registry/Discovery.h"
#include "naming/registry/NamingClient.h"

namespace nacos {

class EventListener;

namespace naming {

/**
 * Nacos-backed implementation of the Discovery facade, mirroring the Go
 * `nacos.Discovery`.
 */
class NacosDiscovery : public Discovery {
private:
    NamingClient *_client;
    bool _ownClient;
    std::list<EventListener *> _listeners;

    NacosDiscovery();

public:
    NacosDiscovery(const NacosString &namespaceId, const NacosString &addr) NACOS_THROW(NacosException);

    /**
     * Build a Discovery on top of an existing NamingClient. Ownership of the
     * client is transferred to this object.
     */
    explicit NacosDiscovery(NamingClient *client);

    ~NacosDiscovery();

    Endpoint getEnableEndpoint(const NacosString &serviceName) NACOS_THROW(NacosException);

    Service getService(const NacosString &serviceName) NACOS_THROW(NacosException);

    void registerCallback(const NacosString &serviceName,
                          const ServiceChangeCallback &callback) NACOS_THROW(NacosException);

    void close();
};

} /*naming*/ } /*nacos*/

#endif
