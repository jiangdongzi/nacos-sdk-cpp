#ifndef __NACOS_REGISTRY_DISCOVERY_H_
#define __NACOS_REGISTRY_DISCOVERY_H_

#include <functional>
#include "NacosString.h"
#include "NacosExceptions.h"
#include "Compatibility.h"
#include "naming/registry/Endpoint.h"
#include "naming/registry/Service.h"

namespace nacos { namespace naming {

/**
 * Callback invoked whenever the instance list of a subscribed service changes.
 * It is the C++ equivalent of the Go `func(svr service.Service)` callback.
 */
typedef std::function<void(const Service &)> ServiceChangeCallback;

/**
 * Discovery is the service-discovery facade.
 *
 * It mirrors the Go `discovery.Discovery` interface. The Go `context.Context`
 * parameter is dropped since it has no idiomatic C++ counterpart, and errors are
 * reported by throwing NacosException instead of being returned.
 */
class Discovery {
public:
    /**
     * Get one available (healthy & enabled) endpoint of a service, selected with
     * a weighted-random strategy. Throws NacosException when no endpoint matches.
     */
    virtual Endpoint getEnableEndpoint(const NacosString &serviceName) NACOS_THROW(NacosException) = 0;

    /**
     * Get the full service (all of its endpoints).
     */
    virtual Service getService(const NacosString &serviceName) NACOS_THROW(NacosException) = 0;

    /**
     * Subscribe to instance changes of a service. The callback is invoked on
     * every push/refresh with the latest Service snapshot.
     */
    virtual void registerCallback(const NacosString &serviceName,
                                  const ServiceChangeCallback &callback) NACOS_THROW(NacosException) = 0;

    /**
     * Release all resources held by this Discovery.
     */
    virtual void close() = 0;

    virtual ~Discovery() {}
};

} /*naming*/ } /*nacos*/

#endif
