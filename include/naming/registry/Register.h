#ifndef __NACOS_REGISTRY_REGISTER_H_
#define __NACOS_REGISTRY_REGISTER_H_

#include <stdint.h>
#include "NacosString.h"
#include "NacosExceptions.h"
#include "Compatibility.h"
#include "naming/registry/Endpoint.h"
#include "naming/registry/Protocol.h"

namespace nacos { namespace naming {

/**
 * Register is the service-registration facade.
 *
 * It mirrors the Go `register.Register` interface. Errors are reported by
 * throwing NacosException instead of being returned.
 */
class Register {
public:
    /**
     * Register an ephemeral instance. The SDK keeps it alive with heartbeats.
     */
    virtual void addEndpoint(const Endpoint &endpoint) NACOS_THROW(NacosException) = 0;

    /**
     * Mark an instance as enabled (able to accept traffic).
     */
    virtual void enableEndpoint(const NacosString &serviceName, const NacosString &ip,
                                int64_t port, Protocol protocol) NACOS_THROW(NacosException) = 0;

    /**
     * Update an instance (weight / enabled state) through the Nacos open-API.
     */
    virtual void updateEndpoint(const Endpoint &endpoint) NACOS_THROW(NacosException) = 0;

    /**
     * Mark an instance as disabled (not accepting traffic).
     */
    virtual void disableEndpoint(const NacosString &serviceName, const NacosString &ip,
                                 int64_t port, Protocol protocol) NACOS_THROW(NacosException) = 0;

    /**
     * Deregister an instance.
     */
    virtual void delEndpoint(const NacosString &serviceName, const NacosString &ip,
                             int64_t port, Protocol protocol) NACOS_THROW(NacosException) = 0;

    /**
     * Release all resources held by this Register.
     */
    virtual void close() = 0;

    virtual ~Register() {}
};

} /*naming*/ } /*nacos*/

#endif
