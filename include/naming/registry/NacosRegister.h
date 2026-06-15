#ifndef __NACOS_REGISTRY_NACOS_REGISTER_H_
#define __NACOS_REGISTRY_NACOS_REGISTER_H_

#include "NacosString.h"
#include "NacosExceptions.h"
#include "Compatibility.h"
#include "naming/registry/Register.h"
#include "naming/registry/NamingClient.h"

namespace nacos { namespace naming {

/**
 * Nacos-backed implementation of the Register facade, mirroring the Go
 * `nacos.Register`.
 */
class NacosRegister : public Register {
private:
    NamingClient *_client;
    bool _ownClient;
    ApiVersion _apiVersion;

    NacosRegister();

public:
    NacosRegister(const NacosString &namespaceId, const NacosString &addr,
                  ApiVersion apiVersion = API_VERSION_OLD) NACOS_THROW(NacosException);

    /**
     * Build a Register on top of an existing NamingClient. Ownership of the
     * client is transferred to this object.
     */
    NacosRegister(NamingClient *client, ApiVersion apiVersion);

    ~NacosRegister();

    void addEndpoint(const Endpoint &endpoint) NACOS_THROW(NacosException);

    void enableEndpoint(const NacosString &serviceName, const NacosString &ip,
                        int64_t port, Protocol protocol) NACOS_THROW(NacosException);

    void updateEndpoint(const Endpoint &endpoint) NACOS_THROW(NacosException);

    void disableEndpoint(const NacosString &serviceName, const NacosString &ip,
                         int64_t port, Protocol protocol) NACOS_THROW(NacosException);

    void delEndpoint(const NacosString &serviceName, const NacosString &ip,
                     int64_t port, Protocol protocol) NACOS_THROW(NacosException);

    void close();
};

} /*naming*/ } /*nacos*/

#endif
