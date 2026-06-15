#ifndef __NACOS_REGISTRY_NAMING_CLIENT_H_
#define __NACOS_REGISTRY_NAMING_CLIENT_H_

#include "NacosString.h"
#include "NacosExceptions.h"
#include "Compatibility.h"
#include "naming/registry/Endpoint.h"

namespace nacos {

class INacosServiceFactory;
class NamingService;

namespace naming {

/**
 * API version used by NamingClient::updateEndpoint, mirroring the Go
 * `nacos.ApiVersion` ("old" / "new").
 *
 *  - API_VERSION_OLD: serviceName is sent grouped as "DEFAULT_GROUP@@<name>" and
 *    the raw "ok" body is expected (legacy behaviour).
 *  - API_VERSION_NEW: serviceName is sent as-is and a JSON `{data:"ok"}` body is
 *    expected (Nacos 2.x open-API behaviour).
 */
enum ApiVersion {
    API_VERSION_OLD = 0,
    API_VERSION_NEW = 1
};

/**
 * Thrown by NamingClient::updateEndpoint when the target instance/service does
 * not exist, mirroring the Go `errors.EndpointNotFound` sentinel. Callers can
 * catch this type specifically to distinguish "not found" from other failures.
 */
class EndpointNotFoundException : public NacosException {
public:
    static const int ENDPOINT_NOT_FOUND = 100404;

    explicit EndpointNotFoundException(const NacosString &errormsg) NACOS_NOTHROW()
            : NacosException(ENDPOINT_NOT_FOUND, errormsg) {};
};

/**
 * NamingClient wraps the lower level nacos-sdk-cpp NamingService and exposes the
 * Nacos open-API based `updateEndpoint`, mirroring the Go `nacos.NamingClient`.
 *
 * The object owns the underlying service factory; deleting it (or calling
 * close()) tears down the naming service, heartbeat reactor, pollers and the
 * gRPC/HTTP subscription listeners.
 */
class NamingClient {
private:
    INacosServiceFactory *_factory;
    NamingService *_naming;
    NacosString _namespaceId;
    NacosString _addr;

    NamingClient();

public:
    NamingClient(const NacosString &namespaceId, const NacosString &addr) NACOS_THROW(NacosException);

    ~NamingClient();

    /**
     * The underlying NamingService. The pointer is owned by this NamingClient.
     */
    NamingService *naming() const { return _naming; };

    NacosString getNamespace() const { return _namespaceId; };

    NacosString getAddr() const { return _addr; };

    /**
     * Update (enable/disable/weight) an instance via the Nacos open-API
     * (PUT /nacos/v2/ns/instance), mirroring Go's `NamingClient.UpdateEndpoint`.
     *
     * @throw EndpointNotFoundException when the instance/service is not found
     * @throw NacosException            on any other failure
     */
    void updateEndpoint(const Endpoint &endpoint, ApiVersion apiVersion) NACOS_THROW(NacosException);

    /**
     * Release the underlying naming service. Safe to call multiple times.
     */
    void close();
};

} /*naming*/ } /*nacos*/

#endif
