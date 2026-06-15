#ifndef __NACOS_REGISTRY_NAMING_H_
#define __NACOS_REGISTRY_NAMING_H_

/**
 * High level service discovery & registration facade for nacos-sdk-cpp.
 *
 * This is a C++ port of the Go `naming` module (the wrapper around the Go nacos
 * SDK). It exposes the same external concepts:
 *
 *   - service types : Protocol / Endpoint / Service
 *   - interfaces    : Discovery / Register
 *   - nacos backend : NacosDiscovery / NacosRegister / NamingClient
 *   - constructors  : NewDiscovery / NewRegister / NewNamingClient
 *
 * Include this single header to use the whole facade.
 */

#include "NacosString.h"
#include "NacosExceptions.h"
#include "Compatibility.h"
#include "naming/registry/Protocol.h"
#include "naming/registry/Endpoint.h"
#include "naming/registry/Service.h"
#include "naming/registry/Discovery.h"
#include "naming/registry/Register.h"
#include "naming/registry/NamingClient.h"
#include "naming/registry/NacosDiscovery.h"
#include "naming/registry/NacosRegister.h"

namespace nacos { namespace naming {

/**
 * Create a Nacos service discovery facade.
 * Mirrors the Go `nacos.NewDiscovery(namespace, addr)`.
 *
 * @param namespaceId nacos namespace id ("" means the public namespace)
 * @param addr        comma separated list of "host:port" nacos servers
 * @return a heap allocated Discovery; the caller owns it (delete / ResourceGuard)
 */
Discovery *NewDiscovery(const NacosString &namespaceId, const NacosString &addr) NACOS_THROW(NacosException);

/**
 * Create a Nacos service registration facade.
 * Mirrors the Go `nacos.NewRegister(namespace, addr, RegisterApiVersion(...))`.
 *
 * @param namespaceId nacos namespace id ("" means the public namespace)
 * @param addr        comma separated list of "host:port" nacos servers
 * @param apiVersion  open-API version used by enable/disable/update endpoint
 * @return a heap allocated Register; the caller owns it (delete / ResourceGuard)
 */
Register *NewRegister(const NacosString &namespaceId, const NacosString &addr,
                      ApiVersion apiVersion = API_VERSION_OLD) NACOS_THROW(NacosException);

/**
 * Create a low level NamingClient.
 * Mirrors the Go `nacos.NewNamingClient(namespace, addr)`.
 *
 * @param namespaceId nacos namespace id ("" means the public namespace)
 * @param addr        comma separated list of "host:port" nacos servers
 * @return a heap allocated NamingClient; the caller owns it (delete / ResourceGuard)
 */
NamingClient *NewNamingClient(const NacosString &namespaceId, const NacosString &addr) NACOS_THROW(NacosException);

} /*naming*/ } /*nacos*/

#endif
