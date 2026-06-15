#ifndef __NACOS_REGISTRY_ENDPOINT_H_
#define __NACOS_REGISTRY_ENDPOINT_H_

#include <stdint.h>
#include "NacosString.h"
#include "naming/registry/Protocol.h"

namespace nacos { namespace naming {

/**
 * Endpoint describes a single service instance.
 *
 * It is the C++ counterpart of the Go `service.Endpoint` struct, keeping the
 * same field set so that callers migrating from the Go `naming` module can use
 * an almost identical data model.
 */
class Endpoint {
public:
    NacosString ip;
    int64_t port;
    int64_t version;
    NacosString serviceName;
    bool enable;
    int64_t weight;
    Protocol protocol;
    int64_t svrId;
    NacosString envBranch;

    Endpoint();

    NacosString toString() const;
};

/**
 * NewEndpoint builds an Endpoint, mirroring the Go `service.NewEndpoint`
 * constructor (the variadic option arguments are exposed here as optional
 * parameters with the same default behaviour).
 *
 * @param serviceName name of the service the endpoint belongs to
 * @param ip          instance ip
 * @param port        instance port
 * @param weight      instance weight (<=0 keeps the value untouched here, the
 *                    Register layer normalises it to 100 like the Go code)
 * @param enable      whether the instance is enabled (accepts traffic)
 * @param protocol    application protocol of the instance
 * @param svrId       optional server id stored in metadata (0 = unset)
 * @param envBranch   optional environment branch stored in metadata ("" = unset)
 */
Endpoint NewEndpoint(const NacosString &serviceName, const NacosString &ip,
                     int64_t port, int64_t weight, bool enable, Protocol protocol,
                     int64_t svrId = 0, const NacosString &envBranch = "");

} /*naming*/ } /*nacos*/

#endif
