#ifndef __NACOS_REGISTRY_SERVICE_H_
#define __NACOS_REGISTRY_SERVICE_H_

#include <vector>
#include "NacosString.h"
#include "naming/registry/Endpoint.h"

namespace nacos { namespace naming {

/**
 * Service is the aggregation of every Endpoint of a logical service.
 *
 * It mirrors the Go `service.Service` struct (the Go version stores
 * `[]*Endpoint`; here we use value semantics via std::vector for simplicity).
 */
class Service {
public:
    NacosString serviceName;
    std::vector<Endpoint> endpoints;

    NacosString toString() const;
};

} /*naming*/ } /*nacos*/

#endif
