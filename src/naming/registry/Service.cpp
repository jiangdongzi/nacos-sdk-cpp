#include "naming/registry/Service.h"
#include "NacosString.h"

namespace nacos { namespace naming {

NacosString Service::toString() const {
    NacosString result = "Service{serviceName:" + serviceName + " endpoints:[";
    for (std::vector<Endpoint>::const_iterator it = endpoints.begin();
         it != endpoints.end(); it++) {
        if (it != endpoints.begin()) {
            result += ", ";
        }
        result += it->toString();
    }
    result += "]}";
    return result;
}

} /*naming*/ } /*nacos*/
