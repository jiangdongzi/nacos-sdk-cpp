#ifndef __NACOS_REGISTRY_PROTOCOL_H_
#define __NACOS_REGISTRY_PROTOCOL_H_

namespace nacos { namespace naming {

/**
 * Protocol of an endpoint.
 *
 * Mirrors the Go `service.Protocol` enum. The underlying integer value is what
 * gets stored in the instance metadata under the "protocol" key, so the values
 * MUST stay wire-compatible with the Go implementation.
 */
enum Protocol {
    ProtocolUnknown = 0,
    ProtocolHTTP = 1,
    ProtocolGRPC = 2,
    ProtocolTCP = 3
};

} /*naming*/ } /*nacos*/

#endif
