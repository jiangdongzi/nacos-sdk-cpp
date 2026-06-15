#include <iostream>
#include <string>
#include <unistd.h>
#include "Nacos.h"
#include "naming/registry/Naming.h"

using namespace std;
using namespace nacos;
using namespace nacos::naming;

static void printService(const Service &svr) {
    cout << "  service:" << svr.serviceName << " endpoints(" << svr.endpoints.size() << "):" << endl;
    for (size_t i = 0; i < svr.endpoints.size(); i++) {
        cout << "    - " << svr.endpoints[i].toString() << endl;
    }
}

/**
 * Demonstrates the high level Discovery / Register facade that mirrors the Go
 * `naming` module.
 *
 * Usage: discovery-register-demo.out [server_addr] [namespace]
 *   server_addr defaults to 127.0.0.1:8848
 *   namespace   defaults to "" (the public namespace)
 */
int main(int argc, char **argv) {
    NacosString addr = (argc > 1) ? argv[1] : "127.0.0.1:8848";
    NacosString ns = (argc > 2) ? argv[2] : "";

    const NacosString serviceName = "registry-demo-service";
    const NacosString ip = "127.0.0.1";
    const int64_t port = 8888;

    //Use the v2 open-API for enable/disable (works with Nacos 2.x).
    Register *reg = NewRegister(ns, addr, API_VERSION_NEW);
    ResourceGuard<Register> _guardReg(reg);

    Discovery *disc = NewDiscovery(ns, addr);
    ResourceGuard<Discovery> _guardDisc(disc);

    try {
        cout << "== registerCallback(" << serviceName << ") ==" << endl;
        disc->registerCallback(serviceName, [](const Service &svr) {
            cout << "[callback] service changed:" << endl;
            printService(svr);
        });

        cout << "== addEndpoint ==" << endl;
        Endpoint ep = NewEndpoint(serviceName, ip, port, 100, true, ProtocolGRPC,
                                  /*svrId*/ 1001, /*envBranch*/ "main");
        reg->addEndpoint(ep);
        sleep(3);

        cout << "== getEnableEndpoint ==" << endl;
        Endpoint enabled = disc->getEnableEndpoint(serviceName);
        cout << "  " << enabled.toString() << endl;

        cout << "== getService ==" << endl;
        Service svr = disc->getService(serviceName);
        printService(svr);

        cout << "== disableEndpoint ==" << endl;
        reg->disableEndpoint(serviceName, ip, port, ProtocolGRPC);
        sleep(3);
        cout << "  after disable, getService:" << endl;
        printService(disc->getService(serviceName));

        try {
            Endpoint afterDisable = disc->getEnableEndpoint(serviceName);
            cout << "  unexpected enabled endpoint:" << afterDisable.toString() << endl;
        } catch (NacosException &e) {
            cout << "  getEnableEndpoint after disable -> no instance (expected): " << e.what() << endl;
        }

        cout << "== enableEndpoint ==" << endl;
        reg->enableEndpoint(serviceName, ip, port, ProtocolGRPC);
        sleep(3);
        cout << "  after enable, getEnableEndpoint:" << endl;
        cout << "  " << disc->getEnableEndpoint(serviceName).toString() << endl;

        cout << "== delEndpoint ==" << endl;
        reg->delEndpoint(serviceName, ip, port, ProtocolGRPC);
        sleep(3);
        cout << "  after del, getService:" << endl;
        printService(disc->getService(serviceName));
    }
    catch (NacosException &e) {
        cout << "demo failed, errorcode:" << e.errorcode() << " reason:" << e.what() << endl;
        return -1;
    }

    cout << "== demo finished ==" << endl;
    return 0;
}
