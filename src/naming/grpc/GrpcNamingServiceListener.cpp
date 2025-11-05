#include "naming/grpc/GrpcNamingServiceListener.h"

#include <chrono>
#include <list>
#include <sstream>
#include <utility>

#include "grpcpp/grpcpp.h"
#include "src/json/rapidjson/document.h"

#include "NacosExceptions.h"
#include "constant/PropertyKeyConst.h"
#include "constant/UtilAndComs.h"
#include "nacos_grpc_service.grpc.pb.h"
#include "src/factory/ObjectConfigData.h"
#include "src/log/Logger.h"
#include "src/naming/subscribe/HostReactor.h"
#include "src/server/NacosServerInfo.h"
#include "src/server/ServerListManager.h"
#include "src/utils/UuidUtils.h"

namespace nacos {
namespace {
const int DEFAULT_HTTP_PORT = 8848;
const int GRPC_PORT_OFFSET = 1000;
const int SERVER_CHECK_TIMEOUT_MS = 5000;
const int UNARY_TIMEOUT_MS = 5000;
const int RECONNECT_DELAY_MS = 2000;

std::string makeRequestId() {
    return UuidUtils::generateUuid();
}

}

struct GrpcNamingServiceListener::GrpcState {
    std::shared_ptr<grpc::Channel> channel;
    std::unique_ptr<::Request::Stub> requestStub;
    std::unique_ptr<::BiRequestStream::Stub> streamStub;
    std::unique_ptr<grpc::ClientContext> streamContext;
    std::unique_ptr<grpc::ClientReaderWriter<::Payload, ::Payload>> stream;
    std::mutex streamMutex;
    std::mutex unaryMutex;
    std::string currentServer;
    std::string connectionId;
};

GrpcNamingServiceListener::GrpcNamingServiceListener(ObjectConfigData *objectConfigData)
        : _objectConfigData(objectConfigData), running(false), connectionReady(false) {
    state = new GrpcState();
}

GrpcNamingServiceListener::~GrpcNamingServiceListener() {
    stop();
    delete state;
}

void GrpcNamingServiceListener::start() {
    if (running.exchange(true)) {
        return;
    }
    workerThread = std::thread(&GrpcNamingServiceListener::run, this);
}

void GrpcNamingServiceListener::stop() {
    if (!running.exchange(false)) {
        return;
    }
    connectionReady.store(false);
    closeStream();
    if (workerThread.joinable()) {
        workerThread.join();
    }
}

void GrpcNamingServiceListener::subscribe(const NacosString &serviceName, const NacosString &groupName,
                                          const NacosString &clusters) {
    GrpcSubscriptionKey key{serviceName, groupName, clusters};
    bool inserted;
    {
        std::lock_guard<std::mutex> lock(subscriptionMutex);
        inserted = subscriptions.insert(key).second;
    }
    if (inserted && connectionReady.load()) {
        if (!sendSubscribeRequest(key, true)) {
            log_warn("[gRPC] subscribe request failed for %s#%s#%s\n",
                     serviceName.c_str(), groupName.c_str(), clusters.c_str());
        }
    }
}

void GrpcNamingServiceListener::unsubscribe(const NacosString &serviceName, const NacosString &groupName,
                                            const NacosString &clusters) {
    GrpcSubscriptionKey key{serviceName, groupName, clusters};
    bool removed;
    {
        std::lock_guard<std::mutex> lock(subscriptionMutex);
        removed = subscriptions.erase(key) > 0;
    }
    if (removed && connectionReady.load()) {
        if (!sendSubscribeRequest(key, false)) {
            log_warn("[gRPC] unsubscribe request failed for %s#%s#%s\n",
                     serviceName.c_str(), groupName.c_str(), clusters.c_str());
        }
    }
}

void GrpcNamingServiceListener::run() {
    while (running.load()) {
        if (!establishConnection()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(RECONNECT_DELAY_MS));
            continue;
        }
        connectionReady.store(true);
        resubscribeAll();

        while (running.load()) {
            ::Payload inbound;
            {
                std::lock_guard<std::mutex> lock(state->streamMutex);
                if (!state->stream) {
                    break;
                }
                if (!state->stream->Read(&inbound)) {
                    log_warn("[gRPC] stream closed from server %s\n", state->currentServer.c_str());
                    break;
                }
            }
            handlePayload(inbound);
        }

        connectionReady.store(false);
        closeStream();
        std::this_thread::sleep_for(std::chrono::milliseconds(RECONNECT_DELAY_MS));
    }
}

bool GrpcNamingServiceListener::establishConnection() {
    closeStream();

    if (_objectConfigData->_serverListManager == NULL) {
        log_warn("[gRPC] server list manager not initialized\n");
        return false;
    }

    std::list<NacosServerInfo> servers = _objectConfigData->_serverListManager->getServerList();
    if (servers.empty()) {
        log_warn("[gRPC] no nacos server available\n");
        return false;
    }

    for (const auto &server : servers) {
        std::string ip = server.getIp();
        int httpPort = server.getPort();
        if (httpPort <= 0) {
            httpPort = DEFAULT_HTTP_PORT;
        }
        int grpcPort = httpPort + GRPC_PORT_OFFSET;
        std::ostringstream oss;
        oss << ip << ":" << grpcPort;
        std::string address = oss.str();

        log_info("[gRPC] dialing %s\n", address.c_str());

        state->channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
        state->requestStub = ::Request::NewStub(state->channel);
        state->streamStub = ::BiRequestStream::NewStub(state->channel);
        state->currentServer = address;

        std::string connectionId;
        if (!performServerCheck(connectionId)) {
            log_warn("[gRPC] server check failed for %s\n", address.c_str());
            continue;
        }

        if (!sendConnectionSetup(connectionId)) {
            log_warn("[gRPC] connection setup failed for %s\n", address.c_str());
            continue;
        }

        log_info("[gRPC] connected to %s with connectionId=%s\n", address.c_str(), connectionId.c_str());
        return true;
    }

    log_warn("[gRPC] failed to connect to any server\n");
    return false;
}

void GrpcNamingServiceListener::closeStream() {
    std::lock_guard<std::mutex> lock(state->streamMutex);
    if (state->stream) {
        state->stream->WritesDone();
        state->stream.reset();
    }
    if (state->streamContext) {
        state->streamContext->TryCancel();
        state->streamContext.reset();
    }
    state->connectionId.clear();
}

void GrpcNamingServiceListener::resubscribeAll() {
    std::set<GrpcSubscriptionKey> snapshot;
    {
        std::lock_guard<std::mutex> lock(subscriptionMutex);
        snapshot = subscriptions;
    }

    for (const auto &key : snapshot) {
        if (!sendSubscribeRequest(key, true)) {
            log_warn("[gRPC] failed to resubscribe %s#%s#%s\n",
                     key.serviceName.c_str(), key.groupName.c_str(), key.clusters.c_str());
        }
    }
}

bool GrpcNamingServiceListener::sendSubscribeRequest(const GrpcSubscriptionKey &key, bool subscribeFlag) {
    std::ostringstream body;
    body << "{\"requestId\":\"" << makeRequestId() << "\",";
    body << "\"module\":\"naming\",";
    body << "\"namespace\":\"" << _objectConfigData->_serverListManager->getNamespace() << "\",";
    body << "\"serviceName\":\"" << key.serviceName << "\",";
    body << "\"groupName\":\"" << key.groupName << "\",";
    body << "\"clusters\":\"" << key.clusters << "\",";
    body << "\"subscribe\":" << (subscribeFlag ? "true" : "false") << "}";

    std::string response;
    if (!callUnary("SubscribeServiceRequest", body.str(), response)) {
        return false;
    }
    log_debug("[gRPC] subscribe response: %s\n", response.c_str());
    return true;
}

bool GrpcNamingServiceListener::performServerCheck(std::string &connectionId) {
    std::ostringstream body;
    body << "{\"requestId\":\"" << makeRequestId() << "\",\"module\":\"internal\"}";

    std::string response;
    if (!callUnary("ServerCheckRequest", body.str(), response)) {
        return false;
    }

    rapidjson::Document doc;
    doc.Parse(response.c_str());
    if (!doc.IsObject()) {
        log_warn("[gRPC] invalid server check response: %s\n", response.c_str());
        return false;
    }

    if (doc.HasMember("errorCode") && doc["errorCode"].IsInt()) {
        int errorCode = doc["errorCode"].GetInt();
        if (errorCode >= 300 && errorCode < 400) {
            log_warn("[gRPC] server not ready (errorCode=%d)\n", errorCode);
            return false;
        }
    }

    if (!doc.HasMember("connectionId") || !doc["connectionId"].IsString()) {
        log_warn("[gRPC] missing connectionId in server check response\n");
        return false;
    }

    connectionId = doc["connectionId"].GetString();
    state->connectionId = connectionId;
    return true;
}

bool GrpcNamingServiceListener::sendConnectionSetup(const std::string &connectionId) {
    state->streamContext.reset(new grpc::ClientContext());
    state->streamContext->set_wait_for_ready(true);

    {
        std::lock_guard<std::mutex> lock(state->streamMutex);
        state->stream = state->streamStub->requestBiStream(state->streamContext.get());
        if (!state->stream) {
            return false;
        }
    }

    NacosString tenant = _objectConfigData->_serverListManager->getNamespace();

    std::ostringstream labels;
    labels << "{\"module\":\"naming\"}";

    std::ostringstream body;
    body << "{\"requestId\":\"" << makeRequestId() << "\",";
    body << "\"module\":\"internal\",";
    body << "\"clientVersion\":\"" << UtilAndComs::VERSION << "\",";
    body << "\"tenant\":\"" << tenant << "\",";
    body << "\"labels\":" << labels.str() << ",";
    body << "\"clientAbilities\":{}";
    body << "}";

    return sendStreamAck("ConnectionSetupRequest", body.str());
}

void GrpcNamingServiceListener::handlePayload(const ::Payload &payload) {
    const std::string &type = payload.metadata().type();
    const std::string &rawBody = payload.body().value();

    log_debug("[gRPC] incoming payload type=%s body=%s\n", type.c_str(), rawBody.c_str());

    if (type == "NotifySubscriberRequest") {
        try {
            _objectConfigData->_hostReactor->processServiceJson(rawBody);
        } catch (NacosException &e) {
            log_warn("[gRPC] processServiceJson failed: %s\n", e.what());
        }

        rapidjson::Document doc;
        doc.Parse(rawBody.c_str());
        std::string requestId;
        if (doc.IsObject() && doc.HasMember("requestId") && doc["requestId"].IsString()) {
            requestId = doc["requestId"].GetString();
        }
        std::ostringstream ack;
        ack << "{\"resultCode\":200,\"errorCode\":0,\"success\":true,\"message\":\"\",\"requestId\":\""
            << requestId << "\"}";
        sendStreamAck("NotifySubscriberResponse", ack.str());
        return;
    }

    if (type == "ClientDetectionRequest") {
        rapidjson::Document doc;
        doc.Parse(rawBody.c_str());
        std::string requestId;
        if (doc.IsObject() && doc.HasMember("requestId") && doc["requestId"].IsString()) {
            requestId = doc["requestId"].GetString();
        }
        std::ostringstream ack;
        ack << "{\"resultCode\":200,\"errorCode\":0,\"success\":true,\"message\":\"\",\"requestId\":\""
            << requestId << "\"}";
        sendStreamAck("ClientDetectionResponse", ack.str());
    }
}

bool GrpcNamingServiceListener::sendStreamAck(const std::string &type, const std::string &body) {
    std::lock_guard<std::mutex> lock(state->streamMutex);
    if (!state->stream) {
        return false;
    }

    ::Payload payload;
    auto *metadata = payload.mutable_metadata();
    metadata->set_type(type);
    metadata->set_clientip("");

    NacosString appName = resolveAppName();
    (*metadata->mutable_headers())["app"] = appName.c_str();
    (*metadata->mutable_headers())["Client-Version"] = UtilAndComs::VERSION.c_str();
    (*metadata->mutable_headers())["User-Agent"] = UtilAndComs::VERSION.c_str();

    payload.mutable_body()->set_value(body);
    payload.mutable_body()->set_type_url("");

    return state->stream->Write(payload);
}

bool GrpcNamingServiceListener::callUnary(const std::string &type, const std::string &body, std::string &responseBody) {
    std::lock_guard<std::mutex> lock(state->unaryMutex);
    if (!state->requestStub) {
        return false;
    }

    ::Payload requestPayload;
    auto *metadata = requestPayload.mutable_metadata();
    metadata->set_type(type);
    metadata->set_clientip("");
    NacosString appName = resolveAppName();
    (*metadata->mutable_headers())["app"] = appName.c_str();
    (*metadata->mutable_headers())["Client-Version"] = UtilAndComs::VERSION.c_str();
    (*metadata->mutable_headers())["User-Agent"] = UtilAndComs::VERSION.c_str();
    requestPayload.mutable_body()->set_value(body);
    requestPayload.mutable_body()->set_type_url("");

    ::Payload responsePayload;
    grpc::ClientContext context;
    auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(UNARY_TIMEOUT_MS);
    context.set_deadline(deadline);

    grpc::Status status = state->requestStub->request(&context, requestPayload, &responsePayload);
    if (!status.ok()) {
        log_warn("[gRPC] unary call failed: code=%d message=%s\n", status.error_code(), status.error_message().c_str());
        return false;
    }

    responseBody = responsePayload.body().value();
    return true;
}

NacosString GrpcNamingServiceListener::resolveAppName() const {
    if (_objectConfigData->_appConfigManager == NULL) {
        return NacosString();
    }
    return _objectConfigData->_appConfigManager->get(PropertyKeyConst::APP_NAME);
}

} // namespace nacos
