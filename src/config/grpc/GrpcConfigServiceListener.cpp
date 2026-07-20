#include "config/grpc/GrpcConfigServiceListener.h"

#include <chrono>
#include <cstddef>
#include <list>
#include <sstream>
#include <utility>

#include "grpcpp/grpcpp.h"
#include "src/json/rapidjson/document.h"
#include "src/json/rapidjson/stringbuffer.h"
#include "src/json/rapidjson/writer.h"

#include "constant/ConfigConstant.h"
#include "constant/PropertyKeyConst.h"
#include "constant/UtilAndComs.h"
#include "listen/Listener.h"
#include "src/config/AppConfigManager.h"
#include "src/crypto/md5/md5.h"
#include "src/factory/ObjectConfigData.h"
#include "src/listen/ClientWorker.h"
#include "src/log/Logger.h"
#include "src/security/SecurityManager.h"
#include "src/server/NacosServerInfo.h"
#include "src/server/ServerListManager.h"
#include "src/utils/NetUtils.h"
#include "src/utils/UuidUtils.h"

namespace nacos {

namespace {
const int DEFAULT_HTTP_PORT = 8848;
const int GRPC_PORT_OFFSET = 1000;
const int UNARY_TIMEOUT_MS = 5000;
const int RECONNECT_DELAY_MS = 2000;
const int DEFAULT_HEALTH_CHECK_TIMEOUT_MS = 3000;
const int DEFAULT_KEEPALIVE_INTERVAL_MS = 5000;
const long long FULL_SYNC_INTERVAL_MS = 180000;

std::string makeRequestId() {
    return UuidUtils::generateUuid();
}

long long steadyMillis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

void addStringMember(rapidjson::Value &object, const char *name, const std::string &value,
                     rapidjson::Document::AllocatorType &allocator) {
    rapidjson::Value jsonValue;
    jsonValue.SetString(value.c_str(), static_cast<rapidjson::SizeType>(value.size()), allocator);
    object.AddMember(rapidjson::StringRef(name), jsonValue, allocator);
}

std::string jsonToString(const rapidjson::Document &document) {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    document.Accept(writer);
    return buffer.GetString();
}

std::string getJsonString(const rapidjson::Value &object, const char *name) {
    if (!object.IsObject() || !object.HasMember(name) || object[name].IsNull() || !object[name].IsString()) {
        return "";
    }
    return object[name].GetString();
}

int getJsonInt(const rapidjson::Value &object, const char *name, int defaultValue) {
    if (!object.IsObject() || !object.HasMember(name)) {
        return defaultValue;
    }
    const rapidjson::Value &value = object[name];
    if (value.IsInt()) {
        return value.GetInt();
    }
    if (value.IsInt64()) {
        return static_cast<int>(value.GetInt64());
    }
    return defaultValue;
}

bool responseSucceeded(const rapidjson::Document &document) {
    if (!document.IsObject()) {
        return false;
    }
    if (document.HasMember("success") && document["success"].IsBool()) {
        return document["success"].GetBool();
    }
    return getJsonInt(document, "resultCode", 0) == 200 && getJsonInt(document, "errorCode", 0) == 0;
}

NacosString normalizeTenant(const NacosString &tenant) {
    if (tenant.empty() || tenant == "public" || tenant == "Public") {
        return "";
    }
    return tenant;
}

GrpcConfigSubscriptionKey makeKey(const NacosString &dataId, const NacosString &group,
                                  const NacosString &tenant) {
    GrpcConfigSubscriptionKey key;
    key.dataId = dataId;
    key.group = group.empty() ? ConfigConstant::DEFAULT_GROUP : group;
    key.tenant = normalizeTenant(tenant);
    return key;
}

NacosString contentMd5(const NacosString &content) {
    MD5 md5;
    md5.update(content);
    return md5.toString();
}
} // namespace

struct GrpcConfigServiceListener::GrpcState {
    std::shared_ptr<grpc::Channel> channel;
    std::unique_ptr<::Request::Stub> requestStub;
    std::unique_ptr<::BiRequestStream::Stub> streamStub;
    std::unique_ptr<grpc::ClientContext> streamContext;
    std::unique_ptr<grpc::ClientReaderWriter<::Payload, ::Payload> > stream;
    std::mutex streamMutex;
    std::mutex unaryMutex;
    std::mutex connectionMutex;
    std::string currentServer;
    std::string connectionId;
};

GrpcConfigServiceListener::GrpcConfigServiceListener(ObjectConfigData *objectConfigData)
    : _objectConfigData(objectConfigData), running(false), connectionReady(false), stopHealth(false),
      healthRunning(false), lastActiveMillis(0), lastFullSyncMillis(0),
      keepaliveIntervalMs(DEFAULT_KEEPALIVE_INTERVAL_MS),
      healthTimeoutMs(DEFAULT_HEALTH_CHECK_TIMEOUT_MS), state(new GrpcState()) {
    if (_objectConfigData != NULL && _objectConfigData->_appConfigManager != NULL) {
        const NacosString &keepalive =
            _objectConfigData->_appConfigManager->get(PropertyKeyConst::GRPC_KEEPALIVE_INTERVAL);
        if (!keepalive.empty() && atoi(keepalive.c_str()) > 0) {
            keepaliveIntervalMs = atoi(keepalive.c_str());
        }
        const NacosString &timeout =
            _objectConfigData->_appConfigManager->get(PropertyKeyConst::GRPC_HEALTHCHECK_TIMEOUT);
        if (!timeout.empty() && atoi(timeout.c_str()) > 0) {
            healthTimeoutMs = atoi(timeout.c_str());
        }
    }

    try {
        clientIp = NetUtils::getHostIp().c_str();
    } catch (...) {
        clientIp = "127.0.0.1";
    }
    if (clientIp.empty()) {
        clientIp = "127.0.0.1";
    }
}

GrpcConfigServiceListener::~GrpcConfigServiceListener() {
    stop();
    delete state;
}

void GrpcConfigServiceListener::start() {
    if (running.exchange(true)) {
        return;
    }
    workerThread = std::thread(&GrpcConfigServiceListener::run, this);
    startHealthLoop();
}

void GrpcConfigServiceListener::stop() {
    if (!running.exchange(false)) {
        return;
    }
    connectionReady.store(false);
    healthCv.notify_all();
    {
        std::lock_guard<std::mutex> lock(state->streamMutex);
        if (state->streamContext) {
            state->streamContext->TryCancel();
        }
    }
    stopHealthLoop();
    if (workerThread.joinable()) {
        workerThread.join();
    }
    closeStream();
}

void GrpcConfigServiceListener::addListener(const NacosString &dataId, const NacosString &group,
                                            const NacosString &tenant, const NacosString &initialContent,
                                            Listener *listener) {
    GrpcConfigSubscriptionKey key = makeKey(dataId, group, tenant);
    ListenSnapshot snapshot;
    bool created = false;
    {
        std::lock_guard<std::mutex> lock(subscriptionMutex);
        std::map<GrpcConfigSubscriptionKey, SubscriptionState>::iterator it = subscriptions.find(key);
        if (it == subscriptions.end()) {
            SubscriptionState newState;
            newState.md5 = initialContent.empty() ? "" : contentMd5(initialContent);
            newState.listeners.insert(listener);
            subscriptions[key] = newState;
            it = subscriptions.find(key);
            created = true;
        } else {
            it->second.listeners.insert(listener);
        }
        snapshot.key = key;
        snapshot.md5 = it->second.md5;
        snapshot.generation = it->second.generation;
    }

    if (created && connectionReady.load()) {
        std::vector<ListenSnapshot> one(1, snapshot);
        sendBatchListen(one, true, true);
    }
    healthCv.notify_all();
}

void GrpcConfigServiceListener::removeListener(const NacosString &dataId, const NacosString &group,
                                               const NacosString &tenant, Listener *listener) {
    GrpcConfigSubscriptionKey key = makeKey(dataId, group, tenant);
    ListenSnapshot removed;
    bool removeRemote = false;
    {
        std::lock_guard<std::mutex> lock(subscriptionMutex);
        std::map<GrpcConfigSubscriptionKey, SubscriptionState>::iterator it = subscriptions.find(key);
        if (it == subscriptions.end()) {
            return;
        }
        it->second.listeners.erase(listener);
        if (it->second.listeners.empty()) {
            removed.key = key;
            removed.md5 = it->second.md5;
            removed.generation = it->second.generation;
            subscriptions.erase(it);
            removeRemote = true;
        }
    }

    if (removeRemote && connectionReady.load()) {
        std::vector<ListenSnapshot> one(1, removed);
        sendBatchListen(one, false, false);
    }
}

bool GrpcConfigServiceListener::isConnected() const {
    return connectionReady.load();
}

void GrpcConfigServiceListener::run() {
    while (running.load()) {
        if (!establishConnection()) {
            std::unique_lock<std::mutex> lock(healthMutex);
            healthCv.wait_for(lock, std::chrono::milliseconds(RECONNECT_DELAY_MS),
                              [this] { return !running.load(); });
            continue;
        }

        connectionReady.store(true);
        markAllDirty();
        std::vector<ListenSnapshot> all = snapshotAll();
        if (!all.empty()) {
            sendBatchListen(all, true, true);
        }
        lastFullSyncMillis.store(steadyMillis());
        markActivity();

        while (running.load()) {
            ::Payload inbound;
            std::unique_ptr<grpc::ClientReaderWriter<::Payload, ::Payload> > *streamPtr = NULL;
            {
                std::lock_guard<std::mutex> lock(state->streamMutex);
                if (!state->stream) {
                    break;
                }
                streamPtr = &state->stream;
            }
            if (!(*streamPtr)->Read(&inbound)) {
                log_warn("[Config gRPC] stream closed by server %s\n", state->currentServer.c_str());
                break;
            }
            handlePayload(inbound);
        }

        connectionReady.store(false);
        markAllDirty();
        closeStream();
        if (running.load()) {
            std::unique_lock<std::mutex> lock(healthMutex);
            healthCv.wait_for(lock, std::chrono::milliseconds(RECONNECT_DELAY_MS),
                              [this] { return !running.load(); });
        }
    }
}

bool GrpcConfigServiceListener::establishConnection() {
    closeStream();
    if (_objectConfigData == NULL || _objectConfigData->_serverListManager == NULL) {
        return false;
    }

    std::list<NacosServerInfo> servers = _objectConfigData->_serverListManager->getServerList();
    for (std::list<NacosServerInfo>::const_iterator it = servers.begin(); it != servers.end(); ++it) {
        std::string ip = it->getIp();
        const std::string httpPrefix = "http://";
        const std::string httpsPrefix = "https://";
        if (ip.compare(0, httpPrefix.size(), httpPrefix) == 0) {
            ip = ip.substr(httpPrefix.size());
        } else if (ip.compare(0, httpsPrefix.size(), httpsPrefix) == 0) {
            ip = ip.substr(httpsPrefix.size());
            log_warn("[Config gRPC] TLS is not configured; using an insecure channel\n");
        }
        if (!ip.empty() && ip[ip.size() - 1] == '/') {
            ip.erase(ip.size() - 1);
        }

        int httpPort = it->getPort() > 0 ? it->getPort() : DEFAULT_HTTP_PORT;
        std::ostringstream address;
        address << ip << ":" << httpPort + GRPC_PORT_OFFSET;

        {
            std::lock_guard<std::mutex> lock(state->unaryMutex);
            state->channel = grpc::CreateChannel(address.str(), grpc::InsecureChannelCredentials());
            state->requestStub = ::Request::NewStub(state->channel);
            state->streamStub = ::BiRequestStream::NewStub(state->channel);
        }
        state->currentServer = address.str();
        log_info("[Config gRPC] dialing %s\n", address.str().c_str());

        std::string connectionId;
        if (!performServerCheck(connectionId)) {
            log_warn("[Config gRPC] server check failed for %s\n", address.str().c_str());
            continue;
        }
        if (!sendConnectionSetup(connectionId)) {
            log_warn("[Config gRPC] connection setup failed for %s\n", address.str().c_str());
            continue;
        }

        log_info("[Config gRPC] connected to %s with connectionId=%s\n", address.str().c_str(),
                 connectionId.c_str());
        return true;
    }
    return false;
}

void GrpcConfigServiceListener::closeStream() {
    {
        std::lock_guard<std::mutex> lock(state->streamMutex);
        if (state->streamContext) {
            state->streamContext->TryCancel();
        }
        if (state->stream) {
            state->stream->WritesDone();
            state->stream.reset();
        }
        state->streamContext.reset();
    }
    {
        std::lock_guard<std::mutex> lock(state->connectionMutex);
        state->connectionId.clear();
    }
}

void GrpcConfigServiceListener::markAllDirty() {
    std::lock_guard<std::mutex> lock(subscriptionMutex);
    for (std::map<GrpcConfigSubscriptionKey, SubscriptionState>::iterator it = subscriptions.begin();
         it != subscriptions.end(); ++it) {
        it->second.dirty = true;
        ++it->second.generation;
    }
}

std::vector<GrpcConfigServiceListener::ListenSnapshot> GrpcConfigServiceListener::snapshotAll() {
    std::vector<ListenSnapshot> result;
    std::lock_guard<std::mutex> lock(subscriptionMutex);
    for (std::map<GrpcConfigSubscriptionKey, SubscriptionState>::const_iterator it = subscriptions.begin();
         it != subscriptions.end(); ++it) {
        ListenSnapshot item;
        item.key = it->first;
        item.md5 = it->second.md5;
        item.generation = it->second.generation;
        result.push_back(item);
    }
    return result;
}

std::vector<GrpcConfigServiceListener::ListenSnapshot> GrpcConfigServiceListener::snapshotDirty() {
    std::vector<ListenSnapshot> result;
    std::lock_guard<std::mutex> lock(subscriptionMutex);
    for (std::map<GrpcConfigSubscriptionKey, SubscriptionState>::const_iterator it = subscriptions.begin();
         it != subscriptions.end(); ++it) {
        if (!it->second.dirty) {
            continue;
        }
        ListenSnapshot item;
        item.key = it->first;
        item.md5 = it->second.md5;
        item.generation = it->second.generation;
        result.push_back(item);
    }
    return result;
}

bool GrpcConfigServiceListener::sendBatchListen(const std::vector<ListenSnapshot> &snapshot, bool listen,
                                                bool processChanges) {
    if (snapshot.empty() || !connectionReady.load()) {
        return snapshot.empty();
    }

    rapidjson::Document request;
    request.SetObject();
    rapidjson::Document::AllocatorType &allocator = request.GetAllocator();
    addStringMember(request, "requestId", makeRequestId(), allocator);
    addStringMember(request, "module", "config", allocator);
    request.AddMember(rapidjson::StringRef("listen"), listen, allocator);

    rapidjson::Value contexts(rapidjson::kArrayType);
    for (std::vector<ListenSnapshot>::const_iterator it = snapshot.begin(); it != snapshot.end(); ++it) {
        rapidjson::Value context(rapidjson::kObjectType);
        addStringMember(context, "dataId", it->key.dataId, allocator);
        addStringMember(context, "group", it->key.group, allocator);
        addStringMember(context, "tenant", it->key.tenant, allocator);
        addStringMember(context, "md5", it->md5, allocator);
        contexts.PushBack(context, allocator);
    }
    request.AddMember(rapidjson::StringRef("configListenContexts"), contexts, allocator);

    std::string responseBody;
    if (!callUnary("ConfigBatchListenRequest", jsonToString(request), responseBody, UNARY_TIMEOUT_MS)) {
        return false;
    }
    markActivity();
    log_debug("[Config gRPC] ConfigChangeBatchListenResponse: %s\n", responseBody.c_str());

    rapidjson::Document response;
    response.Parse(responseBody.c_str());
    if (!responseSucceeded(response)) {
        log_warn("[Config gRPC] batch listen failed: %s\n", responseBody.c_str());
        return false;
    }
    if (!listen) {
        return true;
    }

    std::set<GrpcConfigSubscriptionKey> changedKeys;
    if (response.HasMember("changedConfigs") && response["changedConfigs"].IsArray()) {
        const rapidjson::Value &changed = response["changedConfigs"];
        for (rapidjson::SizeType i = 0; i < changed.Size(); ++i) {
            const rapidjson::Value &item = changed[i];
            changedKeys.insert(makeKey(getJsonString(item, "dataId"), getJsonString(item, "group"),
                                       getJsonString(item, "tenant")));
        }
    }

    bool refreshed = false;
    for (std::vector<ListenSnapshot>::const_iterator it = snapshot.begin(); it != snapshot.end(); ++it) {
        if (changedKeys.find(it->key) == changedKeys.end()) {
            markSnapshotClean(*it);
            continue;
        }
        if (processChanges && refreshConfig(it->key, it->generation, !isInitializing(it->key))) {
            refreshed = true;
        }
    }

    if (refreshed) {
        std::vector<ListenSnapshot> updated = snapshotDirty();
        if (!updated.empty()) {
            sendBatchListen(updated, true, false);
        }
    }
    return true;
}

bool GrpcConfigServiceListener::refreshConfig(const GrpcConfigSubscriptionKey &key,
                                              unsigned long expectedGeneration, bool notifyUser) {
    for (int attempt = 0; attempt < 3; ++attempt) {
        rapidjson::Document request;
        request.SetObject();
        rapidjson::Document::AllocatorType &allocator = request.GetAllocator();
        addStringMember(request, "requestId", makeRequestId(), allocator);
        addStringMember(request, "module", "config", allocator);
        addStringMember(request, "dataId", key.dataId, allocator);
        addStringMember(request, "group", key.group, allocator);
        addStringMember(request, "tenant", key.tenant, allocator);
        addStringMember(request, "tag", "", allocator);

        std::string responseBody;
        if (!callUnary("ConfigQueryRequest", jsonToString(request), responseBody, UNARY_TIMEOUT_MS,
                       notifyUser)) {
            return false;
        }
        markActivity();

        rapidjson::Document response;
        response.Parse(responseBody.c_str());
        bool exists = responseSucceeded(response);
        int errorCode = getJsonInt(response, "errorCode", 0);
        if (!exists && errorCode == 400 && attempt < 2) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }
        if (!exists && errorCode != 300) {
            log_warn("[Config gRPC] query failed for %s@@%s: %s\n", key.group.c_str(),
                     key.dataId.c_str(), responseBody.c_str());
            return false;
        }

        NacosString content = exists ? getJsonString(response, "content") : "";
        NacosString md5 = exists ? getJsonString(response, "md5") : "";
        if (exists && md5.empty()) {
            md5 = contentMd5(content);
        }

        _objectConfigData->_clientWorker->applyGrpcConfigChange(
            key.dataId, key.group, key.tenant, content, md5, exists, notifyUser);
        updateSubscriptionAfterQuery(key, md5, expectedGeneration);
        log_info("[Config gRPC] refreshed dataId=%s group=%s tenant=%s md5=%s\n", key.dataId.c_str(),
                 key.group.c_str(), key.tenant.c_str(), md5.c_str());
        return true;
    }
    return false;
}

void GrpcConfigServiceListener::updateSubscriptionAfterQuery(const GrpcConfigSubscriptionKey &key,
                                                             const NacosString &md5,
                                                             unsigned long expectedGeneration) {
    std::lock_guard<std::mutex> lock(subscriptionMutex);
    std::map<GrpcConfigSubscriptionKey, SubscriptionState>::iterator it = subscriptions.find(key);
    if (it == subscriptions.end()) {
        return;
    }
    it->second.md5 = md5;
    it->second.initializing = false;
    it->second.dirty = true;
    if (it->second.generation == expectedGeneration) {
        ++it->second.generation;
    }
}

void GrpcConfigServiceListener::markSnapshotClean(const ListenSnapshot &snapshot) {
    std::lock_guard<std::mutex> lock(subscriptionMutex);
    std::map<GrpcConfigSubscriptionKey, SubscriptionState>::iterator it =
        subscriptions.find(snapshot.key);
    if (it == subscriptions.end()) {
        return;
    }
    if (it->second.generation == snapshot.generation && it->second.md5 == snapshot.md5) {
        it->second.dirty = false;
        it->second.initializing = false;
    }
}

bool GrpcConfigServiceListener::isInitializing(const GrpcConfigSubscriptionKey &key) {
    std::lock_guard<std::mutex> lock(subscriptionMutex);
    std::map<GrpcConfigSubscriptionKey, SubscriptionState>::const_iterator it = subscriptions.find(key);
    return it != subscriptions.end() && it->second.initializing;
}

bool GrpcConfigServiceListener::performServerCheck(std::string &connectionId) {
    rapidjson::Document request;
    request.SetObject();
    rapidjson::Document::AllocatorType &allocator = request.GetAllocator();
    addStringMember(request, "requestId", makeRequestId(), allocator);
    addStringMember(request, "module", "internal", allocator);

    std::string responseBody;
    if (!callUnary("ServerCheckRequest", jsonToString(request), responseBody, UNARY_TIMEOUT_MS)) {
        return false;
    }

    rapidjson::Document response;
    response.Parse(responseBody.c_str());
    connectionId = getJsonString(response, "connectionId");
    if (connectionId.empty()) {
        return false;
    }
    {
        std::lock_guard<std::mutex> lock(state->connectionMutex);
        state->connectionId = connectionId;
    }
    return true;
}

bool GrpcConfigServiceListener::sendConnectionSetup(const std::string &) {
    state->streamContext.reset(new grpc::ClientContext());
    state->streamContext->set_wait_for_ready(true);
    {
        std::lock_guard<std::mutex> lock(state->streamMutex);
        state->stream = state->streamStub->requestBiStream(state->streamContext.get());
        if (!state->stream) {
            return false;
        }
    }

    rapidjson::Document request;
    request.SetObject();
    rapidjson::Document::AllocatorType &allocator = request.GetAllocator();
    addStringMember(request, "requestId", makeRequestId(), allocator);
    addStringMember(request, "module", "internal", allocator);
    addStringMember(request, "clientVersion", UtilAndComs::VERSION, allocator);
    addStringMember(request, "tenant", effectiveNamespace(), allocator);

    rapidjson::Value labels(rapidjson::kObjectType);
    addStringMember(labels, "module", "config", allocator);
    addStringMember(labels, "source", "sdk", allocator);
    addStringMember(labels, "taskId", "0", allocator);
    request.AddMember(rapidjson::StringRef("labels"), labels, allocator);
    rapidjson::Value abilities(rapidjson::kObjectType);
    request.AddMember(rapidjson::StringRef("clientAbilities"), abilities, allocator);

    if (!sendStreamPayload("ConnectionSetupRequest", jsonToString(request))) {
        return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    return true;
}

void GrpcConfigServiceListener::handlePayload(const ::Payload &payload) {
    const std::string &type = payload.metadata().type();
    const std::string &rawBody = payload.body().value();
    log_debug("[Config gRPC] incoming payload type=%s body=%s\n", type.c_str(), rawBody.c_str());
    markActivity();

    rapidjson::Document request;
    request.Parse(rawBody.c_str());
    const std::string requestId = getJsonString(request, "requestId");

    if (type == "ConfigChangeNotifyRequest") {
        GrpcConfigSubscriptionKey key =
            makeKey(getJsonString(request, "dataId"), getJsonString(request, "group"),
                    getJsonString(request, "tenant"));
        ListenSnapshot changed;
        bool found = false;
        {
            std::lock_guard<std::mutex> lock(subscriptionMutex);
            std::map<GrpcConfigSubscriptionKey, SubscriptionState>::iterator it = subscriptions.find(key);
            if (it != subscriptions.end()) {
                it->second.dirty = true;
                ++it->second.generation;
                changed.key = key;
                changed.md5 = it->second.md5;
                changed.generation = it->second.generation;
                found = true;
            }
        }

        std::ostringstream ack;
        ack << "{\"resultCode\":200,\"errorCode\":0,\"success\":true,\"message\":\"\","
               "\"requestId\":\""
            << requestId << "\"}";
        sendStreamPayload("ConfigChangeNotifyResponse", ack.str());

        if (found) {
            log_info("[Config gRPC] server notified dataId=%s group=%s tenant=%s\n", key.dataId.c_str(),
                     key.group.c_str(), key.tenant.c_str());
            std::vector<ListenSnapshot> one(1, changed);
            sendBatchListen(one, true, true);
        }
        return;
    }

    if (type == "ClientDetectionRequest") {
        std::ostringstream ack;
        ack << "{\"resultCode\":200,\"errorCode\":0,\"success\":true,\"message\":\"\","
               "\"requestId\":\""
            << requestId << "\"}";
        sendStreamPayload("ClientDetectionResponse", ack.str());
    }
}

bool GrpcConfigServiceListener::sendStreamPayload(const std::string &type, const std::string &body) {
    std::lock_guard<std::mutex> lock(state->streamMutex);
    if (!state->stream) {
        return false;
    }
    ::Payload payload;
    payload.mutable_metadata()->set_type(type);
    payload.mutable_metadata()->set_clientip(clientIp);
    addCommonHeaders(payload.mutable_metadata());
    payload.mutable_body()->set_value(body);
    payload.mutable_body()->set_type_url("");
    return state->stream->Write(payload);
}

bool GrpcConfigServiceListener::callUnary(const std::string &type, const std::string &body,
                                          std::string &responseBody, int timeoutMs, bool notifyQuery) {
    std::lock_guard<std::mutex> lock(state->unaryMutex);
    if (!state->requestStub) {
        return false;
    }

    ::Payload request;
    request.mutable_metadata()->set_type(type);
    request.mutable_metadata()->set_clientip(clientIp);
    addCommonHeaders(request.mutable_metadata());
    if (type == "ConfigQueryRequest") {
        (*request.mutable_metadata()->mutable_headers())["notify"] = notifyQuery ? "true" : "false";
    }
    request.mutable_body()->set_value(body);
    request.mutable_body()->set_type_url("");

    ::Payload response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(timeoutMs));
    grpc::Status status = state->requestStub->request(&context, request, &response);
    if (!status.ok()) {
        log_warn("[Config gRPC] unary %s failed: code=%d message=%s\n", type.c_str(),
                 status.error_code(), status.error_message().c_str());
        return false;
    }
    responseBody = response.body().value();
    return true;
}

NacosString GrpcConfigServiceListener::effectiveNamespace() const {
    if (_objectConfigData == NULL || _objectConfigData->_serverListManager == NULL) {
        return "";
    }
    return normalizeTenant(_objectConfigData->_serverListManager->getNamespace());
}

NacosString GrpcConfigServiceListener::resolveAppName() const {
    if (_objectConfigData == NULL || _objectConfigData->_appConfigManager == NULL) {
        return "";
    }
    return _objectConfigData->_appConfigManager->get(PropertyKeyConst::APP_NAME);
}

void GrpcConfigServiceListener::addCommonHeaders(::Metadata *metadata) {
    (*metadata->mutable_headers())["app"] = resolveAppName();
    (*metadata->mutable_headers())["Client-AppName"] = resolveAppName();
    (*metadata->mutable_headers())["Client-Version"] = UtilAndComs::VERSION;
    (*metadata->mutable_headers())["User-Agent"] = UtilAndComs::VERSION;
    (*metadata->mutable_headers())["exConfigInfo"] = "true";
    (*metadata->mutable_headers())["charset"] = "UTF-8";
    {
        std::lock_guard<std::mutex> lock(state->connectionMutex);
        if (!state->connectionId.empty()) {
            (*metadata->mutable_headers())["connectionId"] = state->connectionId;
        }
    }
    if (_objectConfigData != NULL && _objectConfigData->_securityManager != NULL) {
        NacosString accessToken = _objectConfigData->_securityManager->getAccessToken();
        if (!accessToken.empty()) {
            (*metadata->mutable_headers())["accessToken"] = accessToken;
        }
    }
}

void GrpcConfigServiceListener::startHealthLoop() {
    if (healthRunning.exchange(true)) {
        return;
    }
    stopHealth.store(false);
    healthThread = std::thread(&GrpcConfigServiceListener::healthLoop, this);
}

void GrpcConfigServiceListener::stopHealthLoop() {
    stopHealth.store(true);
    healthCv.notify_all();
    if (healthThread.joinable()) {
        healthThread.join();
    }
    healthRunning.store(false);
}

void GrpcConfigServiceListener::healthLoop() {
    std::unique_lock<std::mutex> lock(healthMutex);
    while (!stopHealth.load()) {
        healthCv.wait_for(lock, std::chrono::milliseconds(keepaliveIntervalMs),
                          [this] { return stopHealth.load(); });
        if (stopHealth.load()) {
            break;
        }
        lock.unlock();

        if (running.load() && connectionReady.load()) {
            std::vector<ListenSnapshot> dirty = snapshotDirty();
            if (!dirty.empty()) {
                sendBatchListen(dirty, true, true);
            } else if (steadyMillis() - lastFullSyncMillis.load() >= FULL_SYNC_INTERVAL_MS) {
                std::vector<ListenSnapshot> all = snapshotAll();
                if (all.empty() || sendBatchListen(all, true, true)) {
                    lastFullSyncMillis.store(steadyMillis());
                }
            }

            long long lastActivity = lastActiveMillis.load();
            if (lastActivity == 0 || steadyMillis() - lastActivity >= keepaliveIntervalMs) {
                if (!sendHealthCheck()) {
                    std::lock_guard<std::mutex> streamLock(state->streamMutex);
                    if (state->streamContext) {
                        state->streamContext->TryCancel();
                    }
                }
            }
        }
        lock.lock();
    }
}

void GrpcConfigServiceListener::markActivity() {
    lastActiveMillis.store(steadyMillis());
}

bool GrpcConfigServiceListener::sendHealthCheck() {
    rapidjson::Document request;
    request.SetObject();
    rapidjson::Document::AllocatorType &allocator = request.GetAllocator();
    addStringMember(request, "requestId", makeRequestId(), allocator);
    addStringMember(request, "module", "internal", allocator);

    std::string responseBody;
    if (!callUnary("HealthCheckRequest", jsonToString(request), responseBody, healthTimeoutMs)) {
        return false;
    }
    rapidjson::Document response;
    response.Parse(responseBody.c_str());
    bool ok = responseSucceeded(response);
    if (ok) {
        markActivity();
    }
    return ok;
}

} // namespace nacos
