#include <string>
#include <curl/curl.h>
#include "naming/registry/NamingClient.h"
#include "naming/registry/Naming.h"
#include "factory/NacosFactoryFactory.h"
#include "factory/INacosServiceFactory.h"
#include "naming/NamingService.h"
#include "Properties.h"
#include "constant/PropertyKeyConst.h"
#include "src/json/rapidjson/document.h"

namespace nacos { namespace naming {

namespace {

const char *OPEN_API_PATH = "/nacos/v2/ns/instance";
const char *DEFAULT_GROUP_NAME = "DEFAULT_GROUP";

//Extract the first "host:port" from a (possibly comma separated) server address.
NacosString firstServerAddr(const NacosString &addr) {
    NacosString item = addr;
    size_t commaPos = item.find(',');
    if (commaPos != NacosString::npos) {
        item = item.substr(0, commaPos);
    }

    //strip an optional scheme prefix so we can rebuild the url ourselves
    size_t schemePos = item.find("://");
    if (schemePos != NacosString::npos) {
        item = item.substr(schemePos + 3);
    }

    //strip any trailing path
    size_t slashPos = item.find('/');
    if (slashPos != NacosString::npos) {
        item = item.substr(0, slashPos);
    }

    return item;
}

NacosString urlEncode(CURL *curl, const NacosString &value) {
    char *escaped = curl_easy_escape(curl, value.c_str(), (int) value.size());
    if (escaped == NULL) {
        return "";
    }
    NacosString result(escaped);
    curl_free(escaped);
    return result;
}

size_t writeBodyCallback(void *contents, size_t size, size_t nmemb, void *userp) {
    size_t realsize = size * nmemb;
    NacosString *body = reinterpret_cast<NacosString *>(userp);
    body->append(reinterpret_cast<char *>(contents), realsize);
    return realsize;
}

}//anonymous namespace

NamingClient::NamingClient(const NacosString &namespaceId, const NacosString &addr) NACOS_THROW(NacosException) {
    _namespaceId = namespaceId;
    _addr = addr;
    _factory = NULL;
    _naming = NULL;

    Properties props;
    props[PropertyKeyConst::SERVER_ADDR] = addr;
    props[PropertyKeyConst::NAMESPACE] = namespaceId;
    //Mirror the Go client config: 5s request timeout, do not preload cache at start.
    props[PropertyKeyConst::SERVER_REQ_TIMEOUT] = "5000";
    props[PropertyKeyConst::NAMING_LOAD_CACHE_AT_START] = "false";

    _factory = NacosFactoryFactory::getNacosFactory(props);
    _naming = _factory->CreateNamingService();
}

NamingClient::~NamingClient() {
    close();
}

void NamingClient::close() {
    //Deleting the factory tears down the naming service, beat reactor, pollers
    //and the subscription listeners owned by the event dispatcher.
    _naming = NULL;
    if (_factory != NULL) {
        delete _factory;
        _factory = NULL;
    }
}

void NamingClient::updateEndpoint(const Endpoint &endpoint, ApiVersion apiVersion) NACOS_THROW(NacosException) {
    CURL *curl = curl_easy_init();
    if (curl == NULL) {
        throw NacosException(NacosException::UNABLE_TO_CREATE_SOCKET,
                             "nacos naming client update endpoint failed, unable to init curl handle");
    }

    NacosString serviceNameField = endpoint.serviceName;
    if (apiVersion == API_VERSION_OLD) {
        serviceNameField = NacosString(DEFAULT_GROUP_NAME) + "@@" + endpoint.serviceName;
    }

    //Build the x-www-form-urlencoded body, keeping the exact field set Go sends.
    NacosString body;
    body += "namespaceId=" + urlEncode(curl, _namespaceId);
    body += "&groupName=" + urlEncode(curl, DEFAULT_GROUP_NAME);
    body += "&serviceName=" + urlEncode(curl, serviceNameField);
    body += "&ip=" + urlEncode(curl, endpoint.ip);
    body += "&port=" + urlEncode(curl, NacosStringOps::valueOf(endpoint.port));
    body += "&healthy=" + urlEncode(curl, NacosStringOps::valueOf(true));
    body += "&weight=" + urlEncode(curl, NacosStringOps::valueOf(endpoint.weight));
    body += "&enabled=" + urlEncode(curl, NacosStringOps::valueOf(endpoint.enable));
    body += "&metadata=" + urlEncode(curl, "");
    body += "&ephemeral=" + urlEncode(curl, NacosStringOps::valueOf(true));

    NacosString url = "http://" + firstServerAddr(_addr) + OPEN_API_PATH;

    NacosString respBody;
    long httpCode = 0;

    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/x-www-form-urlencoded");

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long) body.size());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeBodyCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *) &respBody);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 5000L);

    CURLcode rc = curl_easy_perform(curl);
    if (rc == CURLE_OK) {
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (rc != CURLE_OK) {
        throw NacosException(NacosException::SERVER_ERROR,
                             NacosString("nacos naming client update endpoint failed, curl err:") +
                             curl_easy_strerror(rc));
    }

    //Parse the (optional) JSON envelope {code, message, data}.
    int64_t code = 0;
    NacosString message;
    NacosString data;
    rapidjson::Document doc;
    doc.Parse(respBody.c_str());
    if (!doc.HasParseError() && doc.IsObject()) {
        if (doc.HasMember("code") && doc["code"].IsInt64()) {
            code = doc["code"].GetInt64();
        } else if (doc.HasMember("code") && doc["code"].IsInt()) {
            code = doc["code"].GetInt();
        }
        if (doc.HasMember("message") && doc["message"].IsString()) {
            message = doc["message"].GetString();
        }
        if (doc.HasMember("data") && doc["data"].IsString()) {
            data = doc["data"].GetString();
        }
    }

    if (apiVersion == API_VERSION_OLD) {
        if (code == 500 && respBody.find("service not found") != NacosString::npos) {
            throw EndpointNotFoundException("nacos naming client update endpoint failed, endpoint not found");
        }
        if (respBody != "ok") {
            throw NacosException(NacosException::SERVER_ERROR,
                                 "nacos naming client update endpoint failed, body:" + respBody);
        }
        return;
    }

    //API_VERSION_NEW
    if (code == 21004 && data.find("service not found") != NacosString::npos) {
        throw EndpointNotFoundException("nacos naming client update endpoint failed, endpoint not found");
    }
    if (code == 20004 && message.find("resource not found") != NacosString::npos) {
        throw EndpointNotFoundException("nacos naming client update endpoint failed, endpoint not found");
    }
    if (data != "ok") {
        throw NacosException(NacosException::SERVER_ERROR,
                             "nacos naming client update endpoint failed, body:" + respBody);
    }
}

NamingClient *NewNamingClient(const NacosString &namespaceId, const NacosString &addr) NACOS_THROW(NacosException) {
    return new NamingClient(namespaceId, addr);
}

} /*naming*/ } /*nacos*/
