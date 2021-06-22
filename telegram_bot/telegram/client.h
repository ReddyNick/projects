#ifndef SHAD_CPP0_CLIENT_H
#define SHAD_CPP0_CLIENT_H

#include <cassert>
#include <fstream>
#include <memory>
#include <string>
#include <vector>
#include <optional>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>

const std::string kTelegramApiUrl = "https://api.telegram.org/bot";

struct Chat {
    Chat() = default;
    int64_t id;
};

enum MessageType { kUnknown, kText, kSticker };

struct Message {
    Message() = default;
    Message(MessageType type) : type(type) {
    }
    Chat chat;
    MessageType type = kUnknown;
    int message_id = 0;
};

struct Entity {
    Entity(const std::string& type, int offset, int length)
        : type(type), offset(offset), length(length) {
    }

    std::string type;
    int offset;
    int length;
};

struct TextMessage : Message {
    TextMessage(const std::string& message_text, MessageType message_type) {
        text = message_text;
        type = message_type;
    }

    std::string text;
    std::vector<Entity> entities;
};

class Parser;

class Client {
public:
    Client(const std::string& api_key, std::string filename = "offset.txt",
           const std::string& api_url = kTelegramApiUrl)
        : uri_(api_url + api_key + "/"), offset_filename_(filename) {
        if (uri_.getScheme() == "http") {
            session_ =
                std::make_shared<Poco::Net::HTTPClientSession>(uri_.getHost(), uri_.getPort());
        } else if (uri_.getScheme() == "https") {
            session_ =
                std::make_shared<Poco::Net::HTTPSClientSession>(uri_.getHost(), uri_.getPort());
        }
    }

    void SetTimeout(int timeout) {
        timeout_ = timeout;
    }

    Parser MakeRequest(const Poco::URI& uri);
    bool ReceiveMessage(bool parameters = true);
    bool HasOffset();
    void SendTextMessage(const int chat_id,
                         const std::vector<std::pair<std::string, std::string>>& parameters);

    std::vector<std::shared_ptr<Message>> GetMessages() {
        return std::move(messages_);
    }

private:
    void SaveOffset();

private:
    Poco::URI uri_;
    std::shared_ptr<Poco::Net::HTTPClientSession> session_;

    std::optional<int64_t> offset_;
    int timeout_ = 3;  // in seconds
    std::string offset_filename_;

    std::vector<std::shared_ptr<Message>> messages_;
};

class Parser {
public:
    Parser(std::istream& response) : response_(response) {
    }
    bool OkStatus();

    void Parse();
    std::optional<int64_t> GetLastUpdateId();
    std::vector<std::shared_ptr<Message>> GetMessages();

private:
    std::shared_ptr<Message> ParseMessage(Poco::SharedPtr<Poco::JSON::Object> message);
    int GetType(Poco::SharedPtr<Poco::JSON::Object> message);
    std::shared_ptr<Message> ParseTextMessage(Poco::SharedPtr<Poco::JSON::Object> message);

private:
    std::istream& response_;
    Poco::JSON::Object::Ptr json_;
    Poco::SharedPtr<Poco::JSON::Array> result_;
};

#endif  // SHAD_CPP0_CLIENT_H
