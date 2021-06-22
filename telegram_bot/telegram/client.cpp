#include "client.h"

#include <exception>
#include <utility>
#include <sstream>

bool Client::ReceiveMessage(bool parameters) {
    Poco::URI request_uri(uri_.toString() + "getUpdates");

    if (parameters) {
        if (HasOffset()) {
            request_uri.addQueryParameter("offset", std::to_string(offset_.value()));
        }
        request_uri.addQueryParameter("timeout", std::to_string(timeout_));
    }

    Parser parser = MakeRequest(request_uri);

    auto new_offset = parser.GetLastUpdateId();
    if (new_offset == std::nullopt) {
        return false;
    }

    offset_ = new_offset.value() + 1;
    
    messages_ = parser.GetMessages();
    return true;
}

bool Client::HasOffset() {
    if (offset_.has_value()) {
        return true;
    }

    std::ifstream offset_file(offset_filename_);
    if (!offset_file) {
        return false;
    }

    std::string offset_from_file;
    offset_file >> offset_from_file;
    if (!offset_from_file.empty()) {
        offset_ = std::stoi(offset_from_file);
        return true;
    }

    return false;
}

// todo how to pass parameters better?

void Client::SendTextMessage(const int chat_id,
                             const std::vector<std::pair<std::string, std::string>>& parameters) {
    Poco::JSON::Object message;
    assert(chat_id > 0);
    message.set("chat_id", chat_id);
    for (const auto& pair : parameters) {
        message.set(pair.first, pair.second);
    }

    std::stringstream content;
    message.stringify(content);

    Poco::URI request_uri(uri_.toString() + "sendMessage");
    Poco::Net::HTTPRequest request("POST", request_uri.getPathAndQuery());
    request.setContentType("application/json");
    request.setContentLength(content.str().size());

    std::ostream& request_body = session_->sendRequest(request);
    message.stringify(request_body);

    Poco::Net::HTTPResponse response;
    std::istream* response_body;
    try {
        response_body = &session_->receiveResponse(response);
    } catch (Poco::TimeoutException) {
        session_->reset();
        throw std::runtime_error("Client::SendTextMessage - Poco timeout exception.");
    }

    if (response.getStatus() != 200) {
        throw std::runtime_error("Client::SendTextMessage - Bad status" +
                                 std::to_string(response.getStatus()) + "from server.");
    }
}

void Client::SaveOffset() {
    std::ofstream output(offset_filename_);
    if (!output) {
        throw std::runtime_error("Can't save offset to file.");
    }
    output << offset_.value();
}

Parser Client::MakeRequest(const Poco::URI& uri) {
    Poco::Net::HTTPRequest request("GET", uri.getPathAndQuery());

    session_->sendRequest(request);
    Poco::Net::HTTPResponse response;
    std::istream* response_body;
    try {
        response_body = &session_->receiveResponse(response);
    } catch (Poco::TimeoutException) {
        session_.reset();
        throw std::runtime_error("Poco timeout exception.");
    }

    if (response.getStatus() != 200) {
        throw std::runtime_error("Bad status " + std::to_string(response.getStatus()) +
                                 " from server.");
    }

    Parser parser{*response_body};
    parser.Parse();
    if (!parser.OkStatus()) {
        throw std::runtime_error("Ok status in reply is not true");
    }

    return parser;
}

void Parser::Parse() {
    Poco::JSON::Parser poco_parser;
    json_ = poco_parser.parse(response_).extract<Poco::JSON::Object::Ptr>();
    result_ = json_->getArray("result");
}

bool Parser::OkStatus() {
    auto ok_status = json_->getValue<std::string>("ok");
    if (ok_status != "true") {
        return false;
    }
    return true;
}

std::optional<int64_t> Parser::GetLastUpdateId() {
    if (result_->size() == 0) {
        return std::nullopt;
    }
    return result_->getObject(result_->size() - 1)->getValue<int64_t>("update_id");
}

std::vector<std::shared_ptr<Message>> Parser::GetMessages() {
    std::vector<std::shared_ptr<Message>> messages;

    if (result_->size() == 0) {
        return messages;
    }

    for (auto iterator = result_->begin(); iterator != result_->end(); ++iterator) {
        auto update = iterator->extract<Poco::JSON::Object::Ptr>();
        auto message = update->getObject("message");

        auto message_ptr = ParseMessage(message);
        if (!message_ptr) {
            continue;
        }

        auto chat = message->getObject("chat");
        message_ptr->chat.id = chat->getValue<int64_t>("id");
        message_ptr->message_id = message->getValue<int64_t>("message_id");

        messages.push_back(message_ptr);
    }

    return messages;
}

int Parser::GetType(Poco::SharedPtr<Poco::JSON::Object> message) {
    if (message->has("text")) {
        return kText;
    }
    if (message->has("sticker")) {
        return kSticker;
    }

    return kUnknown;
}

std::shared_ptr<Message> Parser::ParseMessage(Poco::SharedPtr<Poco::JSON::Object> message) {
    int type = GetType(message);
    switch (type) {
        case kText:
            return ParseTextMessage(message);
        case kSticker:
            return nullptr;
        case kUnknown:
            return nullptr;
        default:
            throw std::runtime_error("in Parser::ParseMessage(): reached default case");
    }
}

std::shared_ptr<Message> Parser::ParseTextMessage(Poco::SharedPtr<Poco::JSON::Object> message) {
    std::string text = message->getValue<std::string>("text");

    auto text_message = std::make_shared<TextMessage>(text, kText);

    if (message->has("entities")) {
        auto entities = message->getArray("entities");
        for (size_t i = 0; i < entities->size(); ++i) {
            auto entity = entities->getObject(i);

            int length = entity->getValue<int>("length");
            int offset = entity->getValue<int>("offset");
            std::string type = entity->getValue<std::string>("type");

            text_message->entities.emplace_back(type, offset, length);
        }
    }

    return text_message;
}
