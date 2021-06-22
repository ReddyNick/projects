#ifndef SHAD_CPP0_BOT_H
#define SHAD_CPP0_BOT_H

#include "client.h"

class BotBase {
public:
    BotBase(const std::string& api_key, std::string filename = "offset.txt")
        : client_(api_key, filename) {
    }
    void Start();

protected:
    void ProcessMessage(std::shared_ptr<Message> message);
    virtual void ProcessTextMessage(std::shared_ptr<Message> message) = 0;

protected:
    Client client_;
    bool stop_status_ = false;
};

class Bot : public BotBase {
public:
    Bot(const std::string& api_key, std::string filename = "offset.txt")
        : BotBase(api_key, filename) {}

    void ProcessTextMessage(std::shared_ptr<Message> message) override;
    void ProcessCommand(std::shared_ptr<TextMessage> message);

private:
    std::string api_key_;
    std::string offset_filename_;
};

#endif  // SHAD_CPP0_BOT_H
