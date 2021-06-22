#include "bot.h"
#include <random>

void BotBase::Start() {
    while (!stop_status_) {
        try {
            if (!client_.ReceiveMessage()) {
                continue;
            }
            auto messages = client_.GetMessages();
            for (const auto& message : messages) {
                ProcessMessage(message);
            }
        } catch (std::runtime_error error) {
            std::cerr << error.what() << '\n';
            break;
        } catch (std::exception error) {
            std::cerr << "Unknown exception!\n";
            std::cerr << error.what() << '\n';
            break;
        }
    }
}

void BotBase::ProcessMessage(std::shared_ptr<Message> message) {
    switch (message->type) {
        case kText:
            ProcessTextMessage(message);
        default:
            return;
    }
}

void Bot::ProcessTextMessage(std::shared_ptr<Message> message) {
    auto text_message = std::static_pointer_cast<TextMessage>(message);
    assert(text_message);

    if (text_message->entities.size() != 1 || text_message->entities[0].type != "bot_command") {
        return;
    }
    if (static_cast<int>(text_message->text.size()) != text_message->entities[0].length) {
        return;
    }

    ProcessCommand(text_message);
}

void Bot::ProcessCommand(std::shared_ptr<TextMessage> message) {
    if (message->text == "/random") {
        static std::mt19937 generator;
        std::uniform_int_distribution random_number(0, 1000000);

        int number = random_number(generator);
        std::vector<std::pair<std::string, std::string>> text;
        text.emplace_back("text", std::to_string(number));
        client_.SendTextMessage(message->chat.id, std::move(text));
        return;
    }

    if (message->text == "/weather") {
        std::vector<std::pair<std::string, std::string>> text;
        text.emplace_back("text", "Winter Is Coming");
        client_.SendTextMessage(message->chat.id, std::move(text));
        return;
    }

    if (message->text == "/styleguide") {
        std::vector<std::pair<std::string, std::string>> text;
        text.emplace_back("text",
                          "В ресторане под названием \"Глобальные перемены\" раздраженный "
                          "программист к названию каждого блюда в меню подписал слева букву k.");
        client_.SendTextMessage(message->chat.id, std::move(text));
        return;
    }

    if (message->text == "/stop") {
        stop_status_ = true;
        return;
    }

    if (message->text == "/crash") {
        abort();
    }
}

