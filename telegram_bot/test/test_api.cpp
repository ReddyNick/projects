#include <catch.hpp>

#include "../telegram/fake.h"
#include "../telegram/client.h"

TEST_CASE("Checking getMe") {
    telegram::FakeServer fake("Single getMe");
    fake.Start();

    Client client("bot123", "offset.txt", fake.GetUrl());
    Poco::URI uri(fake.GetUrl() + "bot123/" + "getMe");
    client.MakeRequest(uri);

    fake.StopAndCheckExpectations();
}

TEST_CASE("getMe error handling") {
    telegram::FakeServer fake("getMe error handling");
    fake.Start();

    Client client("bot123", "offset.txt", fake.GetUrl());
    Poco::URI uri(fake.GetUrl() + "bot123/" + "getMe");

    try {
        client.MakeRequest(uri);
    }
    catch (std::runtime_error) {
    }
    try {
        client.MakeRequest(uri);
    }
    catch (std::runtime_error) {
    }

    fake.StopAndCheckExpectations();
}

TEST_CASE("Single getUpdates and send messages") {
    telegram::FakeServer fake("Single getUpdates and send messages");
    fake.Start();

    Client client("bot123", "offset.txt", fake.GetUrl());

    client.ReceiveMessage(false);
    auto messages = client.GetMessages();

    int chat_id = messages[0]->chat.id;

    std::vector<std::pair<std::string, std::string>> parameters;
    parameters.emplace_back("text", "Hi!");
    client.SendTextMessage(chat_id, parameters);

    parameters[0] = std::make_pair("text", "Reply");
    parameters.emplace_back("reply_to_message_id", "2");
    client.SendTextMessage(chat_id, parameters);
    client.SendTextMessage(chat_id, parameters);

    fake.StopAndCheckExpectations();
}

TEST_CASE("Handle getUpdates offset") {
    telegram::FakeServer fake("Handle getUpdates offset");
    fake.Start();

    Client client("bot123", "offset.txt", fake.GetUrl());
    client.SetTimeout(5);
    client.ReceiveMessage();
    client.ReceiveMessage();
    client.ReceiveMessage();


    fake.StopAndCheckExpectations();
}



