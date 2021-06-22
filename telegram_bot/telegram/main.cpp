#include <iostream>
#include <string>
#include <utility>
#include "client.h"

#include "bot.h"

int main(int argc, char* argv[]) {
    std::string api_key = "a token here";
    Bot bot(api_key);

    bot.Start();

//
//
//
//    Client client{api_key};
//
//    while (!client.ReceiveMessage()) {
//    }
//
//    auto messages = client.GetMessages();
//
//    int chat_id = 382918104;
//
//    std::vector<std::pair<std::string, std::string>> params;
//    params.emplace_back("text","Hey, haven't seen you for a long!");
//
//    client.SendTextMessage(chat_id, params);
//    return 0;
}