#include <iostream>
#include <string>
#include <utility>
#include "client.h"

#include "bot.h"

int main(int argc, char* argv[]) {
    std::string api_key = "some token";
    Bot bot(api_key);

    bot.Start();
}