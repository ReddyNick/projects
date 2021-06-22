# bot

Телеграм бота на основе библиотеки POCO.

## Знакомство с HTTP-API

HTTP протокол для общения с серверами описан
на [этой](https://core.telegram.org/bots/api) странице.

Чтобы воспользоваться HTTP-API
нужно [получить токен](https://core.telegram.org/bots#6-botfather).

После того как вы получили токен, нужно проверить что HTTP-API
работает. Для этого мы будем дёргать методы командой `curl`. Еще нам
понадобиться утилита `json_pp`.

 * [Метод](https://core.telegram.org/bots/api#getme) `/getMe`
   возвращает информацию о вашем боте. Его удобно использовать для
   проверки токена.

```
~/C/s/bot (master|…) $ curl https://api.telegram.org/bot<YOUR_TOKEN_HERE>/getMe
{"ok":true,"result":{"id":384306257,"is_bot":true,"first_name":"shad-cpp-test","username":"shad_shad_test_test_bot"}}
```

 * [Метод](https://core.telegram.org/bots/api#getting-updates)
   `/getUpdates` возвращает сообщения направленные вашему боту.
   
   - Чтобы этот метод вернул что-то осмысленное, нужно послать
   сообщение своему боту.
   - Обратите внимание на параметр `update_id`.
   - Почему команда возвращает те же самые сообщение при повторном
     запуске? Как сервера телеграмма понимают, что ваш бот обработал
     сообщения?
 
```
~/C/s/bot (master|…) $ curl -s https://api.telegram.org/bot<YOUR_TOKEN_HERE>/getUpdates | json_pp
{
   "ok" : true,
   "result" : [
      {
         "message" : {
            "date" : 1510493105,
            "entities" : [
               {
                  "length" : 6,
                  "offset" : 0,
                  "type" : "bot_command"
               }
            ],
            "from" : {
               "is_bot" : false,
               "username" : "darth_slon",
               "id" : 104519755,
               "first_name" : "Fedor"
            },
            "chat" : {
               "username" : "darth_slon",
               "type" : "private",
               "first_name" : "Fedor",
               "id" : 104519755
            },
            "text" : "/start",
            "message_id" : 1
         },
         "update_id" : 851793506
      }
   ]
}
```

 * [Метод](https://core.telegram.org/bots/api#sendmessage)
   `/sendMessage` позволяет послать сообщение.
 
```
curl -s -H "Content-Type: application/json" -X POST -d '{"chat_id": <CHAT_ID_FROM_GET_UPDATES>, "text": "Hi!"}' https://api.telegram.org/bot<YOUR_TOKEN_HERE>/sendMessage | json_pp
{
   "ok" : true,
   "result" : {
      "chat" : {
         "type" : "private",
         "id" : 104519755,
         "username" : "darth_slon",
         "first_name" : "Fedor"
      },
      "message_id" : 5,
      "from" : {
         "is_bot" : true,
         "first_name" : "shad-cpp-test",
         "id" : 384306257,
         "username" : "shad_shad_test_test_bot"
      },
      "date" : 1510500325,
      "text" : "Hi!"
   }
}
```


## Клиент

 * Все методы клиента сильно типизированы. `Poco::Json` нигде не торчит из интерфейса.

 * Ошибки HTTP-API транслируются в исключения.

 * Клиент сохраняет текущий offset в файл и восстанавливает после перезапуска.

 * Клиент ничего не знает про логику конкретного бота.
 

## Что делает бот

 * Запрос `/random`. Бот посылает случайное число ответом на это сообщение.
 
 * Запрос `/weather`. Бот отвечает в чат `Winter Is Coming`.
 
 * Запрос `/styleguide`. Бот отвечает в чат смешной шуткой на тему code review.
 
 * Запрос `/stop`. Процесс бота завершается штатно.
 
 * Запрос `/crash`. Процесс бота завершается аварийно. Например выполняет `abort();`.

## Полезные классы из библиотеки POCO

 - `Poco::Net::HTTPClientSession`
 - `Poco::Net::HTTPSClientSession`
 - `Poco::Net::HTTPRequest`
 - `Poco::Net::HTTPResponse`
 - `Poco::URI`
 - `Poco::JSON::Object`
 - `Poco::JSON::Parser`

## Установка зависимостей

 * На **Ubuntu** `sudo apt-get install libpoco-dev`
 * На **MacOS** `brew install poco --build-from-source --cc=gcc-8`
 * На **Windows** `TODO`

## Автоматические тесты

Общаться с реальным сервером в тестах - не самая лучшая идея по многим
причинам. Вместо этого, используется класс `FakeServer`, написаны функциональные тесты, проверяющие набор сценариев.

`FakeServer` только прикидывается сервером и отвечает на все запросы
заранее заготовленными ответами.

Код тестов выглядит примерно так:

```c++
TEST_CASE("Checking getMe") {
    FakeServer fake("Single getMe")
    fake.Start();
       
    // Your code here. Doing requests to fake.GetUrl();
    
    fake.StopAndCheckExpectations();
}
```