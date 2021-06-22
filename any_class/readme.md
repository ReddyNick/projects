# Any

Реализация аналога класса [std::any](http://en.cppreference.com/w/cpp/experimental/any) из нового стандарта
C++17. Данный класс позволяет инкапсулировать в себе значение любого типа, например
```
Any a = 5;
Any b = std::string("hello, world");
Any c = std::vector<int>{1, 2, 3};
```

Интерфейс класса приведен в файле `any.h`.

Используется техника type erasure, прочитать о которой можно [здесь](https://en.wikibooks.org/wiki/More_C%2B%2B_Idioms/Type_Erasure).
