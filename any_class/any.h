#pragma once

#include <memory>

class Any {
public:
    Any() = default;

    template <class T>
    Any(const T& value) {
        base_ = std::make_unique<Object<T>>(value);
    }

    template <class T>
    Any& operator=(const T& value) {
        base_ = std::make_unique<Object<T>>(value);
        return *this;
    }

    Any(const Any& rhs) {
        base_ = rhs.base_->Clone();
    }

    Any& operator=(const Any& rhs) {
        base_ = rhs.base_->Clone();
        return *this;
    }

    ~Any() = default;

    bool Empty() const {
        return base_ == nullptr;
    }

    void Clear() {
        base_ = nullptr;
    }

    void Swap(Any& rhs) {
        std::swap(base_, rhs.base_);
    }

    template <class T>
    const T& GetValue() const {
        return dynamic_cast<const Object<T>&>(*base_.get()).value_;
    }

    struct BaseAny {
        virtual ~BaseAny() = default;
        virtual std::unique_ptr<BaseAny> Clone() = 0;
    };

    template <typename T>
    struct Object : public BaseAny {
        T value_;
        explicit Object(const T& value) : value_(value) {
        }
        std::unique_ptr<BaseAny> Clone() override {
            return std::make_unique<Object<T>>(value_);
        }
    };

private:
    std::unique_ptr<BaseAny> base_ = nullptr;
};
