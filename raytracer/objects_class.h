#ifndef SHAD_CPP0_OBJECTS_CLASS_H
#define SHAD_CPP0_OBJECTS_CLASS_H

#include <cassert>
#include <vector>
#include <string>
#include <cmath>
#include <algorithm>

enum class ObjType { Triangle, Sphere, None };

struct Vec3 {
    double x = 0, y = 0, z = 0;

    Vec3() = default;
    explicit Vec3(double x, double y, double z) : x(x), y(y), z(z) {
    }

    Vec3& Normalize() {
        double length = std::sqrt(x * x + y * y + z * z);
        assert(length != 0);
        x /= length;
        y /= length;
        z /= length;
        return *this;
    }

    const double& operator[](int index) const {
        assert(index >= 0 && index < 3);
        switch (index) {
            case 0:
                return x;
            case 1:
                return y;
            case 2:
                return z;
            default:
                return x;
        }
    }

    double& operator[](int index) {
        const Vec3& const_this = *this;
        return const_cast<double&>(const_this.operator[](index));  // NOLINT
    }

    double operator*(const Vec3& other) const {
        return x * other.x + y * other.y + z * other.z;
    }

    Vec3 operator*(const double num) const {
        return Vec3(num * x, num * y, num * z);
    }

    Vec3 operator-(const Vec3& other) const {
        return Vec3(x - other.x, y - other.y, z - other.z);
    }

    Vec3 operator+(const Vec3& other) const {
        return Vec3(x + other.x, y + other.y, z + other.z);
    }

    double Length() const {
        return std::sqrt(x * x + y * y + z * z);
    }
};

Vec3 operator*(double num, const Vec3& vec) {
    return vec * num;
}

struct RGBProperty {
    double r = 0, g = 0, b = 0;
    RGBProperty() = default;
    RGBProperty(double r, double g, double b) : r(r), g(g), b(b) {
    }
    RGBProperty operator+(const RGBProperty& rhs) const {
        return RGBProperty(r + rhs.r, g + rhs.g, b + rhs.b);
    }
    const RGBProperty& operator+=(const RGBProperty& rhs) {
        r += rhs.r;
        g += rhs.g;
        b += rhs.b;
        return *this;
    }

    RGBProperty operator*(const RGBProperty& rhs) const {
        return RGBProperty(r * rhs.r, g * rhs.g, b * rhs.b);
    }

    RGBProperty operator/(const RGBProperty& rhs) const {
        return RGBProperty(r / rhs.r, g / rhs.g, b / rhs.b);
    }
};

RGBProperty operator*(const RGBProperty& rgb, double num) {
    return RGBProperty(rgb.r * num, rgb.g * num, rgb.b * num);
}
RGBProperty operator*(double num, const RGBProperty& rgb) {
    return rgb * num;
}

RGBProperty operator+(double num, const RGBProperty& rgb) {
    return RGBProperty(rgb.r + num, rgb.g + num, rgb.b + num);
}

RGBProperty operator/(const RGBProperty& rgb, double val) {
    return rgb * (1 / val);
}

RGBProperty Pow(const RGBProperty& rgb, double power) {
    return RGBProperty(std::pow(rgb.r, power), std::pow(rgb.g, power), std::pow(rgb.b, power));
}

struct Properties {
    Properties() = default;
    Properties(RGBProperty ka, RGBProperty ke, RGBProperty kd, RGBProperty ks, RGBProperty ns,
               RGBProperty tr, RGBProperty ni, RGBProperty illum)
        : ka(ka), ke(ke), kd(kd), ks(ks), ns(ns), tr(tr), ni(ni), illum(illum) {
    }

    RGBProperty& GetProperty(const std::string& property) {
        if (property == "Ka") {
            return ka;
        }
        if (property == "Ke") {
            return ke;
        }
        if (property == "Kd") {
            return kd;
        }
        if (property == "Ks") {
            return ks;
        }
        if (property == "Ns") {
            return ns;
        }
        if (property == "Tr") {
            return tr;
        }
        if (property == "Ni") {
            return ni;
        }
        if (property == "illum") {
            return illum;
        }
        return illum;
    }
    // Ka/Ke - the ambient colors
    // Kd - the diffuse color
    // Ks - the specular color
    // Ns - specular exponent
    // Tr - transparency (d, Tr)
    // Ni - optical density
    // illum нужно обрабатывать следующим образом - если значение > 2, то необходимо
    // также посчитать отраженный и преломленный лучи, иначе нет.
    RGBProperty ka, ke, kd, ks, ns, tr{0, 0, 0}, ni{1, 1, 1}, illum;
};

struct Vertex {
    Vertex() = default;
    Vertex(const Vec3& vertex, const Vec3& vn) : vertex(vertex), vn(vn) {
    }
    Vec3 vertex;
    Vec3 vn;
    bool defined_normal = false;
};

struct LightSource {
    Vec3 place{};
    RGBProperty intensity{};
    LightSource(const Vec3& place, const RGBProperty& intensity)
        : place(place), intensity(intensity) {
    }
};

class Object {
public:
    Object() = default;
    explicit Object(ObjType type, const Properties& properties, const Vertex& v1, const Vertex& v2,
                    const Vertex& v3)
        : type_(type), properties_(properties), v1_(v1), v2_(v2), v3_(v3) {
        assert(type == ObjType::Triangle);
    }

    explicit Object(ObjType type, Properties& properties, const Vec3& center, double radius)
        : type_(type), properties_(properties), center_(center), radius_(radius) {
        assert(type == ObjType::Sphere);
    }

    RGBProperty Ka() const {
        return properties_.ka;
    }
    RGBProperty Ke() const {
        return properties_.ke;
    }
    RGBProperty Kd() const {
        return properties_.kd;
    }
    RGBProperty Ks() const {
        return properties_.ks;
    }
    RGBProperty Ns() const {
        return properties_.ns;
    }
    RGBProperty Tr() const {
        return properties_.tr;
    }
    RGBProperty Ni() const {
        return properties_.ni;
    }
    RGBProperty Illum() const {
        return properties_.illum;
    }
    ObjType Type() const {
        return type_;
    }
    double Radius() const {
        return radius_;
    }
    const Vec3& Center() const {
        return center_;
    }
    const Vertex& operator()(int index) const {
        assert(index >= 0 && index < 3);
        switch (index) {
            case 0:
                return v1_;
            case 1:
                return v2_;
            case 2:
                return v3_;
            default:
                return v1_;
        }
    }
    const Vertex& V1() const {
        return v1_;
    }
    const Vertex& V2() const {
        return v2_;
    }
    const Vertex& V3() const {
        return v3_;
    }

private:
    ObjType type_ = ObjType::None;
    Properties properties_{};
    Vertex v1_{}, v2_{}, v3_{};
    Vec3 center_{};
    double radius_ = 0;
};

class ObjectSet {
public:
    ObjectSet() = default;

    void InsertObject(const Object& object) {
        objects_.emplace_back(object);
    }

    const Object& operator[](size_t index) const {
        assert(index < objects_.size());
        return objects_[index];
    }

    size_t Size() const {
        return objects_.size();
    }

private:
    std::vector<Object> objects_;
};

#endif  // SHAD_CPP0_OBJECTS_CLASS_H
