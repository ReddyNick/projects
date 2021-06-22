#ifndef SHAD_CPP0_GEOMETRY_H
#define SHAD_CPP0_GEOMETRY_H

#include <cmath>
#include <algorithm>

#include <objects_class.h>
#include <camera_options.h>

const double kComparisonThreshold = 1e-10;
const double kEpsilon = 1e-8;

struct Ray {
    Vec3 origin{}, direction{};
    double distance = 0;
    bool inside = false;
    Ray() = default;
    Ray(const Vec3& origin, const Vec3& direction_in, const double distance = 0)
        : origin(origin), direction(direction_in), distance(distance), inside(false) {
        direction.Normalize();
    }
};

Ray MakeCameraRay(const CameraOptions& camera_options, int pix_height, int pix_width,
                  int screen_height, int screen_width) {
    double aspect_ration = static_cast<double>(screen_width) / screen_height;
    double x_coord = (2 * (pix_width + 0.5) / screen_width - 1) * std::tan(camera_options.fov / 2) *
                     aspect_ration;
    double y_coord =
        (1 - 2 * (pix_height + 0.5) / screen_height) * std::tan(camera_options.fov / 2);
    double z_coord = -1;

    Vec3 origin{};  // (0, 0, 0)
    Vec3 direction{x_coord, y_coord, z_coord};
    return Ray(origin, direction);
}
Vec3 VecProd(const Vec3& first, const Vec3& second) {
    double x_coord = first.y * second.z - first.z * second.y;
    double y_coord = first.z * second.x - first.x * second.z;
    double z_coord = first.x * second.y - first.y * second.x;

    return Vec3(x_coord, y_coord, z_coord);
}

void MakeToWorldMatrix(const CameraOptions& camera_options,
                       std::vector<std::vector<double>>& to_world_matrix) {

    const std::array<double, 3>& look_from = camera_options.look_from;
    const std::array<double, 3>& look_to = camera_options.look_to;

    Vec3 z_camera{look_from[0] - look_to[0], look_from[1] - look_to[1], look_from[2] - look_to[2]};
    z_camera.Normalize();

    Vec3 y_axis{0, 1, 0};

    Vec3 x_camera = VecProd(y_axis, z_camera);
    if (x_camera.x == 0 && x_camera.y == 0 && x_camera.z == 0) {
        x_camera = Vec3{1, 0, 0};
    } else {
        x_camera.Normalize();
    }

    Vec3 y_camera = VecProd(z_camera, x_camera);

    to_world_matrix[0][0] = x_camera.x;
    to_world_matrix[0][1] = y_camera.x;
    to_world_matrix[0][2] = z_camera.x;

    to_world_matrix[1][0] = x_camera.y;
    to_world_matrix[1][1] = y_camera.y;
    to_world_matrix[1][2] = z_camera.y;

    to_world_matrix[2][0] = x_camera.z;
    to_world_matrix[2][1] = y_camera.z;
    to_world_matrix[2][2] = z_camera.z;
}

Ray CameraToWorld(const Ray& ray, const CameraOptions& camera_options,
                  const std::vector<std::vector<double>>& matrix) {
    Ray transformed;
    const std::array<double, 3>& look_from = camera_options.look_from;
    transformed.origin = Vec3{look_from[0], look_from[1], look_from[2]};

    for (int i = 0; i < 3; ++i) {
        double coord = 0;
        for (int k = 0; k < 3; ++k) {
            coord += matrix[i][k] * ray.direction[k];
        }
        transformed.direction[i] = coord;
    }
    return transformed;
}
bool Intersection(const Ray& ray, const Object& object, double* distance);

bool Trace(Ray* ray, const ObjectSet& objects, const Object** object) {
    //если мы находимся внутри объекта а?а?а?

    double min_dist = 0;
    bool found = false;
    for (size_t i = 0; i < objects.Size(); ++i) {
        double distance = 0;
        if (Intersection(*ray, objects[i], &distance) &&
            (distance < min_dist - kComparisonThreshold || !found)) {
            min_dist = distance;
            *object = &objects[i];
            found = true;
        }
    }

    ray->distance = min_dist;
    return found;
}

bool IntersectionSphere(const Ray& ray, const Object& object, double* distance) {
    double b_coef = 2 * ray.direction * (ray.origin - object.Center());
    double c_coef = (ray.origin - object.Center()) * (ray.origin - object.Center()) -
                    object.Radius() * object.Radius();

    double discr = b_coef * b_coef - 4 * c_coef;
    if (discr < 0) {
        return false;
    }
    discr = std::sqrt(discr);
    int sign_b = (b_coef < 0 - kComparisonThreshold) ? -1 : 1;
    double x0 = -(b_coef + sign_b * discr) / 2;
    double x1 = c_coef / x0;

    if (x1 < x0 - kComparisonThreshold) {
        std::swap(x0, x1);
    }

    if (x0 > 0 + kComparisonThreshold) {
        *distance = x0;
        return true;
    } else if (x1 > 0 + kComparisonThreshold) {
        *distance = x1;
        return true;
    }

    return false;
}

Vec3 MakeNormal(const Vertex& one, const Vertex& two, const Vertex& three) {
    Vec3 cut_one = two.vertex - one.vertex;
    Vec3 cut_two = three.vertex - one.vertex;
    return VecProd(cut_one, cut_two);
}

bool IntersectionTriangle(const Ray& ray, const Object& object, double* distance) {
    Vec3 normal{};
    const Vertex& one = object(0);
    const Vertex& two = object(1);
    const Vertex& three = object(2);

    normal = MakeNormal(one, two, three);

    double denominator = ray.direction * normal;
    if (fabs(denominator) < kComparisonThreshold) {
        return false;
    }
    double nominator = one.vertex * normal - ray.origin * normal;
    double length = nominator / denominator;

    if (length < 0 - kComparisonThreshold) {
        return false;
    }

    Vec3 hit_point = ray.origin + length * ray.direction;

    Vec3 edge_12 = two.vertex - one.vertex;
    Vec3 point_1 = hit_point - one.vertex;
    if (VecProd(edge_12, point_1) * normal < 0 - kComparisonThreshold) {
        return false;
    }

    Vec3 edge_23 = three.vertex - two.vertex;
    Vec3 point_2 = hit_point - two.vertex;
    if (VecProd(edge_23, point_2) * normal < 0 - kComparisonThreshold) {
        return false;
    }

    Vec3 edge_31 = one.vertex - three.vertex;
    Vec3 point_3 = hit_point - three.vertex;
    if (VecProd(edge_31, point_3) * normal < 0 - kComparisonThreshold) {
        return false;
    }

    //    std::cout << hit_point.x << ' ' << hit_point.y << ' ' << hit_point.z << '\n';
    *distance = length;
    return true;
}

bool Intersection(const Ray& ray, const Object& object, double* distance) {
    if (object.Type() == ObjType::Sphere) {
        return IntersectionSphere(ray, object, distance);
    }
    return IntersectionTriangle(ray, object, distance);
}

double GetSpace(const Vec3& one, const Vec3& two, const Vec3& three) {
    return VecProd(two - one, three - one).Length() / 2;
}

Vec3 InterpolateNormal(const Object& object, const Vec3& hit_point) {

    const Vertex& one = object(0);
    const Vertex& two = object(1);
    const Vertex& three = object(2);

    double space = GetSpace(one.vertex, two.vertex, three.vertex);
    assert(space != 0);

    double v1_coord = GetSpace(hit_point, two.vertex, three.vertex) / space;
    double v2_coord = GetSpace(hit_point, one.vertex, three.vertex) / space;
    double v3_coord = GetSpace(hit_point, two.vertex, one.vertex) / space;

    return (v1_coord * one.vn + v2_coord * two.vn + v3_coord * three.vn).Normalize();
}

bool VisibleLight(const LightSource& light, const Vec3& hit_point, const ObjectSet& objects,
                  const Vec3& normal) {
    Vec3 to_light = Vec3(light.place - hit_point);
    if (normal * to_light < 0 - kComparisonThreshold) {
        return false;
    }

    double light_distance = to_light.Length();

    to_light.Normalize();
    Ray ray(hit_point + kEpsilon * to_light, to_light);
    const Object* obj{};
    if (Trace(&ray, objects, &obj) && (ray.distance < light_distance - kComparisonThreshold)) {
        return false;
    }

    return true;
}

Ray Refraction(const Ray& ray, const Vec3 normal, const double n_one, const double n_two,
               const Vec3& origin) {
    assert(n_two != 0);
    double cos_1 = -(ray.direction * normal);
    double sin_1 = std::sqrt(1 - cos_1 * cos_1);

    double sin_2 = n_one * sin_1 / n_two;
    if (sin_2 > 1 + kComparisonThreshold) {
        assert(0);
    }

    double cos_2 = std::sqrt(1 - sin_2 * sin_2);

    Vec3 mvec = n_one / n_two * (ray.direction + normal * cos_1);
    Ray refracted(origin, mvec - normal * cos_2);

    return refracted;
}

#endif  // SHAD_CPP0_GEOMETRY_H
