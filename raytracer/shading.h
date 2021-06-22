#ifndef SHAD_CPP0_SHADING_H
#define SHAD_CPP0_SHADING_H

#include <objects_class.h>
#include <geometry.h>
#include <algorithm>
#include <cmath>
#include "render_options.h"
#include <iostream>

RGBProperty Diffuse(const Object& object, const Vec3& to_light, const RGBProperty& intensity,
                    const Vec3& normal) {
    RGBProperty rgb = object.Kd() * intensity * std::max(0.0, normal * to_light);
    return rgb;
}

RGBProperty Specular(const Object& object, const Vec3& vec_e, const Vec3& to_light,
                     const RGBProperty& intensity, const Vec3& normal) {
    Vec3 vec_r = (2.0 * (normal * to_light) * normal - to_light);
    return object.Ks() * intensity * std::pow(vec_e * vec_r, object.Ns().r);
}

Vec3 ComputeNormal(const Ray& ray, const Vec3& hit_point, const Object& object) {
    Vec3 normal{};
    if (object.Type() == ObjType::Triangle) {
        if (object(0).defined_normal) {
            normal = InterpolateNormal(object, hit_point);
        } else {
            normal =
                VecProd(object(1).vertex - object(0).vertex, object(2).vertex - object(0).vertex);
            normal.Normalize();
        }
    } else {
        normal = (hit_point - object.Center()).Normalize();
    }

    if (ray.direction * normal > 0 + kComparisonThreshold) {
        normal = -1 * normal;
    }

    return normal;
}

RGBProperty Shade(const Ray& ray, const Object& object, const ObjectSet& objects,
                  const std::vector<LightSource>& sources, const RenderOptions& render_options,
                  const int depth);

RGBProperty GetI(Ray* ray, const ObjectSet& objects, const std::vector<LightSource>& sources,
                 const RenderOptions& render_options, const int depth) {
    RGBProperty pixel{};
    if (depth > render_options.depth) {
        return pixel;
    }

    const Object* object;
    if (Trace(ray, objects, &object)) {
        pixel = Shade(*ray, *object, objects, sources, render_options, depth);
    }
    return pixel;
}

Ray Reflected(const Ray& ray, const Vec3& normal, const Vec3& hit_point) {
    Vec3 falling = -1 * ray.direction;
    Vec3 direction = 2 * (falling * normal) * normal - falling;

    return Ray(hit_point, direction);
}

RGBProperty Shade(const Ray& ray, const Object& object, const ObjectSet& objects,
                  const std::vector<LightSource>& sources, const RenderOptions& render_options,
                  const int depth) {

    Vec3 hit_point = ray.origin + ray.distance * ray.direction;
    Vec3 normal = ComputeNormal(ray, hit_point, object);

    if (ray.inside) {

        if (object.Tr().r == 0) {
            Ray new_ray = ray;
            new_ray.inside = false;
            return Shade(new_ray, object, objects, sources, render_options, depth);
        }

        Ray refracted = Refraction(ray, normal, object.Ni().r, 1, hit_point);
        refracted.origin = refracted.origin + kEpsilon * refracted.direction;
        refracted.inside = false;
        return GetI(&refracted, objects, sources, render_options, depth + 1);
    }

    RGBProperty ip{};
    ip = object.Ka() + object.Ke();

    for (const auto& source : sources) {
        if (VisibleLight(source, hit_point, objects, normal)) {
            Vec3 to_light = Vec3(source.place - hit_point).Normalize();
            ip += Diffuse(object, to_light, source.intensity, normal) +
                  Specular(object, -1 * ray.direction, to_light, source.intensity, normal);
        }
    }

    if (object.Illum().r > 2) {
        // reflection
        Ray reflected = Reflected(ray, normal, hit_point);
        reflected.origin = reflected.origin + kEpsilon * reflected.direction;

        RGBProperty intensity = GetI(&reflected, objects, sources, render_options, depth + 1);

        Vec3 light_place = reflected.origin + reflected.distance * reflected.direction;
        Vec3 vec_to_light = Vec3(light_place - hit_point).Normalize();

        ip += Diffuse(object, vec_to_light, intensity, normal) +
              Specular(object, -1 * ray.direction, vec_to_light, intensity, normal);
        // refraction
        if (object.Tr().r != 0) {
            assert(object.Ni().r != 0);
            Ray refracted = Refraction(ray, normal, 1, object.Ni().r, hit_point);
            refracted.origin = refracted.origin + kEpsilon * refracted.direction;
            refracted.inside = true;
            ip += object.Tr().r * GetI(&refracted, objects, sources, render_options, depth + 1);
        }
    }

    return ip;
}
#endif  // SHAD_CPP0_SHADING_H
