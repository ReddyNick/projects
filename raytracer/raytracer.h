#pragma once

#include <image.h>
#include <camera_options.h>
#include <render_options.h>
#include <string>

#include <parsing.h>
#include <objects_class.h>
#include <geometry.h>
#include <shading.h>

#include <iostream>
#include <fstream>

void MakeImage(Image& image, const std::vector<std::vector<RGBProperty>>& image_matrix);

Image Render(const std::string& filename, const CameraOptions& camera_options,
             const RenderOptions& render_options) {
    ObjectSet objects;
    std::vector<LightSource> sources;
    GetObjects(filename, objects, sources);

    int screen_width = camera_options.screen_width;
    int screen_height = camera_options.screen_height;

    std::vector<std::vector<RGBProperty>> image_matrix(screen_height,
                                                       std::vector<RGBProperty>(screen_width));

    std::vector<std::vector<double>> to_world_matrix(3, std::vector<double>(3, 0));
    MakeToWorldMatrix(camera_options, to_world_matrix);

    for (int pix_height = 0; pix_height < screen_height; ++pix_height) {
        for (int pix_width = 0; pix_width < screen_width; ++pix_width) {

            Ray ray =
                MakeCameraRay(camera_options, pix_height, pix_width, screen_height, screen_width);

            ray = CameraToWorld(ray, camera_options, to_world_matrix);
            image_matrix[pix_height][pix_width] = GetI(&ray, objects, sources, render_options, 1);
        }
    }

    Image image(screen_width, screen_height);
    MakeImage(image, image_matrix);
    return image;
}

void MakeImage(Image& image, const std::vector<std::vector<RGBProperty>>& image_matrix) {
    double max_pix = image_matrix[0][0].r;

    for (int i = 0; i < image.Height(); ++i) {
        for (int k = 0; k < image.Width(); ++k) {
            const RGBProperty& pixel = image_matrix[i][k];
            max_pix = std::max(max_pix, std::max(pixel.r, std::max(pixel.g, pixel.b)));
        }
    }

    if (fabs(max_pix) < kComparisonThreshold) {
        return;
    }

    for (int height = 0; height < image.Height(); ++height) {
        for (int width = 0; width < image.Width(); ++width) {
            RGBProperty pixel = image_matrix[height][width];

            pixel = pixel * (1.0 + pixel / (max_pix * max_pix)) / (1.0 + pixel);
            pixel = Pow(pixel, 1 / 2.2);
            pixel = pixel * 255;

            assert(pixel.r < 255 + kComparisonThreshold && pixel.g < 255 + kComparisonThreshold &&
                   pixel.b < 255 + kComparisonThreshold);
            RGB rgb;
            rgb.r = static_cast<int>(pixel.r);
            rgb.g = static_cast<int>(pixel.g);
            rgb.b = static_cast<int>(pixel.b);
            image.SetPixel(rgb, height, width);
        }
    }
}
