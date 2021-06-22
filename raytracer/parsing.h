#ifndef SHAD_CPP0_PARSING_H
#define SHAD_CPP0_PARSING_H

#include <cassert>
#include <fstream>
#include <string>
#include <vector>
#include <objects_class.h>
#include <iostream>
#include <unordered_map>
#include <cctype>

class FileStream {
public:
    FileStream(const std::string& filename) {
        input_.open(filename);
        assert(input_);
    }

    FileStream& operator>>(std::string& str) {
        Skip();
        if (input_.eof()) {
            return *this;
        }
        input_ >> str;
        return *this;
    }

    bool Eof() {
        Skip();
        return input_.eof();
    }

    std::ifstream input_;

private:
    void Skip() {
        char symbol = 0;
        symbol = input_.get();
        while (isspace(symbol) || symbol == '#') {
            if (symbol == '#') {
                std::string buf;
                std::getline(input_, buf);
            }
            symbol = input_.get();
        }
        if (input_.eof()) {
            return;
        }
        input_.unget();
    }
};

std::unordered_map<std::string, Properties> ParseMtlFile(FileStream& mtlfile);

void ParseObjFile(FileStream& file, ObjectSet& objectset,
                  std::unordered_map<std::string, Properties>& materials,
                  std::vector<LightSource>& sources);

void GetObjects(const std::string& filename, ObjectSet& objectset,
                std::vector<LightSource>& sources) {
    FileStream objfile(filename);

    std::string mtl_filename = filename.substr(0, filename.find_last_of("/") + 1);
    std::string str;
    objfile >> str;
    assert(str == "mtllib");
    objfile >> str;
    mtl_filename += str;
    FileStream mtlfile(mtl_filename);

    std::unordered_map<std::string, Properties> materials;
    materials = ParseMtlFile(mtlfile);

    ParseObjFile(objfile, objectset, materials, sources);
}
int ReadNumber(const std::string& str, size_t* idx) {
    int number = 0, sign = 1;
    while (*idx < str.size() && isspace(str[*idx])) {
        *idx += 1;
    }
    if (*idx < str.size() && str[*idx] == '-') {
        sign = -1;
        *idx += 1;
    }
    while (*idx < str.size() && isdigit(str[*idx])) {
        number = number * 10 + str[*idx] - '0';
        *idx += 1;
    }
    while (*idx < str.size() && isspace(str[*idx])) {
        *idx += 1;
    }
    return number * sign;
}

void ReadVertices(const std::string& str, std::vector<Vertex>& vertices,
                  const std::vector<Vec3>& veccoord, const std::vector<Vec3>& normals) {
    for (size_t i = 0; i < str.size();) {
        int vertex = 0, normal = 0;
        vertex = ReadNumber(str, &i);
        if (i < str.size() && str[i] == '/') {
            ++i;
            ReadNumber(str, &i);
            if (i < str.size() && str[i] == '/') {
                ++i;
                normal = ReadNumber(str, &i);
            }
        }
        if (vertex < 0) {
            vertex = static_cast<int>(veccoord.size()) + vertex;
        }
        if (normal < 0) {
            normal = static_cast<int>(normals.size()) + normal;
        }

        assert(vertex > 0 && normal >= 0);
        vertices.emplace_back(veccoord[vertex], normals[normal]);
        if (normal > 0) {
            vertices.back().defined_normal = true;
        }
    }
}

void InsertTriangles(ObjectSet& objset, const std::vector<Vertex>& vertices,
                     const Properties& properties) {
    assert(vertices.size() > 2);
    for (size_t i = 1; i + 1 < vertices.size(); ++i) {
        Object object{ObjType::Triangle, properties, vertices[0], vertices[i], vertices[i + 1]};
        objset.InsertObject(object);
    }
}

void ParseObjFile(FileStream& file, ObjectSet& objectset,
                  std::unordered_map<std::string, Properties>& materials,
                  std::vector<LightSource>& sources) {
    std::string str, material;
    std::vector<Vec3> vertices;
    std::vector<Vec3> normals;
    vertices.emplace_back();
    normals.emplace_back();

    while (!file.Eof()) {
        file >> str;
        if (str == "v") {
            double x, y, z;
            file.input_ >> x >> y >> z;
            vertices.emplace_back(x, y, z);
            continue;
        }
        if (str == "usemtl") {
            file.input_ >> material;
            continue;
        }
        if (str == "f") {
            std::vector<Vertex> objvertices;
            std::string strline;
            std::getline(file.input_, strline);
            ReadVertices(strline, objvertices, vertices, normals);
            assert(objvertices.size() > 2);
            InsertTriangles(objectset, objvertices, materials[material]);
            continue;
        }
        if (str == "vn") {
            double x, y, z;
            file.input_ >> x >> y >> z;
            normals.emplace_back(x, y, z);
            continue;
        }
        if (str == "S") {
            double x, y, z;
            file.input_ >> x >> y >> z;
            double radius = 0;
            file.input_ >> radius;
            objectset.InsertObject(
                Object{ObjType::Sphere, materials[material], Vec3{x, y, z}, radius});
            continue;
        }
        if (str == "P") {
            double x, y, z;
            file.input_ >> x >> y >> z;
            double r, g, b;
            file.input_ >> r >> g >> b;
            sources.emplace_back(LightSource{Vec3{x, y, z}, RGBProperty{r, g, b}});
        }
    }
}

std::unordered_map<std::string, Properties> ParseMtlFile(FileStream& mtlfile) {
    std::unordered_map<std::string, Properties> materials;
    std::vector<std::string> to_check{"Ka", "Ke", "Kd", "Ks", "Ns", "Tr", "Ni", "illum", "d"};

    std::string str, material_name;
    mtlfile >> str;
    assert(str == "newmtl");
    while (!mtlfile.Eof()) {

        mtlfile >> material_name;
        Properties properties;
        std::string property;
        mtlfile >> property;

        while (!mtlfile.Eof() && property != "newmtl") {
            std::string value;
            std::vector<double> values_rgb;

            mtlfile >> value;
            while (isdigit(value[0])) {
                values_rgb.push_back(std::stod(value));
                if (mtlfile.Eof()) {
                    break;
                }
                mtlfile >> value;
            }

            bool found = false;
            for (auto& prop : to_check) {
                if (property == prop) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                property = value;
                continue;
            }

            assert(!values_rgb.empty());
            if (values_rgb.size() == 1) {
                if (property == "d") {
                    properties.GetProperty("Tr") =
                        RGBProperty(1 - values_rgb[0], 1 - values_rgb[0], 1 - values_rgb[0]);
                } else {
                    properties.GetProperty(property) =
                        RGBProperty(values_rgb[0], values_rgb[0], values_rgb[0]);
                }
            } else {
                assert(values_rgb.size() == 3);
                properties.GetProperty(property) =
                    RGBProperty(values_rgb[0], values_rgb[1], values_rgb[2]);
            }

            property = value;
        }

        materials[material_name] = properties;
    }

    return materials;
}

#endif  // SHAD_CPP0_PARSING_H
