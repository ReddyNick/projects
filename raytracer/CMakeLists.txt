add_catch(test_raytracer test.cpp)

target_compile_definitions(test_raytracer PUBLIC SHAD_TASK_DIR="${CMAKE_CURRENT_SOURCE_DIR}/")

if (GRADER)
  target_compile_definitions(test_raytracer PUBLIC SHAD_ARTIFACTS_DIR="/tmp/artifacts")
endif()

target_link_libraries(test_raytracer ${PNG_LIBRARY} ${JPEG_LIBRARIES})
