cmake_minimum_required(VERSION 3.20)
project(drift_detector VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# --- Dependencies via vcpkg (set VCPKG_ROOT or pass -DCMAKE_TOOLCHAIN_FILE) ---
find_package(PkgConfig REQUIRED)

# librdkafka
pkg_check_modules(RDKAFKA REQUIRED rdkafka++)

# libpqxx
find_package(libpqxx QUIET)
if(NOT libpqxx_FOUND)
    pkg_check_modules(PQXX REQUIRED libpqxx)
endif()

# nlohmann_json
find_package(nlohmann_json REQUIRED)

# xxHash (header-only mode or system install)
find_package(xxHash QUIET)

# --- Source files ---
set(COMMON_SOURCES
    src/chunker/chunker.cpp
    src/hasher/hasher.cpp
    src/comparator/comparator.cpp
    src/kafka/producer.cpp
)

# --- Main detector binary ---
add_executable(drift_detector
    src/main.cpp
    ${COMMON_SOURCES}
)

target_include_directories(drift_detector PRIVATE
    src/
    ${RDKAFKA_INCLUDE_DIRS}
    ${PQXX_INCLUDE_DIRS}
)

target_link_libraries(drift_detector PRIVATE
    ${RDKAFKA_LIBRARIES}
    ${PQXX_LIBRARIES}
    nlohmann_json::nlohmann_json
    pthread
)

target_compile_options(drift_detector PRIVATE
    -Wall -Wextra -O2
)

# --- Kafka consumer binary (separate process) ---
add_executable(drift_consumer
    src/consumer/consumer_main.cpp
    src/kafka/producer.cpp   # reuse serialisation helpers
)

target_include_directories(drift_consumer PRIVATE
    src/
    ${RDKAFKA_INCLUDE_DIRS}
    ${PQXX_INCLUDE_DIRS}
)

target_link_libraries(drift_consumer PRIVATE
    ${RDKAFKA_LIBRARIES}
    nlohmann_json::nlohmann_json
    pthread
)

# --- Install ---
install(TARGETS drift_detector drift_consumer DESTINATION bin)
