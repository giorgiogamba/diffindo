cmake_minimum_required(VERSION 3.20)
project(diffindo VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(PkgConfig REQUIRED)
pkg_check_modules(PQXX REQUIRED libpqxx)
pkg_check_modules(XXHASH REQUIRED libxxhash)

add_executable(test_chunker
    src/chunker/chunker.cpp
    src/test_chunker.cpp
)

target_include_directories(test_chunker PRIVATE
    src/
    ${PQXX_INCLUDE_DIRS}
)

target_link_libraries(test_chunker PRIVATE
    ${PQXX_LIBRARIES}
    pthread
)

target_compile_options(test_chunker PRIVATE
    -Wall -Wextra -O2
)

add_executable(test_hasher
    src/chunker/chunker.cpp
    src/hasher/hasher.cpp
    src/test_hasher.cpp
)

target_include_directories(test_hasher PRIVATE
    src/
    ${PQXX_INCLUDE_DIRS}
    ${XXHASH_INCLUDE_DIRS}
)

target_link_libraries(test_hasher PRIVATE
    ${PQXX_LIBRARIES}
    ${XXHASH_LIBRARIES}
    pthread
)

target_compile_options(test_hasher PRIVATE
    -Wall -Wextra -O2
)
