cmake_minimum_required(VERSION 3.16)
project(distributed-in-memory-kv-store CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Compiler flags 
# Release: maximum optimisation
# Debug:   sanitisers for development
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
if(NOT APPLE)
  string(APPEND CMAKE_CXX_FLAGS_RELEASE " -march=native")
endif()
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -fno-omit-frame-pointer")

# Suppress some noisy but harmless warnings
add_compile_options(-Wall -Wextra -Wno-unused-parameter)

#  Threads
find_package(Threads REQUIRED)

# Source files 
set(LIB_SOURCES
    src/resp.cpp
    src/store.cpp
    src/eviction.cpp
    src/wal.cpp
    src/server.cpp
    src/rdb.cpp
    src/replication.cpp
    src/replica_client.cpp
)

#  Main binary 
add_executable(kvstore
    src/main.cpp
    ${LIB_SOURCES}
)
target_link_libraries(kvstore Threads::Threads)

#  Tests 
add_executable(run-tests
    tests/test_all.cpp
    src/resp.cpp
    src/store.cpp
    src/eviction.cpp
    src/wal.cpp
    src/rdb.cpp
    src/replication.cpp
    src/replica_client.cpp
)
target_include_directories(run-tests PRIVATE src)
target_link_libraries(run-tests Threads::Threads)

# macOS specifics 
if(APPLE)
    # macOS clang needs this for std::thread stack size control
    target_compile_options(kvstore    PRIVATE -pthread)
    target_compile_options(run-tests  PRIVATE -pthread)
else()
    message(STATUS "Platform: Linux — using epoll event loop")
endif()

message(STATUS "Binary name: kvstore")
message(STATUS "Build type:  ${CMAKE_BUILD_TYPE}")
