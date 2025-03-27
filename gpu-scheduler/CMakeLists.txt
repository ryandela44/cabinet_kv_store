cmake_minimum_required(VERSION 3.26)
project(gpu_scheduler)

# Make Protobuf module compatible.
set(protobuf_MODULE_COMPATIBLE ON)

# Set CMAKE_PREFIX_PATH to include gRPC and Protobuf installation directories.
set(CMAKE_PREFIX_PATH "${CMAKE_PREFIX_PATH};/opt/homebrew/lib/cmake/gRPC;/opt/homebrew/lib/cmake/protobuf")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

message(STATUS "Using Protobuf ${Protobuf_VERSION}")
message(STATUS "Using gRPC ${gRPC_VERSION}")

# Locate the grpc_cpp_plugin if not already found.
if(NOT DEFINED gRPC_CPP_PLUGIN_EXECUTABLE)
    find_program(gRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin PATHS /opt/homebrew/bin)
endif()
if(NOT gRPC_CPP_PLUGIN_EXECUTABLE)
    message(FATAL_ERROR "grpc_cpp_plugin not found. Make sure it is installed and in your PATH.")
else()
    message(STATUS "Found grpc_cpp_plugin: ${gRPC_CPP_PLUGIN_EXECUTABLE}")
endif()

# Assume the proto file is located in the "proto" directory.
set(PROTO_FILE "${CMAKE_CURRENT_SOURCE_DIR}/proto/gpu_status.proto")
get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)

# Specify output directory for generated files.
set(GEN_SRC_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${GEN_SRC_DIR})

set(PROTO_SRCS "${GEN_SRC_DIR}/${PROTO_NAME}.pb.cc")
set(PROTO_HDRS "${GEN_SRC_DIR}/${PROTO_NAME}.pb.h")
set(GRPC_SRCS "${GEN_SRC_DIR}/${PROTO_NAME}.grpc.pb.cc")
set(GRPC_HDRS "${GEN_SRC_DIR}/${PROTO_NAME}.grpc.pb.h")

# Generate protobuf and gRPC files.
add_custom_command(
        OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
        COMMAND ${Protobuf_PROTOC_EXECUTABLE}
        ARGS --grpc_out=${GEN_SRC_DIR}
        --cpp_out=${GEN_SRC_DIR}
        -I ${CMAKE_CURRENT_SOURCE_DIR}/proto
        --plugin=protoc-gen-grpc=${gRPC_CPP_PLUGIN_EXECUTABLE}
        ${PROTO_FILE}
        DEPENDS ${PROTO_FILE}
)

# List the source files.
set(SRCS
        main.cpp
        scheduler.cpp
        ${PROTO_SRCS}
        ${GRPC_SRCS}
)

add_executable(gpu_scheduler ${SRCS})
target_include_directories(gpu_scheduler PRIVATE ${GEN_SRC_DIR})
target_link_libraries(gpu_scheduler
        gRPC::grpc++
        protobuf::libprotobuf
        Threads::Threads
)
