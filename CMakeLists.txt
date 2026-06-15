cmake_minimum_required(VERSION 3.4.1)

project(nacos-sdk-cpp)

#add_definitions(-DNACOS_AUTH)
#add_definitions(-DNACOS_SPAS)

include_directories (
.
include
)

set(NACOS_PROTO_DIR ${CMAKE_SOURCE_DIR}/proto)
set(NACOS_PROTO_FILE ${NACOS_PROTO_DIR}/nacos_grpc_service.proto)

if (EXISTS ${NACOS_PROTO_FILE})
    find_package(Protobuf REQUIRED)
    include_directories(${Protobuf_INCLUDE_DIRS})

    find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
    if (NOT GRPC_CPP_PLUGIN_EXECUTABLE)
        message(FATAL_ERROR "grpc_cpp_plugin not found")
    endif()

    find_package(PkgConfig REQUIRED)
    pkg_check_modules(GRPC REQUIRED grpc++)
    include_directories(${GRPC_INCLUDE_DIRS})
    file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/generated)

    set(PROTO_GEN_SRCS
        ${CMAKE_BINARY_DIR}/generated/nacos_grpc_service.pb.cc
        ${CMAKE_BINARY_DIR}/generated/nacos_grpc_service.grpc.pb.cc)
    set(PROTO_GEN_HDRS
        ${CMAKE_BINARY_DIR}/generated/nacos_grpc_service.pb.h
        ${CMAKE_BINARY_DIR}/generated/nacos_grpc_service.grpc.pb.h)

    add_custom_command(
        OUTPUT ${PROTO_GEN_SRCS} ${PROTO_GEN_HDRS}
        COMMAND ${Protobuf_PROTOC_EXECUTABLE}
                --proto_path=${NACOS_PROTO_DIR}
                --cpp_out=${CMAKE_BINARY_DIR}/generated
                ${NACOS_PROTO_FILE}
        COMMAND ${Protobuf_PROTOC_EXECUTABLE}
                --proto_path=${NACOS_PROTO_DIR}
                --grpc_out=${CMAKE_BINARY_DIR}/generated
                --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
                ${NACOS_PROTO_FILE}
        DEPENDS ${NACOS_PROTO_FILE}
        COMMENT "Generating gRPC sources from ${NACOS_PROTO_FILE}"
        VERBATIM)

    include_directories(${CMAKE_BINARY_DIR}/generated)
endif()

aux_source_directory( src SRCS_DIRS)
aux_source_directory( src/config SRCS_DIRS)
aux_source_directory( src/constant SRCS_DIRS)
aux_source_directory( src/crypto SRCS_DIRS)
aux_source_directory( src/crypto/base64 SRCS_DIRS)
aux_source_directory( src/crypto/hmac_sha1 SRCS_DIRS)
aux_source_directory( src/crypto/hmac_sha1/hmac SRCS_DIRS)
aux_source_directory( src/crypto/hmac_sha1/sha SRCS_DIRS)
aux_source_directory( src/crypto/md5 SRCS_DIRS)
aux_source_directory( src/factory SRCS_DIRS)
aux_source_directory( src/http SRCS_DIRS)
aux_source_directory( src/http/delegate SRCS_DIRS)
aux_source_directory( src/init SRCS_DIRS)
aux_source_directory( src/json SRCS_DIRS)
aux_source_directory( src/json/rapidjson SRCS_DIRS)
aux_source_directory( src/json/rapidjson/error SRCS_DIRS)
aux_source_directory( src/json/rapidjson/internal SRCS_DIRS)
aux_source_directory( src/json/rapidjson/msinttypes SRCS_DIRS)
aux_source_directory( src/listen SRCS_DIRS)
aux_source_directory( src/log SRCS_DIRS)
aux_source_directory( src/naming SRCS_DIRS)
aux_source_directory( src/naming/beat SRCS_DIRS)
aux_source_directory( src/naming/cache SRCS_DIRS)
aux_source_directory( src/naming/selectors SRCS_DIRS)
aux_source_directory( src/naming/subscribe SRCS_DIRS)
aux_source_directory( src/naming/grpc SRCS_DIRS)
aux_source_directory( src/naming/registry SRCS_DIRS)
aux_source_directory( src/security SRCS_DIRS)
aux_source_directory( src/server SRCS_DIRS)
aux_source_directory( src/thread SRCS_DIRS)
aux_source_directory( src/utils SRCS_DIRS)
aux_source_directory( test SRCS_DIRS)
aux_source_directory( test/testcase SRCS_DIRS)

aux_source_directory(test TEST_SRCS_DIRS)
aux_source_directory(test/testcase TEST_SRCS_DIRS)

if (EXISTS ${NACOS_PROTO_FILE})
    add_custom_target(nacos_grpc_proto DEPENDS ${PROTO_GEN_SRCS} ${PROTO_GEN_HDRS})
    set(SRCS_DIRS ${SRCS_DIRS} ${PROTO_GEN_SRCS})
endif()

add_executable(nacos-cli.out ${TEST_SRCS_DIRS} ${SRCS_DIRS})
add_library(nacos-cli SHARED ${SRCS_DIRS})
add_library(nacos-cli-static STATIC ${SRCS_DIRS})

if (TARGET nacos_grpc_proto)
    add_dependencies(nacos-cli.out nacos_grpc_proto)
    add_dependencies(nacos-cli nacos_grpc_proto)
    add_dependencies(nacos-cli-static nacos_grpc_proto)
endif()

set(THREADS_PREFER_PTHREAD_FLAG ON)
set(NACOS_LINK_LIBS Threads::Threads curl z)
if (EXISTS ${NACOS_PROTO_FILE})
    list(APPEND NACOS_LINK_LIBS ${GRPC_LIBRARIES} ${Protobuf_LIBRARIES})
endif()

target_link_libraries(nacos-cli PRIVATE ${NACOS_LINK_LIBS})
target_link_libraries(nacos-cli-static PRIVATE ${NACOS_LINK_LIBS})
target_link_libraries(nacos-cli.out PRIVATE ${NACOS_LINK_LIBS})

add_executable(grpc-sub.out examples/grpc_subscribe_demo.cpp)
target_link_libraries(grpc-sub.out PRIVATE nacos-cli-static ${NACOS_LINK_LIBS})
if (TARGET nacos_grpc_proto)
    add_dependencies(grpc-sub.out nacos_grpc_proto)
    target_compile_options(grpc-sub.out PRIVATE ${GRPC_CFLAGS_OTHER})
endif()

add_executable(discovery-register-demo.out examples/discovery_register_demo.cpp)
target_link_libraries(discovery-register-demo.out PRIVATE nacos-cli-static ${NACOS_LINK_LIBS})
if (TARGET nacos_grpc_proto)
    add_dependencies(discovery-register-demo.out nacos_grpc_proto)
    target_compile_options(discovery-register-demo.out PRIVATE ${GRPC_CFLAGS_OTHER})
endif()

if (EXISTS ${NACOS_PROTO_FILE})
    target_compile_options(nacos-cli PRIVATE ${GRPC_CFLAGS_OTHER})
    target_compile_options(nacos-cli-static PRIVATE ${GRPC_CFLAGS_OTHER})
    target_compile_options(nacos-cli.out PRIVATE ${GRPC_CFLAGS_OTHER})
endif()
find_package(Threads REQUIRED)
find_package(CURL REQUIRED)
find_package(ZLIB REQUIRED)

install(TARGETS nacos-cli
		LIBRARY DESTINATION lib)

install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/
		DESTINATION include/nacos
		FILES_MATCHING PATTERN "*.h*")
