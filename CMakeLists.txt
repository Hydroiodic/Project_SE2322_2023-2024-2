cmake_minimum_required(VERSION 3.22)

project(LSMTree)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_subdirectory(memTable)
add_subdirectory(skipList)
add_subdirectory(ssTable)
add_subdirectory(vLog)

add_executable(${PROJECT_NAME} main.cpp kvstore.cpp)

target_link_libraries(${PROJECT_NAME} memTable skipList ssTable vLog)
