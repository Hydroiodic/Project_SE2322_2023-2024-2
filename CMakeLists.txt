cmake_minimum_required(VERSION 3.22)

project(LSMTree)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_COMPILER "clang++")
set(CMAKE_CXX_FLAGS "-Ofast")

# remember to comment this line when the project is done
# set(CMAKE_BUILD_TYPE "Debug")

add_subdirectory(memTable)
add_subdirectory(skipList)
add_subdirectory(ssTable)
add_subdirectory(vLog)
add_subdirectory(levelManager)

# add_executable(${PROJECT_NAME} main.cpp kvstore.cpp)
add_executable(${PROJECT_NAME} correctness.cpp kvstore.cpp)
# add_executable(${PROJECT_NAME} persistence.cpp kvstore.cpp)

target_link_libraries(${PROJECT_NAME} memTable skipList ssTable vLog levelManager)
