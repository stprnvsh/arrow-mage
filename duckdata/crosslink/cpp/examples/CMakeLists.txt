cmake_minimum_required(VERSION 3.14)

# Flight example
add_executable(flight_example flight_example.cpp)
target_link_libraries(flight_example PRIVATE crosslink_core)
target_include_directories(flight_example PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

# Set the runtime path to find the shared library
if(APPLE)
    set_target_properties(flight_example PROPERTIES
        BUILD_WITH_INSTALL_RPATH TRUE
        INSTALL_RPATH "@executable_path/../lib"
    )
elseif(UNIX)
    set_target_properties(flight_example PROPERTIES
        BUILD_WITH_INSTALL_RPATH TRUE
        INSTALL_RPATH "$ORIGIN/../lib"
    )
endif()

# Install the examples
install(TARGETS flight_example
        RUNTIME DESTINATION "${CMAKE_CURRENT_SOURCE_DIR}/../../bin"
) 