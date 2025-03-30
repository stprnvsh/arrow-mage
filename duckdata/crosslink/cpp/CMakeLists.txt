cmake_minimum_required(VERSION 3.14)
project(crosslink_cpp VERSION 0.1.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Add our custom CMake modules path
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# Find required dependencies
find_package(Arrow REQUIRED)
find_package(DuckDB REQUIRED)

# Add link directories for libraries
link_directories(/opt/homebrew/lib)

# Add core library sources
add_library(crosslink_core SHARED
    src/crosslink.cpp
    core/shared_memory_manager.cpp
    core/arrow_bridge.cpp
    core/metadata_manager.cpp
    core/notification_system.cpp
)

# Setup include directories
target_include_directories(crosslink_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/core
    ${Arrow_INCLUDE_DIRS}
    ${DuckDB_INCLUDE_DIRS}
)

# Link with dependencies
target_link_libraries(crosslink_core PUBLIC
    Arrow::arrow_shared
    ${DuckDB_LIBRARIES}
)

# Set output name
set_target_properties(crosslink_core PROPERTIES
    OUTPUT_NAME crosslink
)

# Python bindings
option(BUILD_PYTHON_BINDINGS "Build Python bindings" ON)
if(BUILD_PYTHON_BINDINGS)
    find_package(pybind11 REQUIRED)
    find_package(Python COMPONENTS Interpreter Development REQUIRED)
    
    pybind11_add_module(crosslink_python_binding bindings/python_binding.cpp)
    
    # Find PyArrow include directories
    execute_process(
        COMMAND python3 -c "import pyarrow; print(pyarrow.get_include())"
        OUTPUT_VARIABLE PYARROW_INCLUDE_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    
    # Link with crosslink_core
    target_link_libraries(crosslink_python_binding PRIVATE crosslink_core)
    
    # Add PyArrow include directories
    target_include_directories(crosslink_python_binding PRIVATE 
        ${PYARROW_INCLUDE_DIR}
    )
    
    # Set output name
    set_target_properties(crosslink_python_binding PROPERTIES
        OUTPUT_NAME crosslink_cpp
    )
    
    # Install to project's python directory instead of site-packages
    install(TARGETS crosslink_python_binding
            DESTINATION "${CMAKE_CURRENT_SOURCE_DIR}/../python/crosslink"
    )
endif()

# R bindings
option(BUILD_R_BINDINGS "Build R bindings" ON)
if(BUILD_R_BINDINGS)
    # Use Rcpp to find R
    execute_process(
        COMMAND Rscript -e "cat(system.file('include', package='Rcpp'))"
        OUTPUT_VARIABLE RCPP_INCLUDE_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET # Suppress errors if Rcpp not found
    )
    
    execute_process(
        COMMAND Rscript -e "cat(R.home('include'))"
        OUTPUT_VARIABLE R_INCLUDE_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET # Suppress errors if R not found
    )
    
    execute_process(
        COMMAND Rscript -e "cat(R.home('lib'))"
        OUTPUT_VARIABLE R_LIB_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )

    # Find R Arrow C++ include directory
    execute_process(
        COMMAND Rscript -e "dir_path <- system.file('include', package = 'arrow'); if (nchar(dir_path) == 0) { stop('Arrow R package not found or include directory missing.') }; cat(dir_path)"
        OUTPUT_VARIABLE R_ARROW_INCLUDE_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE
        RESULT_VARIABLE R_ARROW_FIND_RESULT
        ERROR_VARIABLE R_ARROW_FIND_ERROR
    )

    if(NOT R_ARROW_FIND_RESULT EQUAL 0 OR NOT R_ARROW_INCLUDE_DIR)
        message(WARNING "Rscript failed to find R Arrow package include directory. Trying pkg-config. Error: ${R_ARROW_FIND_ERROR}")
        # Fallback using pkg-config
        find_package(PkgConfig REQUIRED)
        pkg_check_modules(ARROW arrow)
        if(ARROW_FOUND)
            set(R_ARROW_INCLUDE_DIR ${ARROW_INCLUDE_DIRS}) # Use the include dirs found by pkg-config
            message(STATUS "Found Arrow include dir via pkg-config: ${R_ARROW_INCLUDE_DIR}")
        else()
            message(WARNING "pkg-config also failed to find Arrow include directory. R bindings might fail.")
            set(R_ARROW_INCLUDE_DIR "") # Ensure it's empty if not found
        endif()
    else()
        message(STATUS "Found R Arrow include dir via Rscript: ${R_ARROW_INCLUDE_DIR}")
    endif()

    # Check if essential R paths were found
    if(NOT RCPP_INCLUDE_DIR OR NOT R_INCLUDE_DIR)
        message(FATAL_ERROR "Rcpp or R include directory not found. Cannot build R bindings.")
    endif()
    
    message(STATUS "Rcpp include dir: ${RCPP_INCLUDE_DIR}")
    message(STATUS "R include dir: ${R_INCLUDE_DIR}")
    message(STATUS "R lib dir: ${R_LIB_DIR}")
    
    add_library(crosslink_r_binding SHARED bindings/r_binding.cpp)
    
    # Link with crosslink_core
    target_link_libraries(crosslink_r_binding PRIVATE crosslink_core)
    
    # Link with R
    target_link_libraries(crosslink_r_binding PRIVATE
        "${R_LIB_DIR}/libR.dylib"
    )
    
    # Add Rcpp and R Arrow include directories
    target_include_directories(crosslink_r_binding PRIVATE 
        "${RCPP_INCLUDE_DIR}"
        "${R_INCLUDE_DIR}"
        "${R_ARROW_INCLUDE_DIR}"  # Use the R-specific Arrow include path found earlier
    )
    
    # Set output name
    set_target_properties(crosslink_r_binding PROPERTIES
        OUTPUT_NAME crosslink_r
        PREFIX ""
    )
    
    # Install to R package
    install(TARGETS crosslink_r_binding
            DESTINATION "${CMAKE_CURRENT_SOURCE_DIR}/../r/libs"
    )
endif()

# Julia bindings
option(BUILD_JULIA_BINDINGS "Build Julia bindings" ON)
if(BUILD_JULIA_BINDINGS)
    # Find Julia
    find_package(Julia REQUIRED)
    message(STATUS "Found Julia ${Julia_VERSION} at: ${Julia_EXECUTABLE}")
    message(STATUS "Julia include dirs: ${Julia_INCLUDE_DIRS}")
    message(STATUS "Julia libraries: ${Julia_LIBRARIES}")

    # Find JlCxx
    execute_process(
        COMMAND ${Julia_EXECUTABLE} -e "using Pkg; Pkg.add(\"CxxWrap\"); using CxxWrap; print(CxxWrap.prefix_path())"
        OUTPUT_VARIABLE JLCXX_PREFIX_PATH
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    
    if(NOT JLCXX_PREFIX_PATH)
        message(FATAL_ERROR "JlCxx package was not found. Installed CxxWrap but couldn't get prefix path.")
    endif()
    
    message(STATUS "Found JlCxx at: ${JLCXX_PREFIX_PATH}")
    set(CMAKE_PREFIX_PATH "${CMAKE_PREFIX_PATH};${JLCXX_PREFIX_PATH}")
    find_package(JlCxx REQUIRED)

    # Create Julia binding library with simplified dependencies
    add_library(crosslink_julia_binding SHARED
        bindings/julia_binding.cpp
    )
    
    # Link with crosslink_core and JlCxx only
    target_link_libraries(crosslink_julia_binding PRIVATE 
        crosslink_core
        JlCxx::cxxwrap_julia
        # ${JlCxx_LIBRARIES} # Rely on imported target (Commented out again)
        # Explicitly link the underlying libcxxwrap_julia library (Commented out)
        # "/Users/pranavsateesh/.julia/artifacts/a339b092561f57ff8cfb0fd8a100a8d5380695f6/lib/libcxxwrap_julia.0.13.4.dylib"
        # Explicitly link the STL library
        "/Users/pranavsateesh/.julia/artifacts/a339b092561f57ff8cfb0fd8a100a8d5380695f6/lib/libcxxwrap_julia_stl.dylib"
    )
    
    # Include necessary headers - explicitly declare what we need
    target_include_directories(crosslink_julia_binding PRIVATE 
        ${Julia_INCLUDE_DIRS}
        ${JlCxx_INCLUDE_DIRS} # Add JlCxx includes
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/core
        ${Arrow_INCLUDE_DIRS}
    )
    
    # Set output name and rpath
    set_target_properties(crosslink_julia_binding PROPERTIES
        OUTPUT_NAME crosslink_jl
        PREFIX ""
        INSTALL_RPATH "@loader_path"
    )
    
    # Make sure library directories exist
    file(MAKE_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../julia/lib")
    file(MAKE_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../julia/deps")
    
    # Install to Julia package directory
    install(TARGETS crosslink_julia_binding
            DESTINATION "${CMAKE_CURRENT_SOURCE_DIR}/../julia/lib"
    )
    
    # Also install the core library to Julia lib directory
    install(TARGETS crosslink_core
            LIBRARY DESTINATION "${CMAKE_CURRENT_SOURCE_DIR}/../julia/lib"
    )
endif()

# Install headers to project directory instead of system directory
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/
        DESTINATION "${CMAKE_CURRENT_SOURCE_DIR}/../include/crosslink"
)

# Install the core library to project directory
install(TARGETS crosslink_core
        LIBRARY DESTINATION "${CMAKE_CURRENT_SOURCE_DIR}/../lib"
        ARCHIVE DESTINATION "${CMAKE_CURRENT_SOURCE_DIR}/../lib"
)

# Add examples
# add_subdirectory(examples) 