# FindJulia.cmake
# Find Julia executable and libraries

find_program(Julia_EXECUTABLE julia DOC "Julia executable")
if(NOT Julia_EXECUTABLE)
  message(FATAL_ERROR "Julia executable not found")
endif()

# Get Julia version
execute_process(
  COMMAND ${Julia_EXECUTABLE} --version
  OUTPUT_VARIABLE Julia_VERSION_STRING
  ERROR_VARIABLE Julia_ERROR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

if(Julia_ERROR)
  message(FATAL_ERROR "Error fetching Julia version: ${Julia_ERROR}")
endif()

# Extract version using regex
string(REGEX MATCH "([0-9]+\\.[0-9]+\\.[0-9]+)" Julia_VERSION "${Julia_VERSION_STRING}")

# Find Julia include directories
execute_process(
  COMMAND ${Julia_EXECUTABLE} -e "print(joinpath(match(r\"(.*)(bin)\", Sys.BINDIR).captures[1], \"include\", \"julia\"))"
  OUTPUT_VARIABLE Julia_INCLUDE_DIRS
  ERROR_VARIABLE Julia_ERROR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

if(Julia_ERROR)
  message(FATAL_ERROR "Error fetching Julia include directories: ${Julia_ERROR}")
endif()

# Find Julia library directory
execute_process(
  COMMAND ${Julia_EXECUTABLE} -e "print(abspath(joinpath(match(r\"(.*)(bin)\", Sys.BINDIR).captures[1], \"lib\")))"
  OUTPUT_VARIABLE Julia_LIBRARY_DIR
  ERROR_VARIABLE Julia_ERROR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

if(Julia_ERROR)
  message(FATAL_ERROR "Error fetching Julia library directory: ${Julia_ERROR}")
endif()

# Find library
find_library(Julia_LIBRARY
  NAMES julia libjulia
  PATHS ${Julia_LIBRARY_DIR}
  NO_DEFAULT_PATH
)

if(NOT Julia_LIBRARY)
  message(FATAL_ERROR "Julia library not found")
endif()

# Handle standard find_package arguments
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Julia
  REQUIRED_VARS Julia_EXECUTABLE Julia_INCLUDE_DIRS Julia_LIBRARY
  VERSION_VAR Julia_VERSION
)

# Set Julia_LIBRARIES variable
set(Julia_LIBRARIES ${Julia_LIBRARY})

# Mark as advanced variables
mark_as_advanced(
  Julia_EXECUTABLE
  Julia_INCLUDE_DIRS
  Julia_LIBRARY
  Julia_LIBRARIES
  Julia_VERSION
) 