# Julia Bindings TODO

This file contains steps to fix and enable the Julia bindings for CrossLink.

## Current Issues

The Julia bindings compilation is currently failing due to:

1. ~~Missing or incompatible Arrow C++ header files~~ ✅ Fixed by including the proper Arrow C++ headers
2. ~~Issues with ArrowArray, ArrowSchema, and ArrowArrayStream struct definitions~~ ✅ Fixed by using the proper headers
3. ~~Type mismatch in `cpp_table_to_julia_c_data` function~~ ✅ Fixed by correctly converting Table to RecordBatch
4. **NEW ERROR**: Linker error when building `crosslink_jl.dylib`:
   ```
   Undefined symbols for architecture arm64:
     "jlcxx::stl::StlWrappers::instance()", referenced from:
         void jlcxx::stl::apply_stl<...>(jlcxx::Module&) in julia_binding.cpp.o
         ...
   ld: symbol(s) not found for architecture arm64
   ```

## Steps to Fix Julia Bindings

### 1. Fix Linker Error in CMake Configuration

The file `duckdata/crosslink/cpp/CMakeLists.txt` needed investigation to ensure the `jlcxx` (CxxWrap) library, specifically the component containing STL wrappers, is correctly linked to the `crosslink_julia_binding` target.

**Solution:**
- The `jlcxx::stl::apply_stl<std::string>(mod);` function was added to `bindings/julia_binding.cpp` to ensure STL wrappers were instantiated.
- The specific STL library `libcxxwrap_julia_stl.dylib` (found within the `libcxxwrap_julia_jll` artifact) was explicitly linked in `CMakeLists.txt` using its full path:
  ```cmake
  target_link_libraries(crosslink_julia_binding PRIVATE 
      crosslink_core
      JlCxx::cxxwrap_julia
      # Explicitly link the STL library
      "/Users/pranavsateesh/.julia/artifacts/a339b092561f57ff8cfb0fd8a100a8d5380695f6/lib/libcxxwrap_julia_stl.dylib"
  )
  ```
  *Note: Using a hardcoded path is not ideal for portability. A better long-term solution might involve improving how `find_package(JlCxx)` discovers and exports this library, or using CMake logic to find the artifact path dynamically.*

### 2. Install Required Julia Packages

```julia
using Pkg
Pkg.add("CxxWrap")
Pkg.add("libcxxwrap_julia_jll")
Pkg.add("Arrow")
```

### 3. Test Integration

After Julia bindings build successfully:

1. Test basic functionality with a simple Julia script
2. Verify two-way data transfer works correctly
3. Check that all CrossLink features are accessible from Julia

## Resources

- Arrow C Data Interface: https://arrow.apache.org/docs/format/CDataInterface.html
- CxxWrap.jl documentation: https://github.com/JuliaInterop/CxxWrap.jl
- Arrow.jl documentation: https://github.com/apache/arrow-julia
- libcxxwrap-julia documentation: https://github.com/JuliaInterop/libcxxwrap-julia 