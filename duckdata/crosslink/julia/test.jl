using CxxWrap

# Set the path to the crosslink_jl library
const crosslink_path = joinpath(@__DIR__, "lib", "crosslink_jl.dylib")

# Try to load the library
println("Attempting to load library from: ", crosslink_path)
if isfile(crosslink_path)
    try
        @eval module CrossLink
            using CxxWrap
            @wrapmodule(() -> $crosslink_path)
            function __init__()
                @initcxx
            end
        end
        println("SUCCESS: Library loaded successfully!")
        # If loaded, let's try to create an instance
        try
            using .CrossLink
            cl = CrossLink.CrossLinkCpp("test_instance", true)
            println("SUCCESS: Created CrossLink instance")
            println("Available datasets: ", CrossLink.list_datasets(cl))
        catch e
            println("ERROR: Could not create CrossLink instance: ", e)
        end
    catch e
        println("ERROR: Failed to load the library: ", e)
    end
else
    println("ERROR: Library file not found at path: ", crosslink_path)
end 