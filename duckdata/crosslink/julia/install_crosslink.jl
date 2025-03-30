using Pkg

# Make sure we have the General registry
Pkg.Registry.update()

# Add the package directly by path
println("Adding CrossLink package...")
Pkg.add(path="/Users/pranavsateesh/arrow-mage/duckdata/crosslink/julia")

println("CrossLink package installation completed!")
