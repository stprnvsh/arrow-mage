// src/init.cpp
// This file helps R recognize that the package needs compilation and linking.
// It can also be used for C function registration if needed.

#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>

// Define a minimal function to ensure compilation happens
extern "C" void R_init_CrossLink_dummy(DllInfo *dll) {
    // Register routines, allocate resources, etc., if needed
    // For now, just an empty function to satisfy R CMD INSTALL
} 