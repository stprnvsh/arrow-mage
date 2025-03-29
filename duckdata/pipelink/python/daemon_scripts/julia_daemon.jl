#!/usr/bin/env julia
# Julia daemon server for PipeLink

using Sockets
using JSON
using Printf

# Helper to convert bytes to integer
function bytes_to_int(bytes)
    return (Int(bytes[1]) << 24) | (Int(bytes[2]) << 16) | (Int(bytes[3]) << 8) | Int(bytes[4])
end

# Helper to convert integer to bytes
function int_to_bytes(n)
    return [
        UInt8((n >> 24) & 0xFF),
        UInt8((n >> 16) & 0xFF),
        UInt8((n >> 8) & 0xFF),
        UInt8(n & 0xFF)
    ]
end

function handle_client(client)
    try
        # Read message size (4 bytes)
        size_bytes = read(client, 4)
        if isempty(size_bytes)
            return
        end
        
        msg_size = bytes_to_int(size_bytes)
        
        # Read message data
        data = Vector{UInt8}()
        remaining = msg_size
        while remaining > 0
            chunk_size = min(4096, remaining)
            chunk = read(client, chunk_size)
            if isempty(chunk)
                break
            end
            
            append!(data, chunk)
            remaining -= length(chunk)
        end
        
        # Parse request
        request_text = String(data)
        request = JSON.parse(request_text)
        
        if request["action"] == "shutdown"
            @printf("Daemon shutdown requested\n")
            response = Dict("status" => "ok", "message" => "Shutting down")
            
            # Send response then exit
            response_data = Vector{UInt8}(JSON.json(response))
            response_size = length(response_data)
            
            write(client, int_to_bytes(response_size))
            write(client, response_data)
            
            exit(0)
        elseif request["action"] == "execute"
            # Execute script
            script_path = request["script_path"]
            meta_path = request["meta_path"]
            working_dir = request["working_dir"]
            
            @printf("Executing script: %s\n", script_path)
            
            # Set environment variable to encourage zero-copy
            old_env = get(ENV, "PIPELINK_ENABLE_ZERO_COPY", "")
            ENV["PIPELINK_ENABLE_ZERO_COPY"] = "1"
            
            # Execute command
            old_wd = pwd()
            cd(working_dir)
            
            stdout_file = tempname()
            stderr_file = tempname()
            
            cmd = `julia $script_path $meta_path`
            cmd_stdout = open(stdout_file, "w")
            cmd_stderr = open(stderr_file, "w")
            
            process = run(pipeline(cmd, stdout=cmd_stdout, stderr=cmd_stderr), wait=true)
            exit_code = process.exitcode
            
            close(cmd_stdout)
            close(cmd_stderr)
            
            # Restore environment
            ENV["PIPELINK_ENABLE_ZERO_COPY"] = old_env
            cd(old_wd)
            
            # Read output
            stdout_text = ""
            if isfile(stdout_file)
                stdout_text = read(stdout_file, String)
                rm(stdout_file)
            end
            
            stderr_text = ""
            if isfile(stderr_file)
                stderr_text = read(stderr_file, String)
                rm(stderr_file)
            end
            
            # Prepare response
            response = Dict(
                "success" => (exit_code == 0),
                "stdout" => stdout_text,
                "stderr" => stderr_text
            )
        elseif request["action"] == "ping" || request["action"] == "keep_alive"
            # Health check or keep-alive
            response = Dict("status" => "ok")
        else
            # Unknown action
            response = Dict(
                "success" => false,
                "error" => "Unknown action: $(request["action"])"
            )
        end
        
        # Send response
        response_data = Vector{UInt8}(JSON.json(response))
        response_size = length(response_data)
        
        write(client, int_to_bytes(response_size))
        write(client, response_data)
        
    catch e
        @printf("Error handling client: %s\n", e)
        println(stacktrace(catch_backtrace()))
    finally
        close(client)
    end
end

function main()
    if length(ARGS) < 1
        println("Usage: julia julia_daemon.jl SOCKET_PATH")
        exit(1)
    end
    
    socket_path = ARGS[1]
    
    # Remove socket if it already exists
    if isfile(socket_path)
        rm(socket_path)
    end
    
    # Create server socket
    server = listen(socket_path)
    
    @printf("Julia daemon listening on %s\n", socket_path)
    
    # Set up signal handling
    Base.exit_on_sigint(false)
    
    try
        while true
            client = accept(server)
            @async handle_client(client)
        end
    catch e
        if isa(e, InterruptException)
            @printf("Received interrupt signal, shutting down\n")
        else
            @printf("Error in server loop: %s\n", e)
        end
    finally
        close(server)
        if isfile(socket_path)
            rm(socket_path)
        end
    end
end

# Run main
main()
