#' CrossLink: Simple cross-language data sharing with true zero-copy optimization
#' 
#' This compatibility layer maintains backward compatibility with code that imports directly
#' from crosslink.R rather than using the new modular structure.

# === New Streaming Functions ===


#' @title Start Push Stream
#' @description Start a new stream to send data batches. 

#' @param con CrossLink connection (should be the C++ pointer object)
#' @param schema Arrow schema describing the structure of batches to be sent
#' @param name Optional name for the stream
#' @return A list containing the stream ID and the stream writer object (XPtr)
#' @export
push_stream <- function(con, schema, name = "") {
  # Directly call the Rcpp-generated function
  # Assumes 'con' is the XPtr returned by crosslink_connect
  result_list <- crosslink_push_stream(con, schema, name)
  # The C++ binding returns a list with 'stream_id' and 'writer' XPtr
  # We might want to wrap the writer XPtr in an R6 class later for usability
  return(result_list)
}


#' Pull a data stream from CrossLink
#' 
#' Connect to an existing stream to receive data batches.
#'
#' @param con CrossLink connection (should be the C++ pointer object)
#' @param stream_id ID of the stream to connect to
#' @return A stream reader object (XPtr)
#' @export
pull_stream <- function(con, stream_id) {
  # Directly call the Rcpp-generated function
  # Assumes 'con' is the XPtr returned by crosslink_connect
  reader_xptr <- crosslink_pull_stream(con, stream_id)
  # We might want to wrap the reader XPtr in an R6 class later for usability
  return(reader_xptr)
} 