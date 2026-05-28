fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use the vendored protoc binary so the build needs no system protoc
    // (the slim Docker image and CI don't ship one).
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // SAFETY: build scripts are single-threaded; setting PROTOC for tonic-build.
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    // Compile only client stubs for the internal services the poller calls.
    // Protos are vendored under proto/ (self-contained, no cross imports).
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(
            &[
                "proto/leavepulse/server/v1/catalog.proto",
                "proto/leavepulse/server/v1/internal_servers.proto",
                "proto/leavepulse/monitoring/v1/internal.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
