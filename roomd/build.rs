//! Generates Rust gRPC bindings from the `.proto` contracts in `proto/`.
//!
//! `protoc` is vendored through `protoc-bin-vendored` so that builds do not
//! depend on a system-wide protobuf compiler.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;

    let mut prost = tonic_prost_build::Config::new();
    prost.protoc_executable(protoc);

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .message_attribute(".", "#[allow(clippy::all)]")
        .enum_attribute(".", "#[allow(clippy::all)]")
        .file_descriptor_set_path(out_dir.join("roomd_descriptor.bin"))
        .include_file("_includes.rs")
        .compile_with_config(
            prost,
            &["proto/roomd/v1/room.proto", "proto/media/v1/media.proto"],
            &["proto"],
        )?;

    println!("cargo:rerun-if-changed=proto");
    println!("cargo:rerun-if-changed=build.rs");

    Ok(())
}
