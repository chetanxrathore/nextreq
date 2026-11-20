fn main() {
    // `cargo test` links the test harness against libpython. Embed an rpath to
    // the interpreter's lib dir so the binary runs without DYLD_LIBRARY_PATH /
    // LD_LIBRARY_PATH. This is a no-op for extension-module builds via maturin.
    pyo3_build_config::add_libpython_rpath_link_args();
}
