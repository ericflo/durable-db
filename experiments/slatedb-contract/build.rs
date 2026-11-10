fn main() {
    let compiler = std::env::var_os("RUSTC").expect("Cargo supplies rustc");
    let output = std::process::Command::new(compiler)
        .arg("--version")
        .output()
        .expect("rustc version");
    assert!(output.status.success());
    println!(
        "cargo:rustc-env=PROBE_RUSTC={}",
        String::from_utf8(output.stdout)
            .expect("UTF-8 version")
            .trim()
    );
}
