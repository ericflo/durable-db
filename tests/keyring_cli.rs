use durable_db::auth::Auth;
use std::process::Command;

#[test]
fn keyring_cli_creates_private_non_overwriting_files_and_expiring_admin_tokens() {
    let root = tempfile::tempdir().unwrap();
    let binary = env!("CARGO_BIN_EXE_durable-db");
    let a = root.path().join("a.key");
    let b = root.path().join("b.key");
    let ring = root.path().join("ring.json");
    for key in [&a, &b] {
        let output = Command::new(binary)
            .arg("keygen")
            .arg("--output")
            .arg(key)
            .output()
            .unwrap();
        assert!(output.status.success());
    }
    let assemble = || {
        Command::new(binary)
            .arg("keyring")
            .arg("--active-key-file")
            .arg(&b)
            .arg("--verify-key-file")
            .arg(&a)
            .arg("--output")
            .arg(&ring)
            .output()
            .unwrap()
    };
    assert!(assemble().status.success());
    let original = std::fs::read_to_string(&ring).unwrap();
    assert!(!assemble().status.success());
    assert_eq!(std::fs::read_to_string(&ring).unwrap(), original);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&ring).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    let output = Command::new(binary)
        .arg("token")
        .arg("--key-file")
        .arg(&ring)
        .arg("--admin")
        .arg("--ttl-seconds")
        .arg("60")
        .output()
        .unwrap();
    assert!(output.status.success());
    let token = String::from_utf8(output.stdout).unwrap();
    let token = token.trim();
    assert!(token.starts_with("ddb_control_"));
    let auth_b = Auth::new(&std::fs::read_to_string(&b).unwrap()).unwrap();
    let auth_a = Auth::new(&std::fs::read_to_string(&a).unwrap()).unwrap();
    assert!(auth_b.is_admin(token));
    assert!(!auth_a.is_admin(token));
    let ring_auth = Auth::new(&original).unwrap();
    assert!(ring_auth.is_admin(&auth_a.administrator_token(60).unwrap()));
    assert!(!ring_auth.is_admin(&auth_a.admin_token()));
    let duplicate = root.path().join("duplicate.json");
    assert!(
        !Command::new(binary)
            .arg("keyring")
            .arg("--active-key-file")
            .arg(&a)
            .arg("--verify-key-file")
            .arg(&a)
            .arg("--output")
            .arg(&duplicate)
            .output()
            .unwrap()
            .status
            .success()
    );
    assert!(!duplicate.exists());
    assert!(
        !Command::new(binary)
            .arg("token")
            .arg("--key-file")
            .arg(&ring)
            .arg("--admin")
            .arg("--ttl-seconds")
            .arg("0")
            .output()
            .unwrap()
            .status
            .success()
    );
}
