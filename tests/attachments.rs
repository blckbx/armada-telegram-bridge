use std::fs;

use armada_telegram_bridge::attachments::{
    AttachmentError, PrivateTempFile, sanitize_filename, sniff_media_type, validate_declared_size,
    validate_remote_url,
};

#[test]
fn declarations_are_rejected_before_staging_and_actual_size_is_rechecked() {
    assert_eq!(
        validate_declared_size(21, 20),
        Err(AttachmentError::TooLarge)
    );
    assert!(validate_declared_size(0, 20).is_ok());

    let root = tempfile::tempdir().expect("temp root");
    let staged = PrivateTempFile::new(root.path(), Some("file.bin")).expect("private file");
    fs::write(staged.path(), [0_u8; 21]).expect("fixture writes");
    assert_eq!(staged.inspect(20), Err(AttachmentError::TooLarge));
}

#[cfg(unix)]
#[test]
fn temporary_files_are_random_private_and_removed_on_every_drop_path() {
    use std::os::unix::fs::PermissionsExt;

    let root = tempfile::tempdir().expect("temp root");
    let first_path;
    let second_path;
    {
        let first = PrivateTempFile::new(root.path(), Some("report.pdf")).expect("first file");
        let second = PrivateTempFile::new(root.path(), Some("report.pdf")).expect("second file");
        first_path = first.path().to_owned();
        second_path = second.path().to_owned();
        assert_ne!(first_path, second_path);
        assert_eq!(
            fs::metadata(&first_path)
                .expect("metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    assert!(!first_path.exists());
    assert!(!second_path.exists());
}

#[test]
fn hostile_names_are_basename_only_bounded_and_control_free() {
    assert_eq!(
        sanitize_filename(Some("../../..\\secret\u{202e}.exe")),
        "secret.exe"
    );
    assert_eq!(sanitize_filename(Some("...")), "attachment.bin");
    assert!(sanitize_filename(Some(&"a".repeat(500))).len() <= 120);
}

#[test]
fn media_type_comes_from_magic_bytes_not_the_filename() {
    assert_eq!(sniff_media_type(b"\x89PNG\r\n\x1a\nrest"), "image/png");
    assert_eq!(sniff_media_type(b"%PDF-1.7"), "application/pdf");
    assert_eq!(
        sniff_media_type(b"not really a jpeg"),
        "application/octet-stream"
    );
}

#[test]
fn bridge_controlled_urls_reject_local_private_and_forbidden_targets() {
    for unsafe_url in [
        "file:///etc/passwd",
        "ftp://example.com/file",
        "http://localhost/file",
        "http://127.0.0.1/file",
        "http://169.254.169.254/latest/meta-data",
        "http://10.0.0.1/file",
        "http://[::1]/file",
        "http://user:pass@example.com/file",
    ] {
        assert_eq!(
            validate_remote_url(unsafe_url),
            Err(AttachmentError::UnsafeUrl),
            "{unsafe_url}"
        );
    }
    assert!(validate_remote_url("https://cdn.example.com/file").is_ok());
}
