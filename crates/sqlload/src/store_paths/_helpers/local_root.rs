use std::borrow::Cow;

/// `path` with Windows separators read as `/` and the `\\?\` (`\\?\UNC\`) prefix dropped, when `windows`; unchanged otherwise.
pub(crate) fn windows_separators(path: &str, windows: bool) -> Cow<'_, str> {
    if !windows || !path.contains('\\') {
        return Cow::Borrowed(path);
    }
    let path = path.replace('\\', "/");
    Cow::Owned(match path.strip_prefix("//?/") {
        Some(rest) => match rest.get(..4) {
            Some(unc) if unc.eq_ignore_ascii_case("UNC/") => format!("//{}", &rest[4..]),
            _ => rest.to_owned(),
        },
        None => path,
    })
}

/// The root and the rest of `path`, `/`-separated (see [`windows_separators`]), when it is absolute on this machine: `/`, or when `windows` also a drive (`C:/`, `/C:/` as a `file:///C:/` URL gives it) or a share (`//server/share/`). `None` for a relative path.
pub(crate) fn local_root(path: &str, windows: bool) -> Option<(String, &str)> {
    if windows {
        if let Some(share) = path.strip_prefix("//") {
            let mut parts = share.splitn(3, '/');
            let (server, name) = (parts.next()?, parts.next()?);
            return (!server.is_empty() && !name.is_empty()).then(|| {
                (
                    format!("//{server}/{name}/"),
                    parts.next().unwrap_or_default(),
                )
            });
        }
        let drive = path.strip_prefix('/').unwrap_or(path);
        if let [letter, b':', b'/', ..] = drive.as_bytes()
            && letter.is_ascii_alphabetic()
        {
            return Some((
                format!("{}:/", char::from(letter.to_ascii_uppercase())),
                &drive[3..],
            ));
        }
    }
    path.strip_prefix('/').map(|rest| ("/".to_owned(), rest))
}

/// The local path a `file://` URL names, given what follows `file://`: on Windows `file:///C:/folder` and `file://localhost/C:/folder` are `C:/folder`, and `file://server/share/folder` is the share `//server/share/folder`.
pub(crate) fn windows_file_url_path(rest: &str, windows: bool) -> Cow<'_, str> {
    let on_localhost = rest
        .get(.."localhost/".len())
        .filter(|host| host.eq_ignore_ascii_case("localhost/"))
        .map(|host| &rest[host.len()..]);
    match (windows, rest.strip_prefix('/'), on_localhost) {
        (false, _, _) => Cow::Borrowed(rest),
        (true, Some(path), _) if is_drive(path) => Cow::Borrowed(path),
        (true, Some(_), _) => Cow::Borrowed(rest),
        (true, None, Some(path)) if is_drive(path) => Cow::Borrowed(path),
        (true, None, _) => Cow::Owned(format!("//{rest}")),
    }
}

/// Whether `path` starts with a drive letter and its colon.
fn is_drive(path: &str) -> bool {
    matches!(path.as_bytes(), [letter, b':', ..] if letter.is_ascii_alphabetic())
}

/// `prefix` without its leading `folders` (both folder prefixes ending in `/`), whose names match ignoring case when `windows`, as NTFS and SMB shares match them; `None` when `prefix` is not below `folders`.
pub(crate) fn strip_folders<'a>(prefix: &'a str, folders: &str, windows: bool) -> Option<&'a str> {
    let head = prefix.get(..folders.len())?;
    let same = match windows {
        true => head.eq_ignore_ascii_case(folders),
        false => head == folders,
    };
    same.then(|| &prefix[folders.len()..])
}
