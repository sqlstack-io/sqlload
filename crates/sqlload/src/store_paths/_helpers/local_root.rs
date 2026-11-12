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
        let bytes = drive.as_bytes();
        if bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && bytes[2] == b'/'
        {
            return Some((
                format!("{}:/", char::from(bytes[0]).to_ascii_uppercase()),
                &drive[3..],
            ));
        }
    }
    path.strip_prefix('/').map(|rest| ("/".to_owned(), rest))
}

/// The local path a `file://` URL names, given what follows `file://`: on Windows `file:///C:/folder` is `C:/folder` and `file://server/share/folder` is the share `//server/share/folder`.
pub(crate) fn windows_file_url_path(rest: &str, windows: bool) -> Cow<'_, str> {
    match (windows, rest.strip_prefix('/')) {
        (false, _) => Cow::Borrowed(rest),
        (true, Some(path)) if path.as_bytes().get(1) == Some(&b':') => Cow::Borrowed(path),
        (true, Some(_)) => Cow::Borrowed(rest),
        (true, None) => Cow::Owned(format!("//{rest}")),
    }
}
