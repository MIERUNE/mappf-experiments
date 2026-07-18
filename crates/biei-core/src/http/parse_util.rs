pub(crate) fn percent_decode_str(value: &str) -> Result<String, ()> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hi = *bytes.get(i + 1).ok_or(())?;
            let lo = *bytes.get(i + 2).ok_or(())?;
            let nibble = |b: u8| match b {
                b'0'..=b'9' => Some(b - b'0'),
                b'a'..=b'f' => Some(10 + b - b'a'),
                b'A'..=b'F' => Some(10 + b - b'A'),
                _ => None,
            };
            let byte = nibble(hi)
                .and_then(|h| nibble(lo).map(|l| (h << 4) | l))
                .ok_or(())?;
            out.push(byte);
            i += 3;
        } else if bytes[i] == b'+' {
            // Conventional form-encoded space; tolerate for query-string ergonomics.
            out.push(b' ');
            i += 1;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| ())
}
