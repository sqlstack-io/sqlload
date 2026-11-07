//! Reading pgoutput's big-endian fields.

/// A cursor over one message's bytes.
pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], String> {
        let end = self
            .at
            .checked_add(count)
            .filter(|end| *end <= self.bytes.len());
        let Some(end) = end else {
            return Err(format!(
                "the pgoutput message ends after {} bytes, inside a field at byte {}",
                self.bytes.len(),
                self.at
            ));
        };
        let taken = &self.bytes[self.at..end];
        self.at = end;
        Ok(taken)
    }

    pub(crate) fn byte(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn int16(&mut self) -> Result<i16, String> {
        let raw = self.take(2)?;
        Ok(i16::from_be_bytes([raw[0], raw[1]]))
    }

    pub(crate) fn int32(&mut self) -> Result<i32, String> {
        let raw = self.take(4)?;
        Ok(i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]))
    }

    pub(crate) fn uint32(&mut self) -> Result<u32, String> {
        let raw = self.take(4)?;
        Ok(u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]))
    }

    /// A NUL-terminated string.
    pub(crate) fn string(&mut self) -> Result<String, String> {
        let rest = &self.bytes[self.at..];
        let Some(end) = rest.iter().position(|byte| *byte == 0) else {
            return Err("a pgoutput string has no terminating NUL".to_owned());
        };
        let text = String::from_utf8_lossy(&rest[..end]).into_owned();
        self.at += end + 1;
        Ok(text)
    }

    /// `count` bytes as text.
    pub(crate) fn text(&mut self, count: usize) -> Result<String, String> {
        Ok(String::from_utf8_lossy(self.take(count)?).into_owned())
    }
}
