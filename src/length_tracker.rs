use std::io;

pub struct LengthTracker {
    length: usize,
}

impl LengthTracker {
    pub fn new() -> Self {
        LengthTracker { length: 0 }
    }

    pub fn len(&self) -> usize {
        self.length
    }
}

impl io::Write for LengthTracker {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = buf.len();
        self.length += len;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
