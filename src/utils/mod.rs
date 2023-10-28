use std::fs::DirEntry;

use regex::Regex;

pub mod bincode;
pub mod local_event;
pub mod timeout;
pub mod timestamp_nanos;

pub fn get_first_capture(pattern: &Regex, entry: &DirEntry) -> Option<String> {
    let file_name = entry.file_name();
    file_name.to_str().and_then(|file_str| {
        pattern.captures(file_str).and_then(|captures| {
            captures
                .get(1)
                .map(|number_capture| number_capture.as_str().to_string())
        })
    })
}
