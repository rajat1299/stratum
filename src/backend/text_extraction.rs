use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;
use std::io::{Cursor, Read};
use std::sync::Arc;
use tokio::sync::RwLock;
use zip::read::ZipArchive;

use crate::backend::search_index::SearchIndexHead;
use crate::error::VfsError;
use crate::store::ObjectId;

pub const EXTRACTED_TEXT_VERSION_V1: &str = "extracted-text-v1";

pub const MAX_EXTRACT_SOURCE_BYTES: usize = 10 * 1024 * 1024;
pub const MAX_EXTRACTED_TEXT_CHARS: usize = 100_000;
pub const MAX_DOCX_ZIP_ENTRIES: usize = 512;
pub const MAX_DOCX_EXPANDED_XML_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_PDF_PAGES: usize = 500;
pub const MAX_PDF_TEXT_OPERATORS: usize = 250_000;

const EXTRACTOR_PLAIN_TEXT: &str = "plain-text-v1";
const EXTRACTOR_MARKDOWN: &str = "markdown-v1";
const EXTRACTOR_DOCX: &str = "docx-v1";
const EXTRACTOR_PDF: &str = "pdf-v1";
const EXTRACTOR_UNSUPPORTED: &str = "unsupported-v1";

const W_NS: &str = "http://schemas.openformats.org/wordprocessingml/2006/main";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtractedTextStatus {
    Ready,
    Unsupported,
    TooLarge,
    Failed,
}

#[derive(Clone)]
pub struct ExtractedTextRecord {
    pub path: String,
    pub object_id: ObjectId,
    pub source_byte_len: u64,
    pub source_mime_type: Option<String>,
    pub extractor: String,
    pub status: ExtractedTextStatus,
    pub text: Option<String>,
    pub text_hash: Option<String>,
    pub failure_code: Option<String>,
}

impl fmt::Debug for ExtractedTextRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExtractedTextRecord")
            .field("path", &self.path)
            .field("object_id", &self.object_id)
            .field("source_byte_len", &self.source_byte_len)
            .field("source_mime_type", &self.source_mime_type)
            .field("extractor", &self.extractor)
            .field("status", &self.status)
            .field(
                "text",
                &self
                    .text
                    .as_ref()
                    .map(|value| format!("<{} chars>", value.chars().count())),
            )
            .field("text_hash", &self.text_hash)
            .field("failure_code", &self.failure_code)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SupportedExtractor {
    PlainText,
    Markdown,
    Docx,
    Pdf,
}

pub fn extract_text_for_blob(
    path: &str,
    mime_type: Option<&str>,
    object_id: ObjectId,
    bytes: &[u8],
) -> ExtractedTextRecord {
    let ctx = ExtractionContext {
        path,
        object_id,
        source_byte_len: bytes.len() as u64,
        source_mime_type: mime_type.map(str::to_string),
    };

    let Some(kind) = detect_extractor(path, mime_type) else {
        return ctx.record(
            EXTRACTOR_UNSUPPORTED,
            ExtractedTextStatus::Unsupported,
            None,
            None,
            Some("unsupported_type".to_string()),
        );
    };

    if bytes.len() > MAX_EXTRACT_SOURCE_BYTES {
        let failure = match kind {
            SupportedExtractor::PlainText | SupportedExtractor::Markdown => "text_too_large",
            SupportedExtractor::Docx => "docx_zip_too_large",
            SupportedExtractor::Pdf => "pdf_too_large",
        };
        return ctx.record(
            extractor_name(kind),
            ExtractedTextStatus::TooLarge,
            None,
            None,
            Some(failure.to_string()),
        );
    }

    match kind {
        SupportedExtractor::PlainText | SupportedExtractor::Markdown => {
            extract_utf8_text(&ctx, kind, bytes)
        }
        SupportedExtractor::Docx => extract_docx(&ctx, bytes),
        SupportedExtractor::Pdf => extract_pdf(&ctx, bytes),
    }
}

struct ExtractionContext<'a> {
    path: &'a str,
    object_id: ObjectId,
    source_byte_len: u64,
    source_mime_type: Option<String>,
}

impl ExtractionContext<'_> {
    fn record(
        &self,
        extractor: &str,
        status: ExtractedTextStatus,
        text: Option<String>,
        text_hash: Option<String>,
        failure_code: Option<String>,
    ) -> ExtractedTextRecord {
        ExtractedTextRecord {
            path: self.path.to_string(),
            object_id: self.object_id,
            source_byte_len: self.source_byte_len,
            source_mime_type: self.source_mime_type.clone(),
            extractor: extractor.to_string(),
            status,
            text,
            text_hash,
            failure_code,
        }
    }
}

fn extractor_name(kind: SupportedExtractor) -> &'static str {
    match kind {
        SupportedExtractor::PlainText => EXTRACTOR_PLAIN_TEXT,
        SupportedExtractor::Markdown => EXTRACTOR_MARKDOWN,
        SupportedExtractor::Docx => EXTRACTOR_DOCX,
        SupportedExtractor::Pdf => EXTRACTOR_PDF,
    }
}

fn detect_extractor(path: &str, mime_type: Option<&str>) -> Option<SupportedExtractor> {
    if let Some(mime) = mime_type {
        if let Some(kind) = extractor_for_mime(mime) {
            return Some(kind);
        }
        if mime == "application/octet-stream" {
            return extractor_for_extension(path);
        }
        return None;
    }
    extractor_for_extension(path)
}

fn extractor_for_mime(mime: &str) -> Option<SupportedExtractor> {
    match mime {
        "text/plain" => Some(SupportedExtractor::PlainText),
        "text/markdown" => Some(SupportedExtractor::Markdown),
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => {
            Some(SupportedExtractor::Docx)
        }
        "application/pdf" => Some(SupportedExtractor::Pdf),
        _ => None,
    }
}

fn extractor_for_extension(path: &str) -> Option<SupportedExtractor> {
    let ext = path.rsplit('.').next()?;
    match ext.to_ascii_lowercase().as_str() {
        "txt" => Some(SupportedExtractor::PlainText),
        "md" | "markdown" => Some(SupportedExtractor::Markdown),
        "docx" => Some(SupportedExtractor::Docx),
        "pdf" => Some(SupportedExtractor::Pdf),
        _ => None,
    }
}

fn extract_utf8_text(
    ctx: &ExtractionContext<'_>,
    kind: SupportedExtractor,
    bytes: &[u8],
) -> ExtractedTextRecord {
    let Ok(mut text) = std::str::from_utf8(bytes).map(|s| s.to_string()) else {
        return ctx.record(
            extractor_name(kind),
            ExtractedTextStatus::Failed,
            None,
            None,
            Some("text_invalid_utf8".to_string()),
        );
    };
    normalize_text_lines(&mut text);
    strip_utf8_bom(&mut text);
    if text.chars().count() > MAX_EXTRACTED_TEXT_CHARS {
        return ctx.record(
            extractor_name(kind),
            ExtractedTextStatus::TooLarge,
            None,
            None,
            Some("text_too_large".to_string()),
        );
    }
    ready_record(ctx, extractor_name(kind), text)
}

fn normalize_text_lines(text: &mut String) {
    *text = text.replace("\r\n", "\n").replace('\r', "\n");
}

fn strip_utf8_bom(text: &mut String) {
    const BOM: char = '\u{feff}';
    if text.starts_with(BOM) {
        text.remove(0);
    }
}

fn ready_record(ctx: &ExtractionContext<'_>, extractor: &str, text: String) -> ExtractedTextRecord {
    let capped = truncate_chars(text, MAX_EXTRACTED_TEXT_CHARS);
    let hash = hash_text(&capped);
    ctx.record(
        extractor,
        ExtractedTextStatus::Ready,
        Some(capped),
        Some(hash),
        None,
    )
}

fn hash_text(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

fn truncate_chars(mut value: String, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value;
    }
    value.truncate(
        value
            .char_indices()
            .nth(max_chars)
            .map(|(index, _)| index)
            .unwrap_or(value.len()),
    );
    value
}

fn extract_docx(ctx: &ExtractionContext<'_>, bytes: &[u8]) -> ExtractedTextRecord {
    let cursor = Cursor::new(bytes);
    let mut archive = match ZipArchive::new(cursor) {
        Ok(archive) => archive,
        Err(_) => {
            return ctx.record(
                EXTRACTOR_DOCX,
                ExtractedTextStatus::Failed,
                None,
                None,
                Some("docx_zip_invalid".to_string()),
            );
        }
    };
    if archive.len() > MAX_DOCX_ZIP_ENTRIES {
        return ctx.record(
            EXTRACTOR_DOCX,
            ExtractedTextStatus::TooLarge,
            None,
            None,
            Some("docx_zip_too_large".to_string()),
        );
    }

    let mut document_xml = None;
    let mut header_footer_xml = Vec::new();
    for index in 0..archive.len() {
        let Ok(mut entry) = archive.by_index(index) else {
            return ctx.record(
                EXTRACTOR_DOCX,
                ExtractedTextStatus::Failed,
                None,
                None,
                Some("docx_zip_invalid".to_string()),
            );
        };
        let name = entry.name().to_string();
        if name.contains("..") || name.starts_with('/') {
            continue;
        }
        if !name.starts_with("word/") {
            continue;
        }
        let capped_name = name.as_str();
        let is_document = capped_name == "word/document.xml";
        let is_header_footer =
            capped_name.starts_with("word/header") || capped_name.starts_with("word/footer");
        if !is_document && !is_header_footer {
            continue;
        }
        let mut buf = Vec::new();
        if entry.read_to_end(&mut buf).is_err() {
            return ctx.record(
                EXTRACTOR_DOCX,
                ExtractedTextStatus::Failed,
                None,
                None,
                Some("docx_zip_invalid".to_string()),
            );
        }
        if buf.len() > MAX_DOCX_EXPANDED_XML_BYTES {
            return ctx.record(
                EXTRACTOR_DOCX,
                ExtractedTextStatus::TooLarge,
                None,
                None,
                Some("docx_zip_too_large".to_string()),
            );
        }
        if is_document {
            document_xml = Some(buf);
        } else if is_header_footer {
            header_footer_xml.push(buf);
        }
    }

    let Some(document_xml) = document_xml else {
        return ctx.record(
            EXTRACTOR_DOCX,
            ExtractedTextStatus::Failed,
            None,
            None,
            Some("docx_xml_invalid".to_string()),
        );
    };

    let mut text = String::new();
    for xml in std::iter::once(document_xml).chain(header_footer_xml) {
        match parse_docx_xml(&xml, &mut text) {
            Ok(()) => {}
            Err(code) => {
                return ctx.record(
                    EXTRACTOR_DOCX,
                    ExtractedTextStatus::Failed,
                    None,
                    None,
                    Some(code),
                );
            }
        }
    }

    let trimmed = text.trim();
    if trimmed.is_empty() {
        return ctx.record(
            EXTRACTOR_DOCX,
            ExtractedTextStatus::Failed,
            None,
            None,
            Some("docx_no_text".to_string()),
        );
    }
    ready_record(ctx, EXTRACTOR_DOCX, trimmed.to_string())
}

fn parse_docx_xml(xml: &[u8], out: &mut String) -> Result<(), String> {
    use quick_xml::events::Event;
    use quick_xml::reader::Reader;

    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(false);
    let mut buf = Vec::new();
    let mut in_text = false;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(start)) => {
                let local = start.local_name();
                if local.as_ref() == b"t" {
                    in_text = true;
                } else if local.as_ref() == b"tab" {
                    out.push('\t');
                }
            }
            Ok(Event::Text(text)) if in_text => {
                out.push_str(
                    &text
                        .unescape()
                        .map_err(|_| "docx_xml_invalid".to_string())?,
                );
            }
            Ok(Event::End(end)) => {
                let local = end.local_name();
                if local.as_ref() == b"t" {
                    in_text = false;
                } else if local.as_ref() == b"p" {
                    out.push('\n');
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => return Err("docx_xml_invalid".to_string()),
            _ => {}
        }
        buf.clear();
    }
    Ok(())
}

fn extract_pdf(ctx: &ExtractionContext<'_>, bytes: &[u8]) -> ExtractedTextRecord {
    use lopdf::Document;

    let doc = match Document::load_mem(bytes) {
        Ok(doc) => doc,
        Err(_) => {
            return ctx.record(
                EXTRACTOR_PDF,
                ExtractedTextStatus::Failed,
                None,
                None,
                Some("pdf_invalid".to_string()),
            );
        }
    };

    if doc.is_encrypted() {
        return ctx.record(
            EXTRACTOR_PDF,
            ExtractedTextStatus::Failed,
            None,
            None,
            Some("pdf_encrypted".to_string()),
        );
    }

    let page_count = doc.get_pages().len();
    if page_count > MAX_PDF_PAGES {
        return ctx.record(
            EXTRACTOR_PDF,
            ExtractedTextStatus::TooLarge,
            None,
            None,
            Some("pdf_too_large".to_string()),
        );
    }

    let mut operators_seen = 0usize;
    let mut text = String::new();
    let pages: Vec<u32> = doc.get_pages().keys().copied().collect();
    for page in pages {
        operators_seen = operators_seen.saturating_add(1);
        if operators_seen > MAX_PDF_TEXT_OPERATORS {
            return ctx.record(
                EXTRACTOR_PDF,
                ExtractedTextStatus::TooLarge,
                None,
                None,
                Some("pdf_too_large".to_string()),
            );
        }
        match doc.extract_text(&[page]) {
            Ok(page_text) => {
                if !page_text.is_empty() {
                    if !text.is_empty() {
                        text.push('\n');
                    }
                    text.push_str(page_text.trim());
                }
            }
            Err(_) => {
                return ctx.record(
                    EXTRACTOR_PDF,
                    ExtractedTextStatus::Failed,
                    None,
                    None,
                    Some("pdf_invalid".to_string()),
                );
            }
        }
    }

    if text.is_empty() {
        return ctx.record(
            EXTRACTOR_PDF,
            ExtractedTextStatus::Ready,
            Some(String::new()),
            Some(hash_text("")),
            None,
        );
    }
    ready_record(ctx, EXTRACTOR_PDF, text)
}

#[async_trait]
pub trait TextExtractionStore: Send + Sync {
    async fn ensure_available(&self) -> Result<(), VfsError>;

    async fn put_records(
        &self,
        head: SearchIndexHead,
        records: Vec<ExtractedTextRecord>,
    ) -> Result<(), VfsError>;

    async fn record_for_path(
        &self,
        head: &SearchIndexHead,
        path: &str,
    ) -> Result<Option<ExtractedTextRecord>, VfsError>;

    fn available(&self) -> bool;
}

pub struct UnavailableTextExtractionStore;

#[async_trait]
impl TextExtractionStore for UnavailableTextExtractionStore {
    async fn ensure_available(&self) -> Result<(), VfsError> {
        Err(VfsError::NotSupported {
            message: "text extraction store is unavailable".to_string(),
        })
    }

    async fn put_records(
        &self,
        _head: SearchIndexHead,
        _records: Vec<ExtractedTextRecord>,
    ) -> Result<(), VfsError> {
        Err(VfsError::NotSupported {
            message: "text extraction store is unavailable".to_string(),
        })
    }

    async fn record_for_path(
        &self,
        _head: &SearchIndexHead,
        _path: &str,
    ) -> Result<Option<ExtractedTextRecord>, VfsError> {
        Ok(None)
    }

    fn available(&self) -> bool {
        false
    }
}

type InMemoryExtractionState = BTreeMap<(String, String, String, String), ExtractedTextRecord>;

pub struct InMemoryTextExtractionStore {
    state: Arc<RwLock<InMemoryExtractionState>>,
}

impl Default for InMemoryTextExtractionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryTextExtractionStore {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    fn key(head: &SearchIndexHead, path: &str) -> (String, String, String, String) {
        (
            head.repo_id.as_str().to_string(),
            head.commit_id.to_hex(),
            head.root_tree_id.to_hex(),
            path.to_string(),
        )
    }
}

#[async_trait]
impl TextExtractionStore for InMemoryTextExtractionStore {
    async fn ensure_available(&self) -> Result<(), VfsError> {
        Ok(())
    }

    async fn put_records(
        &self,
        head: SearchIndexHead,
        records: Vec<ExtractedTextRecord>,
    ) -> Result<(), VfsError> {
        let mut guard = self.state.write().await;
        for record in records {
            guard.insert(Self::key(&head, &record.path), record);
        }
        Ok(())
    }

    async fn record_for_path(
        &self,
        head: &SearchIndexHead,
        path: &str,
    ) -> Result<Option<ExtractedTextRecord>, VfsError> {
        let guard = self.state.read().await;
        Ok(guard.get(&Self::key(head, path)).cloned())
    }

    fn available(&self) -> bool {
        true
    }
}

pub type SharedTextExtractionStore = Arc<dyn TextExtractionStore>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::ObjectId;
    use std::io::Write;
    use zip::write::SimpleFileOptions;
    use zip::{CompressionMethod, ZipWriter};

    fn oid(seed: u8) -> ObjectId {
        ObjectId::from_bytes(&[seed; 32])
    }

    #[test]
    fn plain_text_extracts_with_line_normalization() {
        let record = extract_text_for_blob(
            "/notes/readme.txt",
            Some("text/plain"),
            oid(1),
            b"line one\r\nline two\r",
        );
        assert_eq!(record.status, ExtractedTextStatus::Ready);
        assert_eq!(record.extractor, EXTRACTOR_PLAIN_TEXT);
        assert_eq!(record.text.as_deref(), Some("line one\nline two\n"));
    }

    #[test]
    fn markdown_extracts_and_strips_bom() {
        let record = extract_text_for_blob(
            "/docs/guide.md",
            Some("text/markdown"),
            oid(2),
            "\u{feff}# Title".as_bytes(),
        );
        assert_eq!(record.extractor, EXTRACTOR_MARKDOWN);
        assert_eq!(record.text.as_deref(), Some("# Title"));
    }

    #[test]
    fn invalid_utf8_text_fails_without_leaking_bytes() {
        let record = extract_text_for_blob("/a.txt", Some("text/plain"), oid(3), &[0xff, 0xfe]);
        assert_eq!(record.status, ExtractedTextStatus::Failed);
        assert_eq!(record.failure_code.as_deref(), Some("text_invalid_utf8"));
        let debug = format!("{record:?}");
        assert!(!debug.contains("secret-token"));
    }

    #[test]
    fn oversized_text_is_too_large() {
        let huge = "x".repeat(MAX_EXTRACTED_TEXT_CHARS + 1);
        let record = extract_text_for_blob("/big.txt", Some("text/plain"), oid(4), huge.as_bytes());
        assert_eq!(record.status, ExtractedTextStatus::TooLarge);
        assert!(record.text.is_none());
    }

    #[test]
    fn debug_output_redacts_extracted_secrets() {
        let record = extract_text_for_blob(
            "/secret.txt",
            Some("text/plain"),
            oid(5),
            b"secret-token-value",
        );
        let debug = format!("{record:?}");
        assert!(!debug.contains("secret-token"));
        assert!(debug.contains("chars>"));
    }

    #[test]
    fn octet_stream_without_extension_is_unsupported() {
        let record =
            extract_text_for_blob("/blob", Some("application/octet-stream"), oid(6), b"abc");
        assert_eq!(record.status, ExtractedTextStatus::Unsupported);
    }

    #[test]
    fn docx_extracts_visible_text() {
        let bytes = minimal_docx(&["Hello", "World"]);
        let record = extract_text_for_blob(
            "/report.docx",
            Some("application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
            oid(7),
            &bytes,
        );
        assert_eq!(record.status, ExtractedTextStatus::Ready);
        assert_eq!(record.extractor, EXTRACTOR_DOCX);
        let text = record.text.expect("text");
        assert!(text.contains("Hello"));
        assert!(text.contains("World"));
    }

    #[test]
    fn corrupt_docx_zip_fails_closed() {
        let record = extract_text_for_blob(
            "/bad.docx",
            Some("application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
            oid(8),
            b"not-a-zip",
        );
        assert_eq!(record.status, ExtractedTextStatus::Failed);
        assert_eq!(record.failure_code.as_deref(), Some("docx_zip_invalid"));
        assert!(!format!("{record:?}").contains("not-a-zip"));
    }

    #[test]
    fn docx_with_too_many_zip_entries_is_too_large() {
        let bytes = docx_with_many_entries(MAX_DOCX_ZIP_ENTRIES + 1);
        let record = extract_text_for_blob("/big.docx", None, oid(9), &bytes);
        assert_eq!(record.status, ExtractedTextStatus::TooLarge);
    }

    #[test]
    fn pdf_extracts_text_from_minimal_fixture() {
        let bytes = minimal_pdf_with_text("checkout flow");
        let record = extract_text_for_blob("/paper.pdf", Some("application/pdf"), oid(10), &bytes);
        assert_eq!(record.status, ExtractedTextStatus::Ready);
        assert!(
            record
                .text
                .unwrap_or_default()
                .to_ascii_lowercase()
                .contains("checkout")
        );
    }

    #[test]
    fn malformed_pdf_fails_with_bounded_code() {
        let record =
            extract_text_for_blob("/bad.pdf", Some("application/pdf"), oid(11), b"%PDF-1.4\n");
        assert_eq!(record.status, ExtractedTextStatus::Failed);
        assert_eq!(record.failure_code.as_deref(), Some("pdf_invalid"));
    }

    fn minimal_docx(paragraphs: &[&str]) -> Vec<u8> {
        let body = paragraphs
            .iter()
            .map(|line| format!(r#"<w:p xmlns:w="{W_NS}"><w:r><w:t>{line}</w:t></w:r></w:p>"#))
            .collect::<Vec<_>>()
            .join("");
        let document_xml =
            format!(r#"<?xml version="1.0"?><w:document xmlns:w="{W_NS}">{body}</w:document>"#);
        let mut zip = ZipWriter::new(Cursor::new(Vec::new()));
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
        zip.start_file("word/document.xml", options)
            .expect("start document");
        zip.write_all(document_xml.as_bytes())
            .expect("write document");
        zip.finish().expect("finish zip").into_inner()
    }

    fn docx_with_many_entries(count: usize) -> Vec<u8> {
        let mut zip = ZipWriter::new(Cursor::new(Vec::new()));
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
        for index in 0..count {
            let name = format!("word/part{index}.xml");
            zip.start_file(name, options).expect("start");
            zip.write_all(b"<x/>").expect("write");
        }
        zip.finish().expect("finish").into_inner()
    }

    fn minimal_pdf_with_text(text: &str) -> Vec<u8> {
        use lopdf::content::{Content, Operation};
        use lopdf::{Dictionary, Document, Object, Stream};

        let mut doc = Document::with_version("1.5");
        let catalog_id = doc.new_object_id();
        let pages_id = doc.new_object_id();
        let page_id = doc.new_object_id();
        let content_id = doc.new_object_id();
        let font_id = doc.new_object_id();

        let content = Content {
            operations: vec![
                Operation::new("BT", vec![]),
                Operation::new(
                    "Tf",
                    vec![Object::Name(b"F1".to_vec()), Object::Integer(12)],
                ),
                Operation::new("Td", vec![Object::Integer(100), Object::Integer(700)]),
                Operation::new(
                    "Tj",
                    vec![Object::String(
                        text.as_bytes().to_vec(),
                        lopdf::StringFormat::Literal,
                    )],
                ),
                Operation::new("ET", vec![]),
            ],
        };
        let encoded = content.encode().expect("encode content");
        doc.objects.insert(
            content_id,
            Object::Stream(Stream::new(Dictionary::new(), encoded)),
        );
        doc.objects.insert(
            font_id,
            Object::Dictionary(Dictionary::from_iter(vec![
                ("Type", Object::Name(b"Font".to_vec())),
                ("Subtype", Object::Name(b"Type1".to_vec())),
                ("BaseFont", Object::Name(b"Helvetica".to_vec())),
            ])),
        );
        let mut page_dict = Dictionary::new();
        page_dict.set("Type", Object::Name(b"Page".to_vec()));
        page_dict.set("Parent", Object::Reference(pages_id));
        page_dict.set(
            "MediaBox",
            Object::Array(vec![0.into(), 0.into(), 300.into(), 300.into()]),
        );
        page_dict.set("Contents", Object::Reference(content_id));
        page_dict.set(
            "Resources",
            Object::Dictionary(Dictionary::from_iter(vec![(
                "Font",
                Object::Dictionary(Dictionary::from_iter(vec![(
                    "F1",
                    Object::Reference(font_id),
                )])),
            )])),
        );
        doc.objects.insert(page_id, Object::Dictionary(page_dict));
        let mut pages_dict = Dictionary::new();
        pages_dict.set("Type", Object::Name(b"Pages".to_vec()));
        pages_dict.set("Count", Object::Integer(1));
        pages_dict.set("Kids", Object::Array(vec![Object::Reference(page_id)]));
        doc.objects.insert(pages_id, Object::Dictionary(pages_dict));
        let mut catalog_dict = Dictionary::new();
        catalog_dict.set("Type", Object::Name(b"Catalog".to_vec()));
        catalog_dict.set("Pages", Object::Reference(pages_id));
        doc.objects
            .insert(catalog_id, Object::Dictionary(catalog_dict));
        doc.trailer.set("Root", Object::Reference(catalog_id));

        let mut buffer = Vec::new();
        doc.save_to(&mut buffer).expect("save pdf");
        buffer
    }
}
