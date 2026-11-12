//! Read the elements at a path of an XML document as records.

use quick_xml::NsReader;
use quick_xml::escape::resolve_predefined_entity;
use quick_xml::events::{BytesStart, Event};
use quick_xml::name::{NamespaceResolver, ResolveResult};

use crate::column_types::models::ColumnType;
use crate::files_source::_helpers::text::inference::csv_value_type;
use crate::files_source::constants::{
    UTF16_BOMS, XML_NIL_VALUES, XML_TEXT_KEY, XSI_NAMESPACE, XSI_NIL,
};
use crate::files_source::models::{Cell, CellRecord, XmlOptions};

/// An element being read: its attributes, child elements and text, in document order.
struct Element {
    name: String,
    attributes: Vec<(String, String)>,
    children: Vec<Element>,
    text: String,
    /// Whether `xsi:nil` marks it as NULL.
    nil: bool,
}

/// A record's field: text, or JSON text for repeated or structured elements.
enum Field {
    Text(String),
    Json(String),
    Null,
}

/// Each element at `options.record_path` in `document` as a record, numbered by the line its tag starts on: attributes and child elements are its columns, by local name. A document that ends inside an element, is UTF-16, or has no element where the path's parent leads is an error.
pub(crate) fn xml_records(
    document: &[u8],
    options: &XmlOptions,
) -> Result<Vec<CellRecord>, String> {
    if UTF16_BOMS.iter().any(|bom| document.starts_with(bom)) {
        return Err("the document is UTF-16; SQLLoad reads XML documents in UTF-8".to_owned());
    }
    let path: Vec<&str> = options
        .record_path
        .trim_matches('/')
        .split('/')
        .map(local_part)
        .collect();
    let mut reader = NsReader::from_reader(document);
    let mut found = false;
    let mut buffer: Vec<u8> = Vec::new();
    let mut outside: Vec<String> = Vec::new();
    let mut open: Vec<Element> = Vec::new();
    let mut records: Vec<CellRecord> = Vec::new();
    let (mut counted, mut line, mut record_line) = (0_usize, 1_u64, 1_u64);
    loop {
        let position = usize::try_from(reader.buffer_position()).unwrap_or(document.len());
        let end = position.min(document.len());
        line += document[counted.min(end)..end]
            .iter()
            .filter(|byte| **byte == b'\n')
            .count() as u64;
        counted = counted.max(end);
        let event = reader
            .read_event_into(&mut buffer)
            .map_err(|error| format!("line {line}: {error}"))?;
        let (started, ended) = match &event {
            Event::Start(start) => (Some(element(start, reader.resolver(), line)?), false),
            Event::Empty(start) => (Some(element(start, reader.resolver(), line)?), true),
            Event::End(_) => (None, true),
            Event::Text(text) => {
                push_text(&mut open, &text.xml10_content());
                (None, false)
            }
            Event::CData(data) => {
                push_text(&mut open, &data.xml10_content());
                (None, false)
            }
            Event::GeneralRef(entity) => {
                let resolved = match entity.resolve_char_ref() {
                    Ok(Some(character)) => character.to_string(),
                    Ok(None) => resolve_predefined_entity(entity)
                        .ok_or_else(|| format!("line {line}: unknown entity &{};", &**entity))?
                        .to_owned(),
                    Err(error) => return Err(format!("line {line}: {error}")),
                };
                push_text(&mut open, &resolved);
                (None, false)
            }
            Event::Eof => break,
            _ => (None, false),
        };
        if let Some(started) = started {
            match open.is_empty() {
                true => {
                    outside.push(started.name.clone());
                    found |=
                        outside.len() + 1 == path.len() && path[..outside.len()] == outside[..];
                    if outside == path {
                        found = true;
                        record_line = line;
                        open.push(started);
                    }
                }
                false => open.push(started),
            }
        }
        if ended {
            match open.pop() {
                Some(done) if open.is_empty() => {
                    outside.pop();
                    records.push(record(done, record_line, options)?);
                }
                Some(done) => {
                    if let Some(parent) = open.last_mut() {
                        parent.children.push(done);
                    }
                }
                None => {
                    outside.pop();
                }
            }
        }
        buffer.clear();
    }
    if let Some(inside) = open.last().map(|element| &element.name).or(outside.last()) {
        return Err(format!("line {line}: the document ends inside <{inside}>"));
    }
    match found {
        true => Ok(records),
        false => Err(format!(
            "no element at record_path {:?}; its names match elements without their namespace prefix",
            options.record_path
        )),
    }
}

/// A name without its namespace prefix.
fn local_part(name: &str) -> &str {
    name.rsplit(':').next().unwrap_or(name)
}

/// The element `start` opens, on `line`: its attributes by local name, namespace declarations and attributes in the XML Schema instance namespace left out; `xsi:nil` marks it NULL. Two attributes with one local name are an error.
fn element(
    start: &BytesStart<'_>,
    resolver: &NamespaceResolver,
    line: u64,
) -> Result<Element, String> {
    let name = start.local_name().into_inner().to_owned();
    let mut attributes: Vec<(String, String)> = Vec::new();
    let mut nil = false;
    for attribute in start.attributes() {
        let attribute = attribute.map_err(|error| format!("line {line}: {error}"))?;
        if attribute.key.as_namespace_binding().is_some() {
            continue;
        }
        let value = attribute
            .normalized_value(quick_xml::XmlVersion::Implicit1_0)
            .map_err(|error| format!("line {line}: {error}"))?;
        let local = attribute.key.local_name().into_inner().to_owned();
        if let (ResolveResult::Bound(namespace), _) = resolver.resolve_attribute(attribute.key)
            && namespace.0 == XSI_NAMESPACE
        {
            nil |= local == XSI_NIL && XML_NIL_VALUES.contains(&value.trim());
            continue;
        }
        if attributes.iter().any(|(taken, _)| *taken == local) {
            return Err(format!(
                "line {line}: <{name}> has two attributes named {local}"
            ));
        }
        attributes.push((local, value.into_owned()));
    }
    Ok(Element {
        name,
        attributes,
        children: Vec::new(),
        text: String::new(),
        nil,
    })
}

fn push_text(open: &mut [Element], text: &str) {
    if let Some(element) = open.last_mut() {
        element.text.push_str(text);
    }
}

/// The record element `done` as cells: attributes, then child elements by first appearance, then its own text under `#text`.
fn record(done: Element, line: u64, options: &XmlOptions) -> Result<CellRecord, String> {
    let mut cells: Vec<(String, Cell)> = Vec::new();
    for (name, field) in fields(&done, options).map_err(|error| format!("line {line}: {error}"))? {
        let cell = match field {
            Field::Null => continue,
            Field::Text(text) => Cell {
                kind: csv_value_type(&text),
                text,
            },
            Field::Json(text) => Cell {
                text,
                kind: ColumnType::Json,
            },
        };
        cells.push((name, cell));
    }
    Ok(CellRecord { line, cells })
}

/// An element's fields: each attribute as text; each child element by name, as its value, or a JSON array of values where it repeats or `options.lists` names it; and its own text, trimmed, where it has attributes or children.
fn fields(element: &Element, options: &XmlOptions) -> Result<Vec<(String, Field)>, String> {
    let mut fields: Vec<(String, Field)> = element
        .attributes
        .iter()
        .map(|(name, value)| (name.clone(), Field::Text(value.clone())))
        .collect();
    let mut names: Vec<&str> = Vec::new();
    for child in &element.children {
        if !names.contains(&child.name.as_str()) {
            names.push(&child.name);
        }
    }
    for name in names {
        if element
            .attributes
            .iter()
            .any(|(attribute, _)| attribute == name)
        {
            return Err(format!(
                "<{}> has an attribute and a child element both named {name}",
                element.name
            ));
        }
        let values: Vec<&Element> = element
            .children
            .iter()
            .filter(|child| child.name == name)
            .collect();
        let field = match values.as_slice() {
            [one] if !options.lists.iter().any(|list| list == name) => value(one, options)?,
            many => Field::Json(format!(
                "[{}]",
                many.iter()
                    .map(|child| value(child, options).map(json))
                    .collect::<Result<Vec<String>, String>>()?
                    .join(",")
            )),
        };
        fields.push((name.to_owned(), field));
    }
    let text = element.text.trim();
    if !text.is_empty() {
        fields.push((XML_TEXT_KEY.to_owned(), Field::Text(text.to_owned())));
    }
    Ok(fields)
}

/// A child element's value: NULL where `xsi:nil` says so, its trimmed text (NULL when empty), or a JSON object of its fields.
fn value(element: &Element, options: &XmlOptions) -> Result<Field, String> {
    if element.nil {
        return Ok(Field::Null);
    }
    if element.attributes.is_empty() && element.children.is_empty() {
        let text = element.text.trim();
        return Ok(match text.is_empty() {
            true => Field::Null,
            false => Field::Text(text.to_owned()),
        });
    }
    let members: Vec<String> = fields(element, options)?
        .into_iter()
        .map(|(name, field)| format!("{}:{}", serde_json::Value::String(name), json(field)))
        .collect();
    Ok(Field::Json(format!("{{{}}}", members.join(","))))
}

/// A field as JSON text: text as a JSON string.
fn json(field: Field) -> String {
    match field {
        Field::Text(text) => serde_json::Value::String(text).to_string(),
        Field::Json(text) => text,
        Field::Null => "null".to_owned(),
    }
}
