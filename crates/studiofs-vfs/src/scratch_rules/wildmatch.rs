use std::borrow::Cow;
use std::fmt::Write;

use globset::GlobBuilder;
use studiofs_meta::fold_dentry_name;

/// Anchored byte regexes for one rule pattern.
pub(super) struct Compiled {
    /// Matches the whole basename or scope-relative path.
    pub(super) expression: String,
    /// Matches a final path component, used to diagnose rule-file matches.
    pub(super) last_component: String,
    /// Every match has an empty component, so no canonical path matches.
    pub(super) empty_component: bool,
}

pub(super) fn compile(pattern: &[u8], case_insensitive: bool) -> Option<Compiled> {
    // Reuse globset's byte regex for ordinary UTF-8 globs. Compile the result
    // directly so Windows path normalization cannot rewrite dentry bytes.
    if !pattern.windows(2).any(|pair| pair == b"**")
        && !pattern.iter().any(|byte| b"[]{}".contains(byte))
    {
        let folded = if case_insensitive {
            fold_dentry_name(pattern)
        } else {
            Cow::Borrowed(pattern)
        };
        if let Ok(pattern) = std::str::from_utf8(&folded) {
            let expression = simple_glob(pattern)?.regex().to_owned();
            let last_component = match pattern.rsplit_once('/') {
                Some((_, last)) => simple_glob(last)?.regex().to_owned(),
                None => expression.clone(),
            };
            return Some(Compiled {
                expression,
                last_component,
                empty_component: simple_empty_component(pattern.as_bytes()),
            });
        }
    }

    // Git's recursive stars, bracket expressions, and raw bytes have different
    // semantics from globset. Translate them to an anchored byte regex.
    let pattern = if case_insensitive && !pattern.is_ascii() {
        fold_dentry_name(pattern)
    } else {
        Cow::Borrowed(pattern)
    };
    let literal = |byte: u8| {
        if case_insensitive {
            byte.to_ascii_lowercase()
        } else {
            byte
        }
    };
    let mut expression = String::from("(?-u)\\A");
    let mut component_start = expression.len();
    let mut empty_component = false;
    let mut cursor = 0;
    while cursor < pattern.len() {
        let byte = pattern[cursor];
        cursor += 1;
        match byte {
            b'\\' => {
                let byte = *pattern.get(cursor)?;
                cursor += 1;
                if byte == b'/' {
                    empty_component |= component_start == expression.len();
                    push_byte(&mut expression, byte);
                    component_start = expression.len();
                } else {
                    push_byte(&mut expression, literal(byte));
                }
            }
            b'/' => {
                empty_component |= component_start == expression.len();
                expression.push('/');
                component_start = expression.len();
            }
            b'?' => expression.push_str("[^/]"),
            b'*' => {
                let start = cursor - 1;
                while pattern.get(cursor) == Some(&b'*') {
                    cursor += 1;
                }
                let recursive = cursor - start > 1 && (start == 0 || pattern[start - 1] == b'/');
                match pattern.get(cursor..) {
                    Some(b"") if recursive => expression.push_str("(?s:.*)"),
                    Some([b'/', ..]) if recursive => {
                        expression.push_str("(?:[^/]+/)*");
                        component_start = expression.len();
                        cursor += 1;
                    }
                    Some([b'\\', b'/', ..]) if recursive => expression.push_str("(?s:.*)"),
                    _ => expression.push_str("[^/]*"),
                }
            }
            b'[' => push_class(&mut expression, &pattern, &mut cursor, case_insensitive)?,
            _ => push_byte(&mut expression, literal(byte)),
        }
    }
    empty_component |= component_start == expression.len();
    expression.push_str("\\z");
    let last_component = format!("(?-u)\\A{}", &expression[component_start..]);
    Some(Compiled {
        expression,
        last_component,
        empty_component,
    })
}

/// Whether a glob without bracket expressions has an empty component.
fn simple_empty_component(pattern: &[u8]) -> bool {
    let mut empty = true;
    let mut escape = false;
    for byte in pattern {
        if !escape && *byte == b'\\' {
            escape = true;
            continue;
        }
        escape = false;
        if *byte == b'/' {
            if empty {
                return true;
            }
            empty = true;
        } else {
            empty = false;
        }
    }
    empty
}

fn simple_glob(pattern: &str) -> Option<globset::Glob> {
    GlobBuilder::new(pattern)
        .literal_separator(true)
        .backslash_escape(true)
        .build()
        .ok()
}

fn push_byte(expression: &mut String, byte: u8) {
    let _ = write!(expression, "\\x{byte:02x}");
}

fn push_class(
    expression: &mut String,
    pattern: &[u8],
    cursor: &mut usize,
    case_insensitive: bool,
) -> Option<()> {
    let negated = matches!(pattern.get(*cursor), Some(b'!' | b'^'));
    *cursor += usize::from(negated);
    let mut members = [false; 256];
    let mut previous = None;
    loop {
        let byte = take_byte(pattern, cursor)?;
        previous = match byte {
            b'\\' => {
                let literal = take_byte(pattern, cursor)?;
                members[usize::from(literal)] = true;
                Some(literal)
            }
            b'-' if previous.is_some() && !matches!(pattern.get(*cursor), None | Some(b']')) => {
                let mut end = take_byte(pattern, cursor)?;
                if end == b'\\' {
                    end = take_byte(pattern, cursor)?;
                }
                for byte in previous?..=end {
                    members[usize::from(byte)] = true;
                }
                None
            }
            b'[' if pattern.get(*cursor) == Some(&b':') => {
                let body = &pattern[*cursor + 1..];
                let end = body.iter().position(|byte| *byte == b']')?;
                if let Some(class) = body[..end].strip_suffix(b":") {
                    push_posix_class(&mut members, class)?;
                    *cursor += end + 2;
                    None
                } else {
                    // Like Git, keep the bracket and skip the colon.
                    members[usize::from(b'[')] = true;
                    *cursor += 1;
                    Some(b'[')
                }
            }
            _ => {
                members[usize::from(byte)] = true;
                Some(byte)
            }
        };
        if pattern.get(*cursor) == Some(&b']') {
            *cursor += 1;
            break;
        }
    }
    if case_insensitive {
        for upper in b'A'..=b'Z' {
            members[usize::from(upper.to_ascii_lowercase())] |= members[usize::from(upper)];
            members[usize::from(upper)] = false;
        }
    }
    expression.push('[');
    if negated {
        expression.push('^');
        expression.push('/');
    } else {
        members[usize::from(b'/')] = false;
        if !members.iter().any(|member| *member) {
            expression.push_str("\\x00");
        }
    }
    for byte in 0..=u8::MAX {
        if members[usize::from(byte)] {
            push_byte(expression, byte);
        }
    }
    expression.push(']');
    Some(())
}

fn take_byte(pattern: &[u8], cursor: &mut usize) -> Option<u8> {
    let byte = *pattern.get(*cursor)?;
    *cursor += 1;
    Some(byte)
}

fn push_posix_class(members: &mut [bool; 256], class: &[u8]) -> Option<()> {
    let matches: fn(&u8) -> bool = match class {
        b"alnum" => u8::is_ascii_alphanumeric,
        b"alpha" => u8::is_ascii_alphabetic,
        b"blank" => |byte| matches!(byte, b' ' | b'\t'),
        b"cntrl" => u8::is_ascii_control,
        b"digit" => u8::is_ascii_digit,
        b"graph" => u8::is_ascii_graphic,
        b"lower" => u8::is_ascii_lowercase,
        b"print" => |byte| matches!(byte, b' '..=b'~'),
        b"punct" => u8::is_ascii_punctuation,
        b"space" => |byte| matches!(byte, b'\t'..=b'\r' | b' '),
        b"upper" => u8::is_ascii_uppercase,
        b"xdigit" => u8::is_ascii_hexdigit,
        _ => return None,
    };
    for byte in 0..=u8::MAX {
        members[usize::from(byte)] |= matches(&byte);
    }
    Some(())
}
