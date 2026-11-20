//! Pure classification of volume paths by compiled `.studiofsignore` rules.

mod wildmatch;

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use regex_automata::meta::Regex;
use regex_automata::nfa::thompson::WhichCaptures;
use regex_automata::util::syntax;
use regex_automata::{Anchored, Input, MatchKind, PatternSet};
use studiofs_meta::fold_dentry_name;

/// Largest rule file payload, in bytes, that is parsed and compiled.
pub const MAX_RULE_BYTES: usize = 64 * 1024;
pub(crate) const RULE_FILE: &[u8] = b".studiofsignore";
/// Lazy DFA cache limit. Thousands of wildcard rules share DFA states that
/// exceed the 2 MiB default, which falls back to a slower NFA simulation.
const DFA_CACHE_BYTES: usize = 8 * 1024 * 1024;

/// Classification of a volume path by the rules that apply to it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScratchClass {
    /// Ordinary volume data; rule files are always tracked.
    Tracked,
    /// Matched by a scratch rule or a built-in default, here or in an
    /// ancestor directory.
    Scratch,
}

/// A pattern that could match a rule file, which is always tracked.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuleDiagnostic {
    /// One-based source line of the pattern.
    pub line: usize,
}

/// Why a rule file was rejected; the directory keeps its prior rules.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RuleUpdateError {
    /// The payload is larger than [`MAX_RULE_BYTES`].
    #[error("rule file exceeds the {MAX_RULE_BYTES}-byte limit")]
    TooLarge,
    /// The payload contains a NUL byte.
    #[error("rule file must not contain NUL bytes")]
    InvalidPayload,
    /// A pattern could not be compiled.
    #[error("cannot compile rule on line {line}")]
    InvalidPattern {
        /// One-based source line of the rejected pattern.
        line: usize,
    },
}

/// Compiled rule files indexed by their volume-relative directory.
#[derive(Clone, Debug)]
pub struct ScratchRules {
    case_insensitive: bool,
    directories: HashMap<Vec<u8>, Arc<RuleSet>>,
}

/// One rule file, with a multi-pattern matcher for each combination of
/// basename-only and directory-only rules, indexed by [`group`].
#[derive(Debug, Default)]
struct RuleSet {
    groups: [Option<RuleGroup>; 4],
    diagnostics: Vec<RuleDiagnostic>,
}

/// Anchored patterns inserted newest first, so the leftmost-first match is
/// the last matching rule in the group.
#[derive(Debug)]
struct RuleGroup {
    matcher: Regex,
    /// Source line and class of each pattern, by pattern ID.
    rules: Vec<(usize, ScratchClass)>,
}

/// Pattern expressions and source rules for one group, in source order.
type GroupSource = (Vec<String>, Vec<(usize, ScratchClass)>);

/// Pre-resolved compiled rules for direct children of one tracked directory.
#[derive(Clone, Debug)]
pub(crate) struct CompiledDirRules {
    case_insensitive: bool,
    ancestor_excluded: bool,
    /// Applicable rule sets ordered innermost-to-outermost, paired with the
    /// slash-terminated prefix to prepend to a folded child name (empty when
    /// the rule file lives in this directory itself).
    scopes: Vec<(Vec<u8>, Arc<RuleSet>)>,
}

impl CompiledDirRules {
    /// Classify a direct child entry name of this directory without re-walking
    /// ancestor paths or looking up rule-file maps.
    pub(crate) fn classify_child(&self, name: &[u8], is_dir: bool) -> ScratchClass {
        let folded = if self.case_insensitive {
            fold_dentry_name(name)
        } else {
            Cow::Borrowed(name)
        };
        if folded.is_empty() || folded.as_ref() == RULE_FILE {
            return ScratchClass::Tracked;
        }
        if self.ancestor_excluded {
            return ScratchClass::Scratch;
        }
        let mut relative_buf = Vec::new();
        for (prefix, set) in &self.scopes {
            let relative = if prefix.is_empty() {
                folded.as_ref()
            } else {
                relative_buf.clear();
                relative_buf.reserve(prefix.len() + folded.len());
                relative_buf.extend_from_slice(prefix);
                relative_buf.extend_from_slice(folded.as_ref());
                relative_buf.as_slice()
            };
            if let Some(class) = set.matching_class(folded.as_ref(), relative, is_dir) {
                return class;
            }
        }
        if folded.starts_with(b"._") {
            ScratchClass::Scratch
        } else {
            ScratchClass::Tracked
        }
    }
}

impl ScratchRules {
    /// Pre-resolve the rule scopes applicable to direct children of `dir_path`
    /// (empty for the volume root).
    pub(crate) fn compile_directory(&self, dir_path: &[u8]) -> CompiledDirRules {
        let dir_path = self.fold(dir_path);
        let mut ancestor_excluded = false;
        if !dir_path.is_empty() {
            for (end, _) in dir_path
                .iter()
                .enumerate()
                .filter(|(_, byte)| **byte == b'/')
            {
                if basename(&dir_path[..end]) != RULE_FILE
                    && self.classify_entry(&dir_path[..end], true) == ScratchClass::Scratch
                {
                    ancestor_excluded = true;
                    break;
                }
            }
            if !ancestor_excluded
                && basename(&dir_path) != RULE_FILE
                && self.classify_entry(&dir_path, true) == ScratchClass::Scratch
            {
                ancestor_excluded = true;
            }
        }

        let mut scopes = Vec::new();
        if !ancestor_excluded {
            if let Some(set) = self.directories.get(dir_path.as_ref()) {
                scopes.push((Vec::new(), Arc::clone(set)));
            }
            if !dir_path.is_empty() {
                for (index, _) in dir_path
                    .iter()
                    .enumerate()
                    .rev()
                    .filter(|(_, byte)| **byte == b'/')
                {
                    let scope = &dir_path[..index];
                    if let Some(set) = self.directories.get(scope) {
                        let mut prefix = Vec::with_capacity(dir_path.len() - index);
                        prefix.extend_from_slice(&dir_path[index + 1..]);
                        prefix.push(b'/');
                        scopes.push((prefix, Arc::clone(set)));
                    }
                }
                if let Some(set) = self.directories.get(b"".as_slice()) {
                    let mut prefix = Vec::with_capacity(dir_path.len() + 1);
                    prefix.extend_from_slice(&dir_path);
                    prefix.push(b'/');
                    scopes.push((prefix, Arc::clone(set)));
                }
            }
        }

        CompiledDirRules {
            case_insensitive: self.case_insensitive,
            ancestor_excluded,
            scopes,
        }
    }

    /// Create an empty rule index. With `case_insensitive`, patterns,
    /// directory keys, and classified paths are compared case-folded.
    pub fn new(case_insensitive: bool) -> Self {
        Self {
            case_insensitive,
            directories: HashMap::new(),
        }
    }

    /// Replace one directory's rules. A failed update retains its prior rules
    /// and diagnostics. Directory keys use the same format as classification
    /// paths, with an empty key for the volume root.
    pub fn update(
        &mut self,
        directory: &[u8],
        payload: &[u8],
    ) -> Result<&[RuleDiagnostic], RuleUpdateError> {
        if payload.len() > MAX_RULE_BYTES {
            return Err(RuleUpdateError::TooLarge);
        }
        if payload.contains(&0) {
            return Err(RuleUpdateError::InvalidPayload);
        }

        let mut groups: [GroupSource; 4] = Default::default();
        let mut rule_file_patterns = Vec::new();
        let mut rule_file_lines = Vec::new();
        let payload = payload.strip_prefix(b"\xef\xbb\xbf").unwrap_or(payload);
        for (index, line) in payload.split(|byte| *byte == b'\n').enumerate() {
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            if line.starts_with(b"#") {
                continue;
            }
            let line = trim_unescaped_spaces(line);
            let (pattern, class) = match line.strip_prefix(b"!") {
                Some(pattern) => (pattern, ScratchClass::Tracked),
                None => (line, ScratchClass::Scratch),
            };
            let directory_only = pattern.ends_with(b"/");
            let pattern = pattern.strip_suffix(b"/").unwrap_or(pattern);
            let basename_only = !pattern.contains(&b'/');
            let pattern = pattern.strip_prefix(b"/").unwrap_or(pattern);
            if pattern.is_empty() {
                continue;
            }
            let line = index + 1;
            let compiled = wildmatch::compile(pattern, self.case_insensitive)
                .ok_or(RuleUpdateError::InvalidPattern { line })?;
            if !directory_only && !compiled.empty_component {
                rule_file_patterns.push(compiled.last_component);
                rule_file_lines.push(line);
            }
            let (patterns, rules) = &mut groups[group(basename_only, directory_only)];
            patterns.push(compiled.expression);
            rules.push((line, class));
        }

        let mut set = RuleSet::default();
        for (slot, (mut patterns, mut rules)) in set.groups.iter_mut().zip(groups) {
            if patterns.is_empty() {
                continue;
            }
            patterns.reverse();
            rules.reverse();
            let matcher = build(&patterns, MatchKind::LeftmostFirst)
                .ok_or(RuleUpdateError::InvalidPattern { line: rules[0].0 })?;
            *slot = Some(RuleGroup { matcher, rules });
        }
        if !rule_file_patterns.is_empty() {
            let matcher = build(&rule_file_patterns, MatchKind::All).ok_or(
                RuleUpdateError::InvalidPattern {
                    line: rule_file_lines[0],
                },
            )?;
            let mut matches = PatternSet::new(matcher.pattern_len());
            matcher.which_overlapping_matches(
                &Input::new(RULE_FILE).anchored(Anchored::Yes),
                &mut matches,
            );
            set.diagnostics = matches
                .iter()
                .map(|pattern| RuleDiagnostic {
                    line: rule_file_lines[pattern.as_usize()],
                })
                .collect();
        }
        let directory = self.fold(directory).into_owned();
        Ok(&self
            .directories
            .entry(directory)
            .insert_entry(Arc::new(set))
            .into_mut()
            .diagnostics)
    }

    /// Remove one directory's rule file and fall back to inherited rules.
    pub fn remove(&mut self, directory: &[u8]) {
        let directory = self.fold(directory);
        self.directories.remove(directory.as_ref());
    }

    /// Classify a canonical volume-relative path of slash-separated dentry
    /// names. Callers supply a path with no empty, dot, or dot-dot components,
    /// and whether its final entry is a directory.
    pub fn classify(&self, path: &[u8], is_dir: bool) -> ScratchClass {
        let path = self.fold(path);
        if path.is_empty() || basename(&path) == RULE_FILE {
            return ScratchClass::Tracked;
        }

        for (end, _) in path.iter().enumerate().filter(|(_, byte)| **byte == b'/') {
            if basename(&path[..end]) != RULE_FILE
                && self.classify_entry(&path[..end], true) == ScratchClass::Scratch
            {
                return ScratchClass::Scratch;
            }
        }
        self.classify_entry(&path, is_dir)
    }

    fn classify_entry(&self, path: &[u8], is_dir: bool) -> ScratchClass {
        let name = basename(path);
        let scopes = path
            .iter()
            .enumerate()
            .rev()
            .filter(|(_, byte)| **byte == b'/')
            .map(|(index, _)| (&path[..index], &path[index + 1..]))
            .chain(std::iter::once((&path[..0], path)));
        for (scope, relative) in scopes {
            if let Some(set) = self.directories.get(scope)
                && let Some(class) = set.matching_class(name, relative, is_dir)
            {
                return class;
            }
        }
        if name.starts_with(b"._") {
            ScratchClass::Scratch
        } else {
            ScratchClass::Tracked
        }
    }

    fn fold<'a>(&self, path: &'a [u8]) -> Cow<'a, [u8]> {
        // Slash breaks Unicode lowercase context between dentry names.
        if self.case_insensitive {
            fold_dentry_name(path)
        } else {
            Cow::Borrowed(path)
        }
    }
}

impl RuleSet {
    /// Return the class of the last rule that matches, searching each
    /// applicable group once.
    fn matching_class(&self, name: &[u8], relative: &[u8], is_dir: bool) -> Option<ScratchClass> {
        let directory_only: &[bool] = if is_dir { &[false, true] } else { &[false] };
        [(true, name), (false, relative)]
            .into_iter()
            .flat_map(|(basename_only, haystack)| {
                directory_only.iter().filter_map(move |directory_only| {
                    let group = self.groups[group(basename_only, *directory_only)].as_ref()?;
                    let found = group
                        .matcher
                        .search_half(&Input::new(haystack).anchored(Anchored::Yes))?;
                    Some(group.rules[found.pattern().as_usize()])
                })
            })
            .max_by_key(|(line, _)| *line)
            .map(|(_, class)| class)
    }
}

fn group(basename_only: bool, directory_only: bool) -> usize {
    usize::from(basename_only) << 1 | usize::from(directory_only)
}

fn build(patterns: &[String], kind: MatchKind) -> Option<Regex> {
    // The rule file size bounds the NFA, so do not reject large files.
    Regex::builder()
        .configure(
            Regex::config()
                .match_kind(kind)
                .utf8_empty(false)
                .which_captures(WhichCaptures::Implicit)
                .nfa_size_limit(None)
                .hybrid_cache_capacity(DFA_CACHE_BYTES),
        )
        .syntax(syntax::Config::new().utf8(false))
        .build_many(patterns)
        .ok()
}

fn basename(path: &[u8]) -> &[u8] {
    path.rsplit(|byte| *byte == b'/').next().unwrap_or_default()
}

fn trim_unescaped_spaces(line: &[u8]) -> &[u8] {
    let mut escape = false;
    let mut end = 0;
    for (index, byte) in line.iter().enumerate() {
        if escape || *byte != b' ' {
            end = index + 1;
        }
        escape = !escape && *byte == b'\\';
    }
    &line[..end]
}
