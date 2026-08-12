use std::sync::Arc;

use aho_corasick::{AhoCorasick, AhoCorasickBuilder, MatchKind};
use arc_swap::ArcSwap;

use crate::event::{FrameMeta, MatchEvent};

/// Compiled Aho-Corasick automaton with DNS-label-boundary matching.
pub struct KeywordAutomaton {
    ac: Option<AhoCorasick>,
    keywords: Vec<String>,
}

impl KeywordAutomaton {
    pub fn new<I, S>(keywords: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let keywords: Vec<String> = keywords
            .into_iter()
            .map(|s| s.as_ref().to_string())
            .collect();
        if keywords.is_empty() {
            return Self { ac: None, keywords };
        }
        let ac = AhoCorasickBuilder::new()
            .ascii_case_insensitive(true)
            .match_kind(MatchKind::Standard)
            .build(&keywords)
            .expect("Aho-Corasick automaton should build from UTF-8 keywords");
        Self {
            ac: Some(ac),
            keywords,
        }
    }

    pub fn keywords(&self) -> &[String] {
        &self.keywords
    }

    pub fn is_empty(&self) -> bool {
        self.keywords.is_empty()
    }

    pub fn inspect<D: AsRef<str>>(&self, domains: &[D]) -> Option<MatchEvent> {
        self.inspect_with_meta(domains, FrameMeta::default())
    }

    pub fn inspect_with_meta<D: AsRef<str>>(
        &self,
        domains: &[D],
        meta: FrameMeta<'_>,
    ) -> Option<MatchEvent> {
        let ac = self.ac.as_ref()?;
        let mut matched_domains = Vec::new();
        let mut matched_keywords = Vec::new();

        for domain in domains {
            let domain = domain.as_ref();
            if domain.is_empty() {
                continue;
            }
            let mut hit = false;
            for mat in ac.find_overlapping_iter(domain) {
                if !is_label_boundary(domain, mat.start(), mat.end()) {
                    continue;
                }
                hit = true;
                let kw = &self.keywords[mat.pattern().as_usize()];
                if !matched_keywords.iter().any(|existing| existing == kw) {
                    matched_keywords.push(kw.clone());
                }
            }
            if hit {
                matched_domains.push(domain.to_string());
            }
        }

        if matched_domains.is_empty() {
            return None;
        }

        Some(MatchEvent::new(
            matched_domains,
            matched_keywords,
            meta.seen,
            meta.source.map(str::to_string),
            meta.fingerprint.map(str::to_string),
        ))
    }
}

fn is_label_boundary(haystack: &str, start: usize, end: usize) -> bool {
    let bytes = haystack.as_bytes();
    let left_ok = start == 0 || bytes.get(start.wrapping_sub(1)) == Some(&b'.');
    let right_ok = end == bytes.len() || bytes.get(end) == Some(&b'.');
    left_ok && right_ok
}

/// Lock-free hot-swappable automaton (`arc-swap`).
pub struct HotAutomaton {
    inner: ArcSwap<KeywordAutomaton>,
}

impl HotAutomaton {
    pub fn new(automaton: KeywordAutomaton) -> Self {
        Self {
            inner: ArcSwap::from_pointee(automaton),
        }
    }

    pub fn swap(&self, automaton: KeywordAutomaton) {
        self.inner.store(Arc::new(automaton));
    }

    pub fn load(&self) -> arc_swap::Guard<Arc<KeywordAutomaton>> {
        self.inner.load()
    }

    pub fn inspect<D: AsRef<str>>(&self, domains: &[D]) -> Option<MatchEvent> {
        self.inspect_with_meta(domains, FrameMeta::default())
    }

    pub fn inspect_with_meta<D: AsRef<str>>(
        &self,
        domains: &[D],
        meta: FrameMeta<'_>,
    ) -> Option<MatchEvent> {
        self.inner.load().inspect_with_meta(domains, meta)
    }
}
