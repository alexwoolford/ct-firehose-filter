use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::error::KeywordSourceError;
use crate::event::{FrameMeta, MatchEvent};

/// Result of inspecting SANs against the watchlist (+ optional suppress set).
/// Production inspect uses an empty suppress set ([`DomainWatchlist::new`]): every
/// watchlist hit archives. [`new_with_suppress`] remains for tests / eval.
#[derive(Debug, Clone, PartialEq)]
pub struct InspectOutcome {
    pub event: Option<MatchEvent>,
    /// Watchlist matched, but every implicated name was on the suppress list.
    /// Production inspect has an empty suppress set, so this stays false.
    pub fully_suppressed: bool,
}

impl InspectOutcome {
    fn none() -> Self {
        Self {
            event: None,
            fully_suppressed: false,
        }
    }

    fn suppressed() -> Self {
        Self {
            event: None,
            fully_suppressed: true,
        }
    }

    fn matched(event: MatchEvent) -> Self {
        Self {
            event: Some(event),
            fully_suppressed: false,
        }
    }
}

/// Registrable-domain watchlist with PSL eTLD+1 lookup.
///
/// Matching is **exact host-suffix containment** only: a SAN hits when any DNS
/// suffix of the hostname equals a watchlist eTLD+1 (e.g. `s3.amazonaws.com` →
/// `amazonaws.com`). Brand-in-label and hyphen-token fuzzy matching are out of
/// scope — they false-positive heavily on large lists.
///
/// Optional **suppress** names exist only on [`new_with_suppress`]. Production
/// inspect is [`DomainWatchlist::new`]: every watchlist hit enqueues. A′ ignore
/// (event-df / partner-degree, plus optional operator files) lives in NoveltySink.
pub struct DomainWatchlist {
    names: HashSet<String>,
    suppress: HashSet<String>,
}

impl DomainWatchlist {
    pub fn new<W, WS>(watchlist: W) -> Self
    where
        W: IntoIterator<Item = WS>,
        WS: AsRef<str>,
    {
        Self::new_with_suppress(watchlist, std::iter::empty::<&str>())
    }

    pub fn new_with_suppress<W, WS, S, SS>(watchlist: W, suppress: S) -> Self
    where
        W: IntoIterator<Item = WS>,
        WS: AsRef<str>,
        S: IntoIterator<Item = SS>,
        SS: AsRef<str>,
    {
        let mut names = HashSet::new();
        for raw in watchlist {
            let Some(etld1) = etld1(raw.as_ref()) else {
                continue;
            };
            names.insert(etld1);
        }

        let mut suppress_set = HashSet::new();
        for raw in suppress {
            let Some(etld1) = etld1(raw.as_ref()) else {
                continue;
            };
            suppress_set.insert(etld1);
        }

        Self {
            names,
            suppress: suppress_set,
        }
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn suppress_len(&self) -> usize {
        self.suppress.len()
    }

    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    pub fn inspect<D: AsRef<str>>(&self, domains: &[D]) -> Option<MatchEvent> {
        self.inspect_outcome(domains, FrameMeta::default()).event
    }

    pub fn inspect_with_meta<D: AsRef<str>>(
        &self,
        domains: &[D],
        meta: FrameMeta<'_>,
    ) -> Option<MatchEvent> {
        self.inspect_outcome(domains, meta).event
    }

    pub fn inspect_outcome<D: AsRef<str>>(
        &self,
        domains: &[D],
        meta: FrameMeta<'_>,
    ) -> InspectOutcome {
        if self.names.is_empty() {
            return InspectOutcome::none();
        }

        let mut implicated: HashSet<String> = HashSet::new();
        let mut san_hosts: Vec<(String, String)> = Vec::new(); // (raw, normalized host)

        for san in domains {
            let raw = san.as_ref();
            let Some(host) = normalize_host(raw) else {
                continue;
            };
            let mut hit = false;

            for candidate in host_suffixes(&host) {
                if self.names.contains(candidate) {
                    implicated.insert(candidate.to_string());
                    hit = true;
                }
            }

            if hit {
                san_hosts.push((raw.to_string(), host));
            }
        }

        if implicated.is_empty() {
            return InspectOutcome::none();
        }

        let before_suppress = implicated.len();
        implicated.retain(|name| !self.suppress.contains(name));
        if implicated.is_empty() {
            debug_assert!(before_suppress > 0);
            return InspectOutcome::suppressed();
        }

        let mut matched_domains = Vec::new();
        for (raw, host) in san_hosts {
            let keep = host_suffixes(&host).any(|candidate| implicated.contains(candidate));
            if keep {
                matched_domains.push(raw);
            }
        }

        if matched_domains.is_empty() {
            return InspectOutcome::suppressed();
        }

        let mut matched_watchlist: Vec<String> = implicated.into_iter().collect();
        matched_watchlist.sort_unstable();

        let san_count = u32::try_from(domains.len()).unwrap_or(u32::MAX);
        InspectOutcome::matched(
            MatchEvent::new(
                matched_domains,
                matched_watchlist,
                meta.seen,
                meta.source.map(str::to_string),
                meta.fingerprint.map(str::to_string),
            )
            .with_san_count(san_count),
        )
    }
}

/// Lock-free hot-swappable watchlist.
pub struct HotWatchlist {
    inner: ArcSwap<DomainWatchlist>,
}

impl HotWatchlist {
    pub fn new(watchlist: DomainWatchlist) -> Self {
        Self {
            inner: ArcSwap::from_pointee(watchlist),
        }
    }

    pub fn swap(&self, watchlist: DomainWatchlist) {
        self.inner.store(Arc::new(watchlist));
    }

    pub fn load(&self) -> arc_swap::Guard<Arc<DomainWatchlist>> {
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
        self.inspect_outcome(domains, meta).event
    }

    pub fn inspect_outcome<D: AsRef<str>>(
        &self,
        domains: &[D],
        meta: FrameMeta<'_>,
    ) -> InspectOutcome {
        self.inner.load().inspect_outcome(domains, meta)
    }
}

/// One domain per line. Blank lines and `#` comments are ignored.
pub fn load_domain_file(path: impl AsRef<Path>) -> Result<Vec<String>, KeywordSourceError> {
    let path = path.as_ref();
    let text = std::fs::read_to_string(path)
        .map_err(|e| KeywordSourceError::Load(format!("{}: {e}", path.display())))?;
    Ok(parse_domain_lines(&text))
}

/// Load an optional classifier / ignore file. Empty or missing path ⇒ no names.
pub fn load_suppress_file(path: impl AsRef<Path>) -> Result<Vec<String>, KeywordSourceError> {
    let path = path.as_ref();
    if path.as_os_str().is_empty() || !path.exists() {
        return Ok(Vec::new());
    }
    load_domain_file(path)
}

/// Merge optional operator ignore files for NoveltySink (not inspect).
/// Production inspect is [`DomainWatchlist::new`] — do not pass this set to
/// [`DomainWatchlist::new_with_suppress`] or those names never archive.
pub fn load_suppress_and_glue(
    suppress_path: impl AsRef<Path>,
    glue_path: impl AsRef<Path>,
) -> Result<Vec<String>, KeywordSourceError> {
    let mut names = load_suppress_file(suppress_path)?;
    names.extend(load_suppress_file(glue_path)?);
    Ok(names)
}

pub fn parse_domain_lines(text: &str) -> Vec<String> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_string)
        .collect()
}

fn normalize_host(raw: &str) -> Option<String> {
    let trimmed = raw.trim().trim_end_matches('.').trim();
    if trimmed.is_empty() {
        return None;
    }
    let lower = trimmed.to_ascii_lowercase();
    let host = lower.strip_prefix("*.").unwrap_or(&lower);
    if host.is_empty() {
        None
    } else {
        Some(host.to_string())
    }
}

/// Every DNS suffix of `host` (`a.b.c.com` → `a.b.c.com`, `b.c.com`, `c.com`, `com`).
///
/// This is the uniform "is this SAN under a watchlist name?" check. It works even when
/// the SAN itself is a Public Suffix entry (e.g. `s3.amazonaws.com`).
fn host_suffixes(host: &str) -> impl Iterator<Item = &str> {
    std::iter::once(host).chain(host.match_indices('.').map(|(i, _)| &host[i + 1..]))
}

fn etld1(host: &str) -> Option<String> {
    let host = normalize_host(host)?;
    let parsed = addr::parse_domain_name(&host).ok()?;
    let suffix = parsed.suffix();
    // The name is itself a public suffix. Inserting `com` / `github.io` would
    // make suffix-walk match every child. Deeper private suffixes such as
    // `s3.amazonaws.com` stay as keys so a more specific watchlist still works.
    if parsed.has_known_suffix() && suffix.eq_ignore_ascii_case(&host) {
        let labels = host.split('.').filter(|l| !l.is_empty()).count();
        if parsed.is_icann() || labels <= 2 {
            return None;
        }
        return Some(host);
    }
    parsed.root().map(str::to_ascii_lowercase)
}
