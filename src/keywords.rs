use std::path::{Path, PathBuf};

use async_trait::async_trait;

use crate::error::KeywordSourceError;

#[async_trait]
pub trait KeywordSource: Send + Sync {
    async fn load(&self) -> Result<Vec<String>, KeywordSourceError>;
}

#[derive(Clone, Debug, Default)]
pub struct MemoryKeywordSource {
    keywords: Vec<String>,
}

impl MemoryKeywordSource {
    pub fn new<I, S>(keywords: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self {
            keywords: keywords
                .into_iter()
                .map(|s| s.as_ref().to_string())
                .collect(),
        }
    }

    pub fn set<I, S>(&mut self, keywords: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.keywords = keywords
            .into_iter()
            .map(|s| s.as_ref().to_string())
            .collect();
    }
}

#[async_trait]
impl KeywordSource for MemoryKeywordSource {
    async fn load(&self) -> Result<Vec<String>, KeywordSourceError> {
        Ok(self.keywords.clone())
    }
}

/// One keyword per line. Blank lines and `#` comments are ignored.
#[derive(Clone, Debug)]
pub struct FileKeywordSource {
    path: PathBuf,
}

impl FileKeywordSource {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }
}

#[async_trait]
impl KeywordSource for FileKeywordSource {
    async fn load(&self) -> Result<Vec<String>, KeywordSourceError> {
        let text = tokio::fs::read_to_string(&self.path)
            .await
            .map_err(|e| KeywordSourceError::Load(format!("{}: {e}", self.path.display())))?;
        Ok(text
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(str::to_string)
            .collect())
    }
}
