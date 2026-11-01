use super::v03;
use native_db::{ToKey, native_db};
use native_model::{Model, native_model};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// An ebook linked into the folder of a matching audiobook, besides its own library folder
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[native_model(id = 9, version = 19)]
#[native_db(export_keys = true)]
pub struct EbookLink {
    /// An audiobook folder gets at most one linked ebook
    #[primary_key]
    pub audiobook_id: String,
    #[secondary_key]
    pub ebook_id: String,
    /// The linked file in the audiobook's folder
    pub path: PathBuf,
    /// The ebook's file in its own library folder that was linked
    pub source: PathBuf,
    /// Identifies the linked file, so a file put there later isn't taken for it
    pub file_id: Option<String>,
    pub created_at: v03::Timestamp,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MODELS, migrate};

    #[test]
    fn test_ebook_link_by_ebook() {
        let db = native_db::Builder::new().create_in_memory(&MODELS).unwrap();
        migrate(&db).unwrap();
        let link = |audiobook: &str| EbookLink {
            audiobook_id: audiobook.to_owned(),
            ebook_id: "ebook".to_owned(),
            path: PathBuf::from(format!("/audiobooks/{audiobook}/book.epub")),
            source: PathBuf::from("/ebooks/book/book.epub"),
            file_id: None,
            created_at: v03::Timestamp::now(),
        };
        let rw = db.rw_transaction().unwrap();
        rw.insert(link("a")).unwrap();
        rw.insert(link("b")).unwrap();
        rw.commit().unwrap();

        let r = db.r_transaction().unwrap();
        let links = r
            .scan()
            .secondary::<EbookLink>(EbookLinkKey::ebook_id)
            .unwrap()
            .range("ebook".to_owned()..="ebook".to_owned())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let audiobooks = links
            .iter()
            .map(|l| l.audiobook_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(audiobooks, ["a", "b"]);
        assert_eq!(links[0].path, PathBuf::from("/audiobooks/a/book.epub"));
    }
}
