use super::{v03, v04, v17};
use native_db::{ToKey, native_db};
use native_model::{Model, native_model};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[native_model(id = 3, version = 18, from = v17::SelectedTorrent)]
#[native_db(export_keys = true)]
pub struct SelectedTorrent {
    #[primary_key]
    pub mam_id: u64,
    pub goodreads_id: Option<u64>,
    #[secondary_key(unique, optional)]
    pub hash: Option<String>,
    pub dl_link: String,
    pub unsat_buffer: Option<u64>,
    pub wedge_buffer: Option<u64>,
    pub cost: v04::TorrentCost,
    pub category: Option<String>,
    pub tags: Vec<String>,
    #[secondary_key]
    pub title_search: String,
    pub meta: v17::TorrentMeta,
    pub grabber: Option<String>,
    /// Selected from MLM's web pages rather than by an autograbber or list import
    pub by_hand: bool,
    pub created_at: v03::Timestamp,
    pub started_at: Option<v03::Timestamp>,
    pub removed_at: Option<v03::Timestamp>,
}

impl From<v17::SelectedTorrent> for SelectedTorrent {
    fn from(t: v17::SelectedTorrent) -> Self {
        Self {
            mam_id: t.mam_id,
            goodreads_id: t.goodreads_id,
            hash: t.hash,
            dl_link: t.dl_link,
            unsat_buffer: t.unsat_buffer,
            wedge_buffer: t.wedge_buffer,
            cost: t.cost,
            category: t.category,
            tags: t.tags,
            title_search: t.title_search,
            meta: t.meta,
            grabber: t.grabber,
            by_hand: false,
            created_at: t.created_at,
            started_at: t.started_at,
            removed_at: t.removed_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MODELS, migrate, v10, v13};

    #[test]
    fn test_migrate_from_v17() {
        let db = native_db::Builder::new().create_in_memory(&MODELS).unwrap();
        let old = v17::SelectedTorrent {
            mam_id: 1,
            goodreads_id: Some(5),
            hash: None,
            dl_link: "dl".to_owned(),
            unsat_buffer: Some(2),
            wedge_buffer: Some(3),
            cost: v04::TorrentCost::TryWedge,
            category: None,
            tags: vec!["tag".to_owned()],
            title_search: "book".to_owned(),
            meta: v17::TorrentMeta {
                mam_id: 1,
                vip_status: None,
                cat: None,
                media_type: v13::MediaType::Audiobook,
                main_cat: None,
                categories: vec![],
                language: None,
                flags: None,
                filetypes: vec!["m4b".to_owned()],
                num_files: 1,
                size: v03::Size::from_bytes(100),
                title: "Book".to_owned(),
                edition: None,
                authors: vec![],
                narrators: vec![],
                series: vec![],
                source: v10::MetadataSource::Mam,
                uploaded_at: v03::Timestamp::now(),
            },
            grabber: Some("grabber".to_owned()),
            created_at: v03::Timestamp::now(),
            started_at: None,
            removed_at: None,
        };
        let rw = db.rw_transaction().unwrap();
        rw.insert(old).unwrap();
        rw.commit().unwrap();

        migrate(&db).unwrap();

        // Selections made before by_hand existed count as automatic
        let r = db.r_transaction().unwrap();
        let new = r.get().primary::<SelectedTorrent>(1u64).unwrap().unwrap();
        assert!(!new.by_hand);
        assert_eq!(new.goodreads_id, Some(5));
        assert_eq!(new.unsat_buffer, Some(2));
        assert_eq!(new.wedge_buffer, Some(3));
        assert_eq!(new.cost, v04::TorrentCost::TryWedge);
        assert_eq!(new.tags, ["tag"]);
        assert_eq!(new.title_search, "book");
        assert_eq!(new.grabber.as_deref(), Some("grabber"));
        assert_eq!(new.meta.title, "Book");
        let old_left = r
            .scan()
            .primary::<v17::SelectedTorrent>()
            .unwrap()
            .all()
            .unwrap()
            .count();
        assert_eq!(old_left, 0);
    }
}
