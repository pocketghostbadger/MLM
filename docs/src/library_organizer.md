# Library Organizer

The library organizer hardlinks files into a directory with the directory structure:
```sh
"Author/Series/Series #1 - Title {Narrator}/" # For audiobooks with a series
"Author/Title {Narrator}/" # For audiobooks without a series
"Author/Series/Series #1 - Title/" # For ebooks with a series
"Author/Title/" # For ebooks without a series
```

If you do not want to include narrator in the audiobook folder names, the top level option
```toml
exclude_narrator_in_library_dir = true
```
can be set. This makes the MLM directory structure match booktree which allows easier migration from booktree.

You can select either a category or a qBittorrent download directory to link to a library.

Link all torrents with category "Audiobooks" to "/mnt/Data/Library/Audiobooks":
```toml
[[library]]
category = "Audiobooks"
library_dir = "/mnt/Data/Library/Audiobooks"
```

Link all torrents with download directory/save location "/mnt/Data/Downloads/Ebooks" to "/mnt/Data/Library/Audiobooks":
```toml
[[library]]
download_dir = "/mnt/Data/Downloads/Ebooks"
library_dir = "/mnt/Data/Library/Ebooks"
```

<div class="warning">
Note that you can use either `category` or `download_dir` to select torrents for a library, not both.
</div>

It's possible to use tags to additionally filter down which torrents to link:
```toml
[[library]]
download_dir = "/mnt/Data/Uploads/Audiobooks"
library_dir = "/mnt/Data/Library/Audiobooks"
allow_tags = [ "library" ] # only links torrents with tag "library"
deny_tags = [ "skip" ] # but not if also having tag "skip"
```

When specifying multiple `allow_tags`, the torrent just need to have any of them to be linked.
When specifying multiple `deny_tags`, the torrent just need to have any of them to be skipped.

### Method
It's possible to instead copy or symlink files to the library if hardlinking does not work for you:
```
method = "hardlink_or_copy" # Try hardlinking but fallback to copy if required
method = "hardlink_or_symlink" # Try hardlinking but fallback to symlink if required
method = "copy" # Always copy files
method = "symlink" # Always symlink files
```

Example usage:
```toml
[[library]]
category = "Audiobooks"
library_dir = "/mnt/Data/Library/Audiobooks"
method = "copy"
```

### File Types
A list of audio and ebook file types in order of preference that will be linked from this library, the default config are:
```toml
audio_types = ["m4b", "m4a", "mp4", "mp3", "ogg"]
ebook_types = ["cbz", "epub", "pdf", "mobi", "azw3", "azw", "cbr"]
```

Only one format from each list will be linked. This means that a multi-format ebook torrent will only have its best format linked. E.g. for a torrent with `epub`, `pdf` and `mobi` files, only the `epub` will be linked. But as one format from each list is selected, an audiobook torrent with a supplementary PDF will have both the audiofiles and the PDF linked.

### Ebooks in audiobook folders
Audiobookshelf shows an ebook in an audiobook's folder as that audiobook's ebook, and apps like Prologue can show its text while the audio plays. An ebook library can also link its ebooks into the folders of matching audiobooks, while the ebook keeps its own folder:
```toml
[[library]]
download_dir = "/mnt/Data/Downloads/Ebooks"
library_dir = "/mnt/Data/Library/Ebooks"
link_to_audiobooks = true             # off by default
link_to_audiobooks_types = ["epub"]   # formats that go into audiobook folders, in order of preference (default)
link_to_audiobooks_existing = true    # also pair books that were both linked before MLM last started (default)
```

An ebook matches an audiobook in the library with the same title and language and at least one shared author. Editions only keep them apart when both have a numbered edition and the numbers differ, so a 2nd edition ebook doesn't go into a 3rd edition audiobook's folder, but "Dune, 40th Anniversary Edition" goes into the folder of "Dune". Comics, manga and periodicals aren't linked. If an ebook's folder is in the `library_dir` of several libraries (the same folder, or one inside another), MLM tells them apart by a category library's category, or by a `name` you give each `[[library]]` (stored for each torrent on the next linker run); an ebook it can't place isn't paired, and the log says so.

Each audiobook folder gets at most one ebook, even if several audiobooks share it, linked with the ebook library's method and without a `metadata.json` of its own. MLM adds none if the folder already has a file in one of the ebook library's `link_to_audiobooks_types`, such as an epub from the audiobook's own torrent or one put there by hand, and never replaces a file. If several ebooks match, it picks the one whose format comes first in `link_to_audiobooks_types`, then one with the same edition as the audiobook, then the one MLM added last; when several audiobooks share a folder, the ebooks matching any of them are compared this way. Once a folder has an ebook, it keeps it when another matching ebook is added later.

Pairing runs after the library cleaner, each time the linker runs. When either book is relinked, replaced or cleaned, MLM first takes the ebook out of the audiobook's folder, then works out again which ebook goes where. Links also go when the two books no longer match, for example after a metadata edit. MLM only deletes a linked ebook it recognises as the file it linked, by its file id, as the ebook's own file, or by the same content; a file put in its place that passes none of these checks is left alone. While `link_to_audiobooks` is on, a folder whose linked ebook was deleted gets an ebook again on the next run, picked as above, so it can be a better one added since (with `link_to_audiobooks_existing = false`, only if the audiobook or that ebook was linked since MLM last started). Turning `link_to_audiobooks` off stops new links, and removing a format from `link_to_audiobooks_types` stops new links in that format. Ebooks already linked stay, unless one of the reasons above removes them.

To keep ebooks out of an audiobook's folder, give the audiobook's torrent a qBittorrent tag listed in the ebook library's `link_to_audiobooks_deny_tags` (in each ebook library's, if you have several), for example on the torrent's page in MLM. If several audiobooks share the folder, tag each of them. The same tag on an ebook's torrent keeps that ebook out of every audiobook folder, though another matching ebook can then take its place:
```toml
link_to_audiobooks_deny_tags = ["no-ebook-link"]
```
On the next run, MLM takes out the ebooks it linked where a tag applies, and doesn't link them there again until the tag is removed. Create the tag in qBittorrent first, since the torrent page only lists tags that exist. If a qBittorrent instance can't be reached or doesn't answer within 30 seconds, MLM doesn't add links for ebook libraries with `link_to_audiobooks_deny_tags`, or remove any because of tags, until it can.

With `link_to_audiobooks_existing = false`, an ebook and an audiobook are only paired if at least one of them was linked since MLM last started; each restart starts this over. Keep the ebook library's folder out of your Audiobookshelf libraries: by default, Audiobookshelf would list each ebook there as a book of its own.
