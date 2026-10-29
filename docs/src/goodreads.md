# Goodreads Import

Goodreads lists/bookshelves can be used as a source for autograbbing books.
Give MLM the URL of the bookshelf page, e.g. `https://www.goodreads.com/review/list/12345678-name?shelf=to-read`.

Goodreads stopped serving the bookshelf RSS feeds in October 2026. MLM now reads the shelf through Goodreads' public
bookshelf widget instead, which returns the 100 most recently added books. Existing `review/list_rss` URLs keep
working and don't need to be changed.

Example configuration:
```toml
[[goodreads_list]]
url = "https://www.goodreads.com/review/list/..." # A Goodreads bookshelf page, or an old list_rss feed URL

[[goodreads_list.grab]] # A block deciding what torrents to grab from the list,
                        # if a torrent matches multiple blocks, the first one is used
cost = "all"
languages = [ "english" ] # you can use the same search filters as for autograb blocks
max_size = "15 MiB"

[[goodreads_list.grab]]
cost = "wedge" # automatically wedge torrents before download, as we have an cost=all block
               # before this one with a max_size, this will only wedge torrents > 15 MiB
languages = [ "english" ]
```

Each list needs at least one `goodreads_list.grab` block that select what torrents to grab. To see how to select torrents and what fields you can set, see [Search Filters](./search_filters.md).
