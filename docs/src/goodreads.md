# Goodreads Import

Goodreads lists/bookshelves can be used as a source for autograbbing books.
Give MLM the URL of the bookshelf page, e.g. `https://www.goodreads.com/review/list/12345678-name?shelf=to-read`.
The URL has to include a `shelf`, MLM can't import all books of a user.

Goodreads stopped serving the bookshelf RSS feeds in October 2026. MLM now reads the shelf through Goodreads' public
bookshelf widget instead. This differs from the RSS feeds in a few ways:
- The widget doesn't use the RSS `key` and needs no sign-in. In testing (October 2026) it still returned the shelves
  of profiles set to "Goodreads members" or "just my friends", so private profiles keep working. Be aware that this
  also means anyone can read your shelves through the widget, whatever your profile privacy setting. Goodreads may
  change this at any time. When a shelf comes back without books, MLM logs a warning and skips the list.
- Only the 100 most recently added books of a shelf are read. The RSS feeds had the same cap, so this works as
  before: books stay on the list after newer additions push them out of the newest 100.

Existing `review/list_rss` URLs keep working, don't change them: MLM identifies a list by its URL, so switching an
existing list to the bookshelf page URL creates a new list and searches all its books again.

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
