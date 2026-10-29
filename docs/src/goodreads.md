# Goodreads Import

Goodreads lists/bookshelves can be used as a source for autograbbing books.
Give MLM the URL of the bookshelf page, e.g. `https://www.goodreads.com/review/list/12345678-name?shelf=to-read`.
The URL has to include a `shelf`, MLM can't import all books of a user.

Goodreads stopped serving the bookshelf RSS feeds in October 2026. MLM now reads the shelf through Goodreads' public
bookshelf widget instead. This only sees public data, so it can't do everything the RSS feeds could:
- Private shelves most likely can't be imported any more. The RSS `key` used to give MLM access to a private profile,
  but the widget doesn't use it. Making the shelf public makes it work again, but also makes your reading list
  visible to everyone. When a shelf comes back without books, MLM logs a warning and skips the list.
- Only the 100 most recently added books of the shelf are read. This isn't new: the RSS feeds were also limited to
  the 100 newest books, and MLM only ever read one page of them. Books MLM has seen stay on the list after they drop
  out of the newest 100.

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
