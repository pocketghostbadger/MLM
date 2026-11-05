# Goodreads Import

Goodreads lists/bookshelves can be used as a source for autograbbing books.
Give MLM the URL of the bookshelf page, e.g. `https://www.goodreads.com/review/list/12345678-name?shelf=to-read`.
The URL has to name a single `shelf`. MLM can't import all of a user's books, so the "All" shelf (`#ALL#`) doesn't
work either.

Goodreads stopped serving the bookshelf RSS feeds in October 2026. MLM now reads a user's shelf through Goodreads'
public bookshelf widget instead. Goodreads no longer serves the RSS feeds of group bookshelves either, so MLM rejects
group bookshelf URLs (`/group/bookshelf/...`) with an error and those lists can't be imported anymore. The widget
differs from the RSS feeds in a few ways:
- The widget doesn't use the RSS `key` and needs no sign-in. In testing (October 2026) it still returned the shelves
  of profiles set to "Goodreads members" or "just my friends", so private profiles keep working. Be aware that this
  also means anyone can read your shelves through the widget, whatever your profile privacy setting. Goodreads may
  change this at any time. When a shelf comes back without books, MLM logs a warning and skips the list.
- Only the 100 most recently added books of a shelf are read. As far as we know the RSS feeds had the same limit.
  Books that newer additions push out of the newest 100 stay on the list in MLM, but by default aren't searched while
  they're outside the newest 100. `search_stored_books` changes that, see
  [Books outside the newest 100](#books-outside-the-newest-100).

Keep existing `review/list_rss` URLs as they are. MLM identifies a list by the user part of its URL and the shelf, so
switching to the bookshelf page URL (`12345678-name` instead of `12345678`) creates a new list: MLM goes through all
its books again and searches MAM for the ones it doesn't have yet. Adding a shelf to a URL that had none, or replacing
`#ALL#` with a shelf, does the same.

Example configuration:
```toml
[[goodreads_list]]
url = "https://www.goodreads.com/review/list/...?shelf=to-read" # A Goodreads bookshelf page, or an old list_rss feed URL
search_stored_books = false # true also searches the books outside the newest 100, see below

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

## Books outside the newest 100

With `search_stored_books = true` in a `[[goodreads_list]]`, each import also goes through the list's other stored
books: the ones MLM read from the shelf on an earlier import that weren't among the books it just read. It searches
for those still missing a format the same way as for the books in the newest 100: it checks the library first,
searches only while the import has free unsatisfied slots, and waits longer after each search that finds nothing (2
hours, doubling up to a day). Once the import's slots are used up, it stops going through them. If searching for one,
or selecting its torrent, fails, that book waits as if nothing had been found, unless MyAnonamouse refused the request
or MLM has paused its requests, and MLM stops going through them until the next import. If MLM can't read the shelf,
the shelf comes back without books, or searching for one of the newest 100 fails, it searches none of them in that
import. The setting is off by default.

This means more searches on MyAnonamouse, each usually one request (up to three for books with many results). The
wait is only kept in memory, and a search that found a torrent isn't followed by one, even when MLM didn't take the
torrent (unless selecting it failed). With hourly imports, a book that isn't found is searched about five times in the
first 30 hours after the setting is turned on or MLM restarts, then once a day (imports run every 135 minutes unless
you set `import_interval`, or the list's `search_interval`). Torrents it finds are grabbed through the list's `grab`
blocks like any others.

When the shelf has fewer than 100 books, MLM reads all of it, so the stored books that aren't in it have left the shelf
and none of them are searched. Otherwise MLM can't tell whether a book outside the newest 100 is still on the shelf:
books you remove from the shelf, or move to another shelf, stay on the list in MLM and are searched and grabbed too.
Press **mark done** on the list page for the books you don't want anymore. Books MLM never read from the shelf aren't
on the list, so they aren't searched.
