# Selected Torrents

This page shows the torrents that MLM has picked to download, but which have not finished yet. They are waiting in line to be sent to your torrent client, or are being downloaded now.

## The summary

Above the table you can see your MyAnonamouse standing:

- **Buffer** – how much you can download before your ratio becomes a problem,
- **Unsats** – how many unsatisfied torrents you have, out of the maximum allowed,
- **Wedges** and **Bonus** – your wedges and bonus points,
- **Queued Torrents** and **Downloading Torrents** – how many selected torrents are waiting and how many are in progress.

## The table

Choose extra columns with **Columns**. Besides the title and type, you see the **Cost** (how the torrent will be paid for) and **Required Unsats**. The latter is the number of free unsat slots MLM wants to keep before it downloads this torrent.

A torrent picked by a `ratio` (`all`) or `try_wedge` block shows that cost even when it's freeleech, since it's downloaded either way. One picked as freeleech or VIP by a `free` or `wedge` block is only downloaded while it's still free; when it isn't, MLM drops it, and a later search can select it again as its block allows.

## Actions

Tick one or more torrents and these buttons appear:

- **unselect for download** – cancels the selection.
- **set required unsats to** – changes the number in the box next to it for the ticked torrents. A higher number makes MLM wait until more slots are free.
