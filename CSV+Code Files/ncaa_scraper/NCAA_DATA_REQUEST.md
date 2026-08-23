# Draft: data access request to the NCAA

Send to the NCAA's statistics contact. Current options, in order of likely
usefulness:

- **NCAA Media Coordination & Statistics** — the group that runs stats.ncaa.org.
  Contact form at `ncaa.org/about/contact-us`, or the stats staff directory at
  `ncaa.org/sports/2013/11/22/statistics-staff.aspx`.
- **research@ncaa.org** — the NCAA Research department, which handles academic
  data requests and is the better fit for a paper.
- Your Northeastern librarian can also route institutional data requests, and a
  request that arrives through a university library carries more weight.

Send it from your `@northeastern.edu` address. Keep the attachment small or link
the repo rather than attaching.

---

**Subject:** Academic data request — Division I baseball player statistics, 2021–2026

Dear NCAA Research team,

I'm an undergraduate researcher at Northeastern University working on a
publicly-documented statistical model that predicts MLB draft outcomes for
Division I college baseball players. I'm writing to ask whether there is a
supported way to obtain player-level season statistics in bulk for research use.

**What I'm asking for:** season-total batting and pitching statistics for
Division I baseball players, 2021 through 2026 — the same figures published on
each team's season-to-date statistics page at stats.ncaa.org. Roughly 1,800
team-seasons. Any machine-readable format works; a CSV export or a one-time data
dump would be ideal.

**Why I'm asking rather than collecting it from the website:** my project
currently depends on a third-party vendor's export of these statistics, which I
am not permitted to redistribute. That makes the work difficult to reproduce,
which matters for a paper. My preference is to cite the NCAA as the source and
publish the underlying data so the results can be checked. I attempted to collect
the figures from stats.ncaa.org directly at a deliberately slow rate, and the
site's bot protection blocked the requests — which I took as a signal to ask
rather than to work around.

**Intended use:** non-commercial academic research, single-author paper. The NCAA
would be cited as the data source. I'm happy to accept restrictions on
redistribution, share the manuscript before publication, or sign a data use
agreement if that's the normal process.

**Scope, precisely:**

- Sport: baseball, Division I
- Seasons: 2021–2026
- Level: individual player season totals, batting and pitching
- Fields: the counting statistics already public on the team pages — games,
  at-bats, hits, doubles, triples, home runs, runs, RBI, walks, strikeouts, hit
  by pitch, sacrifices, stolen bases, caught stealing; and for pitchers,
  appearances, games started, innings, batters faced, hits, runs, earned runs,
  walks, strikeouts, home runs allowed, wild pitches, balks, saves.
- Also useful, if it exists: a player identifier stable across seasons, and class
  year.

If a bulk export isn't something you provide, I'd be grateful for any pointer —
whether there's a licensing route, an existing research dataset, or a rate and
user-agent I could use to collect it from the site without tripping the bot
protection.

Thank you for your time.

Mateo Biggs
Northeastern University
biggs.m@northeastern.edu
<link to the public repository>

---

## Notes on sending this

- **Lead with the ask, not the scrape.** The paragraph about being blocked is
  there because it is true and because it shows good faith, not as a complaint.
- **The reproducibility argument is your strongest one.** You are asking so you
  can cite them and publish checkable data. That is a reason for them to say yes.
- **Say what you'll accept.** Offering to take redistribution limits or sign a DUA
  removes the easiest reason to decline.
- **Expect slow or no reply**, and don't chase it more than once. Meanwhile the
  fallbacks in `README.md` under "If the block persists" still apply.
