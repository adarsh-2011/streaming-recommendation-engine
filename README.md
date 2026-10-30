# Streaming Recommendation Engine (demo)

A real-time recommendation engine with a live dashboard. A simulated event
stream (views, clicks, add-to-cart, purchases) flows continuously, and a
hybrid recommender updates every user's recommendations as events arrive.

Built with **zero dependencies** — only Node.js built-ins (`node:http`).
No Kafka, no database, no API keys, no setup beyond Node itself.

## How it works

### The stream (simulated)

Every ~900ms (at 1x speed) the server generates one event: a random sample
user performs a weighted action (`view` 55%, `click` 25%, `add-to-cart` 12%,
`purchase` 8%) on a product. Product choice is biased toward things the user
is likely to touch — same-category items (50%), items that co-occur with
their history (30%), or anything at random (20%). The stream can be paused,
resumed, and sped up (0.5x–4x) from the dashboard.

### The hybrid recommender

Recommendations are computed from two signals:

1. **Item-item collaborative filtering.** For every user, every pair of
   distinct items they touched adds
   `weight(action_i) × weight(action_j)` to a co-occurrence matrix
   (`view=1, click=2, add-to-cart=3, purchase=5`). A candidate item's score
   is the sum, over everything the user touched, of
   `co-occurrence[item][candidate] × weight(item)`, plus a small trending
   tie-breaker. Items the user already purchased are never recommended.
   Each recommendation carries a reason: *"Because you viewed X"* or
   *"Because you bought X — users like you also picked this"*.
2. **Trending / popular fallback (cold start).** A user with no history gets
   the globally most-weighted items, labelled *"Trending now"*. The same
   fallback fills gaps when collaborative scores are thin.

The model is recomputed from the event log on every read, and
`lastModelUpdate` advances on every event, so the dashboard always shows
fresh results. State (users + events) persists to `state.json` on disk, so
a restart keeps history. The event log is capped at 5,000 entries.

## Run it

```bash
node server.js
# open http://localhost:3000
```

Custom port: `PORT=4000 node server.js`

## Try it

- Watch the event feed and the "For you" panels update live as the stream runs.
- Use **Simulate event**: pick a user, a product, and an action (try a
  `purchase`) — their recommendations change immediately, and the purchased
  item disappears from their list.
- Click **Add cold-start user**: a brand-new user with no history gets
  sensible popular recommendations, marked with a `cold-start` badge.

## API

| Endpoint | Method | What it does |
|---|---|---|
| `/api/status` | GET | Stream running/paused, speed, uptime |
| `/api/control` | POST | `{ action: "pause" \| "resume", speed?: number }` |
| `/api/events?limit=50` | GET | Recent events, newest first |
| `/api/users` | GET / POST | List users / create a cold-start user |
| `/api/recommendations?userId=u1` | GET | Top 5 recommendations with reasons |
| `/api/trending` | GET | Top trending items |
| `/api/metrics` | GET | Events/sec, total events, last model update |
| `/api/simulate` | POST | `{ userId, productId, action }` — inject an event |
| `/api/catalog` | GET | The 24-product catalog |

## Honest limit

The stream is **simulated**, not a real Kafka cluster — this is a portfolio
demo, not a production pipeline. The recommender logic itself (co-occurrence
scoring, trending fallback, cold-start handling) is real and runs on the
live event log; swapping the simulator for a Kafka consumer would only mean
changing where events come from.

## License

MIT — see `LICENSE`.
