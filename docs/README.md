# Chat image catalog

Everything in this folder is served as static files alongside `index.html` —
nothing here ever goes through Supabase or the WebSocket server. Sending a
picked image just sends the plain-text token `{image:<id>}` through the
normal chat pipeline; every client resolves that id against its own local
copy of `image.json` and displays the matching file straight from this
folder.

## Format

`image.json` is a flat object mapping an image id to its filename in this
same folder:

```json
{
  "gg": "gg.png",
  "victory_dance": "victory_dance.gif",
  "sad_pest": "sad_pest.png"
}
```

- **id** — used in the `{image:id}` token. Keep it to letters, numbers,
  underscores, and hyphens (the client's token parser only recognizes
  `[a-zA-Z0-9_-]`, 1–64 characters).
- **file** — the filename of the actual image, sitting in this same
  `chat-images/` folder. Any browser-renderable format works (png, jpg,
  gif, webp, svg).

## Replacing the demo catalog

`gg.svg` and `wave.svg` are placeholder demo images just so the picker has
something to show out of the box. Swap in your real catalog by:

1. Dropping your image files into this folder.
2. Replacing `image.json` with your own id → filename map.

No code changes needed — the picker, the send button on every chat surface,
and the `{image:id}` → `<img>` rendering all read `image.json` directly.
