# 08d — Megabridge msgconv Completeness Audit

Audit target: `/Users/danielagoston/utmshared/Projects/git/generalclaude/mautrix/_reference/googlechat-megabridge/pkg/msgconv/` (11 files, ~1,250 LoC incl. tests).
Assessed against: `docs/research/03-python-bridge-features.md` §4 (formatter) and `docs/research/07-gap-analysis.md` rows "Annotations → HTML" / "HTML formatting → annotations" (both rated L-effort there).

All file:line citations below are relative to `/Users/danielagoston/utmshared/Projects/git/generalclaude/mautrix/_reference/googlechat-megabridge/` unless prefixed otherwise. Findings marked **[verified]** were confirmed by running the package's tests plus ad-hoc probe tests under Go 1.26.3 (`go test`/`go vet` both clean; probe files removed afterwards, reference tree left clean).

## Package layout

| File | LoC | Role |
|---|---|---|
| `pkg/msgconv/msgconv.go` | 52 | `MessageConverter` struct, MXID→GC-UID resolver wiring |
| `pkg/msgconv/from-gchat.go` | 143 | `ToMatrix`: text part + attachment parts, thread/reply mapping |
| `pkg/msgconv/from-matrix.go` | 18 | `ToGChat`: thin wrapper over `matrixfmt.Parse` |
| `pkg/msgconv/gchatfmt/convert.go` | 230 | GC annotations → Matrix HTML (recursive interval renderer + normalizer) |
| `pkg/msgconv/gchatfmt/utils.go` | 28 | Annotation constructors (used by both directions + tests) |
| `pkg/msgconv/gchatfmt/convert_test.go` | 60 | 3 table-driven cases |
| `pkg/msgconv/matrixfmt/html.go` | 471 | Matrix HTML → `EntityString` (UTF-16 string + entity ranges); ported from mautrix-signal |
| `pkg/msgconv/matrixfmt/convert.go` | 29 | Entry point: `MessageEventContent` → (text, `[]*proto.Annotation`) |
| `pkg/msgconv/matrixfmt/tags.go` | 67 | `BodyRangeValue` (Style, Mention) → proto metadata |
| `pkg/msgconv/matrixfmt/tree.go` | 107 | `BodyRange` arithmetic + `LinkedRangeTree` (**dead code**, see §6) |
| `pkg/msgconv/matrixfmt/convert_test.go` | 46 | 2 table-driven cases |

UTF-16 primitive: `UTF16String` (`[]uint16` via `utf16.Encode([]rune(s))`) lives in `pkg/gchatmeow/channel.go:62-70` — shared with the chunk parser. `matrixfmt` provenance: mautrix-signal's msgconv (the `tree.go:81` doc comment still says "parse a list of Signal body ranges").

---

## 1. GC → Matrix (`gchatfmt` + `from-gchat.go`)

### 1.1 Annotation-type coverage (formatting)

Dispatch is in `annotationsToMatrix`, `pkg/msgconv/gchatfmt/convert.go:158-219`:

| Annotation | Python behavior (03 §4.1) | Megabridge | Cite |
|---|---|---|---|
| BOLD → `<strong>` | ✔ | ✔ | convert.go:162-163 |
| ITALIC → `<em>` | ✔ | ✔ | convert.go:164-165 |
| UNDERLINE → `<u>` | ✔ | ✔ | convert.go:166-167 |
| STRIKE → `<del>` | ✔ | ✔ | convert.go:168-169 |
| MONOSPACE → `<code>` | ✔ | ✔ | convert.go:170-171 |
| MONOSPACE_BLOCK → `<pre><code>` | ✔ | ✔ | convert.go:172-173 |
| FONT_COLOR → `<font color>` | ✔ `(rgb+2^31)&0xFFFFFF` | ✔ same transform; `FontColor` is `uint32` (`proto/googlechat.pb.go:9019`) so mod-2^32 arithmetic gives identical low-24-bit result **[verified: 0xFFFF0000 → `#ff0000`]** | convert.go:174-177 |
| BULLETED_LIST → `<ul>` / _ITEM → `<li>` | ✔ | ✔ **[verified]**; Python's extra tie-break ordering list wrapper before item at equal offsets (`_annotation_key`, 03:183) is only approximated by length-desc sort | convert.go:178-181, 52-57 |
| HIDDEN → drop text | ✔ | ✔ | convert.go:160-161 |
| SOURCE_CODE, CLIENT_HIDDEN | plain text | plain text (default → `skipEntity`) | convert.go:182-183 |
| `url_metadata` → `<a href>` | ✔ | ✔ but **unescaped attribute — bug B1** | convert.go:185-186 |
| `user_mention_metadata` MENTION | ✔ pill to user MXID, displayname substitution | ◐ **deviates — bug B2** | convert.go:187-216 |
| MENTION_ALL → `@room` | ✔ literal | ✔ literal, but no `m.mentions.room` (gap G4) | convert.go:188-189 |

### 1.2 UTF-16 code-unit indexing — correct **[verified]**

`Parse` re-encodes the body once (`gchatmeow.NewUTF16String(msg.TextBody)`, convert.go:32) and every slice/length operation in `annotationsToMatrix` operates on `[]uint16` (convert.go:106-149). Probe with astral chars: body `"🎆🎆 bold italic"` with BOLD(5,10) + ITALIC(10,6) rendered `🎆🎆 <strong>bold <em>itali</em></strong><em>c</em>` — offsets and the overlap split both correct in code-unit space. The shipped test corpus also has one astral case (convert_test.go:39-46). This satisfies gap-analysis risk #5 (07:350) mechanically.

### 1.3 Overlapping-span normalization — present and working **[verified]**

`normalizeAnnotations` (convert.go:47-97) is a faithful port of Python `_normalize_annotations` (03:183): sort by (start asc, length desc), then split any annotation crossing the current one's end and re-insert the tail. Probe BOLD(0,4)+ITALIC(2,4) over `"abcdef"` → `<strong>ab<em>cd</em></strong><em>ef</em>`; nested BOLD(0,6)+ITALIC(2,2) → `<strong>ab<em>cd</em>ef</strong>`. Caveats:

- It **mutates the caller's annotation slice** in place (sort + `annotation.Length = end - annotation.StartIndex`, convert.go:80); the same `msg.Annotations` slice is re-iterated for attachments afterwards (from-gchat.go:34). Harmless today, footgun tomorrow.
- Out-of-bounds annotations abort the whole conversion with an error (convert.go:134-135); `Parse` then keeps the plain body and only `log.Printf`s (convert.go:35) — formatting silently lost for the whole message.

### 1.4 chip_render_type filtering — half present

- Formatting path: correct — non-`DO_NOT_RENDER` annotations are skipped by the renderer (convert.go:128-131), matching Python (03:184).
- Attachment path: **missing — bug B3 (serious)**. `ToMatrix` feeds *every* annotation to `gcAnnotationToMatrix` (from-gchat.go:34-43) and that function treats *any* `url_metadata` as a downloadable attachment (from-gchat.go:90-98). Python only processed `chip_render_type == RENDER` annotations as attachments (`portal.py:1465 _preprocess_annotations`, 03:45). Consequence: a plain inline hyperlink (URL annotation, DO_NOT_RENDER) is **downloaded via bare `http.Get`** (`pkg/gchatmeow/client.go:276` — no context, no timeout, arbitrary external host = SSRF surface) **and re-uploaded to Matrix as an `m.image`**, in addition to being rendered as a link. Every message containing a link is affected.

### 1.5 Drive / Meet / YouTube — absent

No reference to `DriveMetadata`, `YoutubeMetadata`, or `VideoCallMetadata` anywhere in `pkg/msgconv/` or `pkg/connector/` (grep-verified). Those annotations hit the `uploadMeta == nil && urlMeta == nil` branch and are silently dropped (from-gchat.go:99-100). Python appended YouTube URLs and emitted Beeper `com.beeper.linkpreviews` for url/drive/youtube metadata (03:49-50, 03:191). Gap row "Drive / Meet / YouTube annotations" (07:61): 0% done. URL previews (07 / 03:191): 0%.

### 1.6 Incoming attachments (upload_metadata) — present, rough

`gcAnnotationToMatrix` (from-gchat.go:65-143):
- ✔ `FIFE_URL` + `sz=w10000-h10000` for `image/*`, `DOWNLOAD_URL` otherwise, through `https://chat.google.com/api/get_attachment_url` (from-gchat.go:74-88) — matches Python (03:45).
- ✔ Authed download with manual redirect following for `*.google.com` (client.go:259-273); ✔ streaming re-upload via `intent.UploadMediaStream` (from-gchat.go:125-134, encryption handled by intent); ✔ filename fallback from Content-Disposition/mime (from-gchat.go:107-116); ✔ `MergeCaption()` (from-gchat.go:60).
- ✘ **MsgType hardcoded `MsgImage`** for all attachments — video/audio/PDF arrive as `m.image` (from-gchat.go:123).
- ✘ No homeserver media-size cap (Python size-capped, 03:45); no image dimensions/thumbnails.

### 1.7 Mentions (incoming) — deviates from Python — bug B2

Python resolved gcid → bridge user or ghost MXID and pilled `matrix.to/#/{user MXID}` with the target's current displayname (03:188). Megabridge (convert.go:191-215) instead:
1. looks up DM portals with the gcid and pills **the DM room's MXID** (`dmPortals[0].MXID.URI()`, convert.go:198-202) — a room pill, not a user pill; wrong target, and only works if the recipient happens to share a DM portal with the mentioned user;
2. else, if the gcid is a *logged-in bridge user*, pills that user's MXID (convert.go:204-211) — correct but in practice only covers self-mentions;
3. else falls back to plain text — **ghost users are never pilled** (no `FormatGhostMXID`/ghost-intent use).

Additionally, neither user mentions nor `@room` populate `content.Mentions` (`m.mentions`), so spec-compliant clients will not ping (gap G4). `gchatfmt.Parse` builds only `Body`/`FormattedBody` (convert.go:26-44).

### 1.8 HTML escaping — bug B1 **[verified]**

Text segments are escaped (`escape()`, convert.go:99-101,140,228) but **attribute values are not**: `fmt.Fprintf("<a href='%s'>", url)` (convert.go:186) and the same pattern for mention hrefs (convert.go:198-210). Probe with URL `https://example.com/?a=1&b='<x>` produced `<a href='https://example.com/?a=1&b='<x>'>link</a>` — attacker-controlled GC content can break out of the attribute and inject markup into the Matrix event. Client sanitizers blunt the worst outcomes, but this is below adoption grade as-is.

---

## 2. Matrix → GC (`matrixfmt` + `from-matrix.go`)

Architecture: the `EntityString` approach lifted from mautrix-signal — a UTF-16 string paired with entity ranges that survive every split/trim/join/append (html.go:18-190). This is a *different* mechanism than the "mention placeholder-locator trick" named in 07:39 (the meta-bridge pattern), but it solves the same problem — offsets are computed natively in UTF-16 code units at composition time — and is architecturally sounder. Probe `"🎆🎆 <b>bold</b>"` → BOLD at start=5 code units **[verified]**.

### 2.1 Coverage matrix

| Matrix HTML | Python (03 §4.2) | Megabridge | Cite |
|---|---|---|---|
| `b/strong` → BOLD, `i/em` → ITALIC, `s/del/strike` → STRIKE | ✔ | ✔ **[verified]** | html.go:311-316 |
| `u/ins` → UNDERLINE | ✔ | ✘ **dropped** — returns unformatted (`return str`) even though `StyleUnderline` exists (tags.go:49) | html.go:317-318 **[verified]** |
| `tt/code` → MONOSPACE | ✔ | ✔ | html.go:319-320 |
| `pre` → MONOSPACE_BLOCK | ✔ | ◐ emits inline **MONOSPACE (5) instead of MONOSPACE_BLOCK (7)** | html.go:392-399 **[verified]** |
| font color → FONT_COLOR (`(hex\|0x7F000000)-2^31`, 03:200) | ✔ | ✘ `span/font` recurses without reading `color`/`data-mx-color`; `Style.Proto()` cannot even carry a color value | html.go:325-327, tags.go:53-59 **[verified]** |
| `a` (plain link) → URL annotation | ✔ | ✘ **no URL annotation** — renders `text (url)` plain text; zero `UrlMetadata` references in matrixfmt (grep-verified) | html.go:344-368 **[verified]** |
| `a` (user pill) → USER_MENTION | ✔ ghosts only, TODOs for real users (03:201) | ✔ **better than Python**: ghosts *and* real Matrix users via `GetUIDFromMXID` → `ParseGhostMXID`/`FindPreferredLogin` (msgconv.go:32-45); honors `m.mentions` allow-list (html.go:353-356); emits `@Name` + MENTION annotation **[verified: start/len correct]** | html.go:350-364, tags.go:23-32 |
| `@room` → `@all` + MENTION_ALL (03:205) | ✔ | ✘ **absent** — no MENTION_ALL/`@room` handling anywhere in matrixfmt (grep-verified) | — |
| plain-text body containing `@room` (03:195) | ✔ | ✘ plain bodies bypass parsing entirely | matrixfmt/convert.go:12-14 |
| `ul` → BULLETED_LIST(_ITEM) annotations (03:203) | ✔ | ◐ **text-only** `* ` prefixes, no list annotations; child formatting offsets stay correct **[verified]** | html.go:266-306 |
| `ol` | text numbering fallback | ✔ same (start attr, digit-aligned indent) **[verified]** | html.go:271-293 |
| `h1-h6` → `#`-prefixed bold (03:204) | ✔ | ✔ | html.go:329-333 |
| `blockquote` → `> ` prefixes | ✔ | ✔ **[verified]** | html.go:335-342 |
| `br`, `hr`, `p`, block spacing | ✔ | ✔ (`br`→`\n`, `hr`→`---`, block tags newline-wrapped) | html.go:380-391, 428-454 |
| spoilers flattened, room pills dropped (03:202) | ✔ | ✔ (default recursion; non-`@` matrix.to links fall through) | html.go:350-368 |

### 2.2 accept_format_annotations — set ✔

Not in msgconv itself but at both connector call sites: `proto.MessageInfo{AcceptFormatAnnotations: true}` for new messages (`pkg/connector/handlematrix.go:33-35`) and edits (handlematrix.go:139-141). Matches Python `maugclib/client.py:453,467` (03:207).

### 2.3 Style enum ↔ proto alignment — correct

`Style` iota values (tags.go:40-51) line up exactly with `FormatMetadata_FormatType` 0-9 (BOLD=1 … FONT_COLOR=9, checked against `proto/googlechat.pb.go:2454-2466`); `Style.Proto()` casts directly (tags.go:53-59). Fragile-but-correct; `StyleSourceCode/Hidden/MonospaceBlock/Underline/FontColor` are never produced by the parser.

### 2.4 Connector-side integration bug — B4

`HandleMatrixMessage` (handlematrix.go:37-78): for media it builds `annotations = [UPLOAD_METADATA]`, then unconditionally runs the caption through `ToGChat` and **overwrites** the slice whenever the caption has any formatting: `if entities != nil { … annotations = entities }` (handlematrix.go:75-78). A media message with a formatted caption loses its attachment (plain captions survive because `entities == nil`).

---

## 3. Tests

### 3.1 Shipped corpus — enumerated

`pkg/msgconv/gchatfmt/convert_test.go` — `TestParse`, 3 cases:
1. `plain` (:21-25) — no annotations, body passthrough.
2. `bold italic strike underline` (:27-37) — four non-overlapping single-char spans.
3. `emoji` (:39-46) — one astral char (`🎆`) before a BOLD span; the only UTF-16 indexing assertion in the suite.

`pkg/msgconv/matrixfmt/convert_test.go` — `TestParse`, 2 cases:
1. `Plain` (:23).
2. `Bold` (:24-28) — single `<strong>` span.

### 3.2 Execution **[verified]**

`go test -count=1 ./pkg/msgconv/...` under Go 1.26.3 linux/arm64: **all 5 cases pass**; `go vet` clean. Ad-hoc probes (overlap split, nesting, font color, lists, astral-offset overlap, attribute escaping, pills, blockquote, `<pre>`, `<u>`, `<font color>`) all executed without panics; results embedded in §1/§2.

### 3.3 Corpus quality — poor

Table-driven and easily extensible (a genuine asset), but coverage is minimal: **no** overlapping-span case, **no** nested formatting, **no** mention/pill case, **no** list/link/color/blockquote case, **no** round-trip test, **no** `normalizeAnnotations` unit test, and only one astral-char case in one direction. 07:350 calls for an "exhaustive round-trip test corpus" — the corpus does not exist yet, only the harness does. Notably, my probes show the *untested* core (normalization, UTF-16 splitting) actually works; the confirmed bugs all live in glue paths the tests never touch.

---

## 4. Media conversion

Present in **both** directions, living partly outside msgconv:

- **GC → Matrix**: `gcAnnotationToMatrix` (from-gchat.go:65-143) + `Client.DownloadAttachment` (pkg/gchatmeow/client.go:259-277). Works for images/files via get_attachment_url; streaming re-upload. Defects: msgtype hardcode, chip-filter bug B3, bare `http.Get` for external URLs (§1.4, §1.6).
- **Matrix → GC**: connector `HandleMatrixMessage` media branch (handlematrix.go:37-55) — `Bot.DownloadMedia` (handles encrypted files) → `Client.UploadFile` implementing the full resumable protocol (`x-goog-upload-*` start → PUT `upload, finalize` → base64 → `UploadMetadata` proto, `pkg/gchatmeow/api.go:191-235`) → `Annotation{UPLOAD_METADATA, RENDER}` (handlematrix.go:46-54). Matches the Python flow (03:23) — but per 07:40 **the upload endpoint has returned HTTP 500 since ~Feb 2026 (upstream issue #114)**; this code is unproven against current Google servers. Fully in-memory (`[]byte`), no size guard. Caption-overwrite bug B4 applies.
- No sticker/voice/gif specialization; no video/audio msgtype mapping in either direction; captions GC→Matrix handled via `MergeCaption` (better than Python, which lost captions — 03:23).

---

## 5. Completeness score

Weighted against the full formatting requirement (03 §4 + 07 rows 39/59/61):

| Area | Done | Notes |
|---|---|---|
| gchatfmt (annotations→HTML) | ~75% | all format types + normalization + UTF-16 correct; mention pills wrong-target (B2), attribute escaping (B1), no `m.mentions` |
| matrixfmt (HTML→annotations) | ~60% | core engine + offsets + pills solid; underline/color/URL-annotations/MONOSPACE_BLOCK/@room/list-annotations missing |
| Media both directions | ~70% | mechanics present both ways; B3/B4, no msgtype mapping, upload-endpoint 500 risk |
| Drive/Meet/YouTube + URL previews | 0% | silently dropped |
| Test corpus | ~15% | harness exists, corpus does not |

**Overall: ~60% of the full formatting requirement.** One gap-analysis directive already satisfied: the Python `if annotations:` always-truthy bug (03:190, 07:59) was *not* replicated — gchatfmt gates on `len(msg.Annotations) > 0` (convert.go:31).

## 6. Adoption-grade verdict

**Adopt-and-fix: yes. Adopt-as-is: no.**

For: the two hard problems the gap analysis ranks as the top formatting risk (07:350) — UTF-16 code-unit indexing and overlapping-span normalization — are implemented and demonstrably correct, including astral-plane cases, in both directions. The `EntityString` architecture (proven in mautrix-signal) is a better foundation than re-porting Python's surrogate-padding dance, and the table-driven harness is ready to receive a real corpus. Outgoing mention resolution is already *ahead* of Python (real-user mentions work; Python had TODOs).

Against — must fix before shipping:
- **B1** attribute injection via unescaped hrefs (gchatfmt/convert.go:186, 198-210) — security-relevant.
- **B2** incoming mentions pill DM rooms instead of user/ghost MXIDs; no `m.mentions` (convert.go:191-216).
- **B3** missing chip_render_type filter on attachments → every inline link is downloaded (bare `http.Get`, SSRF surface) and re-posted as `m.image` (from-gchat.go:34-43, 90-101; client.go:276).
- **B4** formatted caption wipes the UPLOAD_METADATA annotation (handlematrix.go:75-78).
- Missing outgoing: underline, font color, URL annotations, MONOSPACE_BLOCK, `@room`→MENTION_ALL, list annotations, plain-text `@room` path (§2.1).
- Missing incoming: Drive/Meet/YouTube, URL previews, msgtype-by-mime, media size caps (§1.5-1.6).
- Hygiene: std-lib `log.Printf` instead of zerolog (gchatfmt/convert.go:35, from-gchat.go:37); dead code (`LinkedRangeTree`, tree.go:79-107 — unused signal leftover); `normalizeAnnotations` mutates caller state (convert.go:52-97); package-global mutable `DebugLog` (html.go:23).

Effort to parity: the four bugs are one-function fixes; the missing matrixfmt types are new `case` arms plus a color/URL-capable `BodyRangeValue`; roughly 2-4 focused days plus corpus writing — far below the L-rated from-scratch estimate for both packages (07:39,59). For the fork-vs-greenfield decision, this package is one of the strongest arguments *for* forking: the risky 40% is done and correct; what remains is enumerable feature-arm work.
