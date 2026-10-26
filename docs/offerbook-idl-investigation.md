# Offerbook IDL investigation — 2026-10-26

The deployed program emits event versions missing from this repository's IDL.
Allowing null event names does not resolve this analytics gap. The check-only
patch in PR #17 is a draft pending verified schemas and downstream support.

## Direct on-chain evidence

Read through `https://api.mainnet-beta.solana.com` at finalized context slot
461626335:

- Program: `offerbkFMvVfpQhL8ZQ5iromnjct5rz3r52B9ewu3ie`.
- ProgramData: `ANwCXRSc7vwD1WpdFE7LmsWTzjq9Xbp7R7JmATgAFXzA`.
- Last deployment slot in the ProgramData header: 456774686.
- Published IDL metadata: `FS2xtZ5qzmFc5xB24uXVhu67EiWeDegaeAeRk68xNBRF`,
  owned by `ProgM6JCCvbYkfKqJYHePx4xxSUSqJp7rh8Lyv7nk7S`.

The metadata account contains zlib-compressed IDL JSON at byte offset 96.
After decompression and JSON parsing, it is identical to `idl/offerbook.json`:
27 instructions, 22 events, and 69 types. Its canonical JSON SHA-256 is
`8b035db707a44166fdd7bd80d1dcbccfaf4683697f190596ddf75abfcd05cc49`
(Python `json.dumps(idl, sort_keys=True, separators=(',', ':'))`).

Consequently, replacing the repository IDL with the currently published metadata
IDL would not add the missing event definitions. The legacy `anchor idl fetch`
path returned `IDL account not found`.

Two successful transactions from the same program demonstrate the gap:

| Transaction | UTC block time | Event discriminator | Matching Anchor event name |
|---|---|---|---|
| [4xBiCM…bUQ7M](https://explorer.solana.com/tx/4xBiCM4gbFDNRmCxW7gCHkpf73Sk7vtCFpb7utsyy1Nc4iAREKo8Tre89HfoxnCe83cgCsU4yfuQGZPaYK1bUQ7M) | 2026-10-26 10:11:02 | `4d8be585aa05203c` | `OfferCancelledV3` |
| Same transaction | 2026-10-26 10:11:02 | `1a829416649b508b` | `OfferCreatedV3` |
| [66nov9…5Np8](https://explorer.solana.com/tx/66nov9Sorzc9yyUhzpxPs1BzD4sZyYA8sL22rKZSLwRMfJ8JcF24txA55aXuGQVRs2mZEqkjPt3oP4qRbXfy5Np8) | 2026-10-26 10:07:51 | `1ad295733383bc11` | `LoanRepaidV2` |

Each instruction resolves to the Offerbook program and begins with the expected
Anchor CPI event tag `e445a52e51cb9a1d`. The event names above are inferred by
matching `sha256('event:' + name)[:8]` to the following eight bytes. This identifies
the discriminators; it does **not** establish their Borsh field layouts. Running
the current decoder against these payloads returns `unknown_event`.

Other sampled events (`EscrowWithdraw`, `ProtocolFee`, `EscrowDeposit`) decode
successfully using the existing IDL. This rules out a universal event framing
failure in these samples.

These live samples demonstrate IDL coverage drift. They do not establish the
discriminator breakdown of all 86,026 historical unknown records from failed run
40268722645; that requires querying the production decoded table.

## Jupiter source checks

Jupiter's public Offerbook frontend and the searched public `jup-ag` GitHub code
did not expose a newer Offerbook IDL. The public API and builder hosts returned
404 for `/openapi.json` and `/api/v1/idl`. No newer authoritative field schema
was obtained. This is a search result, not a claim that no updated IDL exists.

## Remaining repair

1. Obtain Jupiter's current Offerbook IDL or verified source definitions for the
   missing event and instruction versions. Do not reinterpret V3/V2 payloads as
   older structs merely because their names are similar.
2. Validate the supplied schemas against actual successful transaction payloads,
   requiring complete consumption and checking identities, amounts, and times.
3. Extend `int_offerbook_offer_events` and loan lifecycle models. They explicitly
   select offer versions through V2 and loan versions through V1; an IDL update
   alone would still leave newer events out of analytics.
4. Inspect the production unknown-discriminator distribution and replay affected
   signatures. The event decoder fingerprints the IDL, so an IDL change schedules
   rescanning; account for the historical backlog and workflow batch limits.
5. Keep a separate IDL-coverage gate when correcting nullable-field checks, so
   supported quarantine records do not silently certify complete analytics.
6. Rebuild and validate downstream marts before claiming production recovery.
