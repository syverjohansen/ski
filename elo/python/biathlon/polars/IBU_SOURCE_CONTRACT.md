# Official IBU source contract

Nordic Numbers treats the International Biathlon Union Datacenter as the
authoritative Biathlon schedule, result, identity, and start-list-report
boundary. FirstSkiSport is not required by this contract.

`ibu_source_contract.py` is intentionally read-only. It queries the same public
JSON service used by the official Datacenter web client, validates stable IBU
identities and supported race shapes, and writes raw evidence plus a report to
an isolated directory. It does not write to `excel365`, replay Elo, or publish.

## Supported World Cup shapes

- Individual (`IN`)
- Short Individual (`SI`)
- Sprint (`SP`)
- Pursuit (`PU`)
- Mass Start (`MS`)
- Men's and women's Relay (`RL`)
- Mixed Relay (`RL` with mixed category)
- Single Mixed Relay (`SR`)

Unknown categories or disciplines fail closed. This matters because Short
Individual exists in current official data but was not explicit in the legacy
FirstSkiSport mapping.

## Identity rules

- `SeasonId`, `EventId`, and `RaceId` are the event hierarchy.
- `IBUId` is the canonical athlete identity.
- Relay identity preserves team, athlete, and leg. Repeated legs by the same
  athlete in Single Mixed Relay are expected and are not deduplicated.
- Official start-list evidence must be exactly one `Start List` PDF hosted at
  `ibu.blob.core.windows.net`.

## Authentication boundary

The Datacenter's public browser client sends an API key published in its own
application bundle. The command uses `IBU_API_KEY` when supplied; otherwise it
discovers the current published value from the official client. The key is
never written to raw evidence or reports. A missing or changed public-client
contract fails visibly.

## Historical smoke

The fixture-backed contract uses Oestersund World Cup 1 from season 2025/26 and
covers an individual Sprint, a women's Relay, and a Single Mixed Relay. To run
the same read-only capture live:

```bash
python ibu_source_contract.py \
  --season-id 2526 \
  --event-id BT2526SWRLCP01 \
  --race-id BT2526SWRLCP01SWSP \
  --race-id BT2526SWRLCP01SWRL \
  --race-id BT2526SWRLCP01MXSR \
  --output-dir /tmp/biathlon-ibu-contract
```

Some local Python framework installations do not have a configured CA bundle.
Set `SSL_CERT_FILE` to the system or `certifi` CA bundle in that environment;
TLS verification must not be disabled.

## Shadow compatibility adapter

`biathlon_official_adapter.py` converts a validated evidence bundle into
candidate Nordic Numbers schedule and result CSVs. It resolves official
`IBUId` values against accepted local athlete IDs with deterministic normalized
names. Ambiguous or missing identities fail closed. A small reviewed alias file
handles spelling or surname changes that cannot be inferred safely; the
official `CISBios` endpoint is available to corroborate those reviews with
gender and birthdate evidence.

The adapter deliberately preserves the legacy compatibility semantics needed
for historical parity:

- DNS/non-ranked entries do not become Elo observations.
- Relay members inherit their team's official finish rank.
- Single Mixed Relay's repeated second leg for each athlete is collapsed to
  the one observation used by the accepted histories.
- Result race numbering preserves the historical men's/mixed and women's
  sequences, while schedule numbering preserves its separate mixed sequence.

Run it only against an isolated evidence bundle and an output directory outside
the accepted `excel365` tree:

```bash
python biathlon_official_adapter.py \
  --evidence-dir /tmp/biathlon-ibu-contract \
  --accepted-dir excel365 \
  --aliases ibu_identity_aliases.csv \
  --output-dir /tmp/biathlon-adapter-candidate
```

Outputs are `schedule_candidate.csv`, separate men's and women's result
candidates, `identity_map.csv`, and `parity_report.json`. The report records
that accepted data was not mutated and publication is disabled. Promotion and
runtime integration remain separate future approvals.

The Oestersund 2025/26 historical shadow proves exact core parity for all ten
competitions: 10/10 schedule rows, 410/410 men's observations, and 417/417
women's observations, with no unresolved identities.

## Next boundary

The next slice replays individual and relay Elo from the parity-proven official
candidate rows and compares the resulting rating artifacts with accepted
outputs. It must remain shadow-only until the Elo comparison also passes.
