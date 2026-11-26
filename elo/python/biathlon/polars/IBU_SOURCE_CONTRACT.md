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

`biathlon_elo_parity.py` implements the next boundary without replacing the
existing rating mathematics. It splices the official candidate event into
in-memory copies of both accepted history layouts, invokes `elo.py` unchanged,
and compares every generated `Pelo` and `Elo` value with the accepted direct
rating artifacts. The individual policy verifies overall, Individual, Sprint,
Pursuit, and Mass Start streams; the relay policy verifies the combined stream
that applies the existing quarter-weight Relay/Mixed Relay and half-weight
Single Mixed Relay updates.

Install the pinned shadow dependencies, then run:

```bash
python -m pip install -r requirements-elo-parity.txt
python biathlon_elo_parity.py \
  --candidate-dir /tmp/biathlon-adapter-candidate \
  --output-dir /tmp/biathlon-elo-parity
```

The Oestersund shadow compared 499,283 accepted output rows across twelve
sex/policy/discipline streams. Every row and rating matched within `1e-9`; the
largest floating-point difference was `4.55e-13`. Accepted data was not mutated
and publication remained disabled.

`biathlon_relay_team_parity.py` proves the downstream team boundary. It takes
the publication-disabled relay athlete ratings above, verifies and injects
them into in-memory copies of the accepted chronos, invokes the existing relay
aggregation functions unchanged, and compares the complete men's, women's,
mixed, and single-mixed team artifacts:

```bash
python biathlon_relay_team_parity.py \
  --event-ratings-dir /tmp/biathlon-elo-parity/event_ratings \
  --output-dir /tmp/biathlon-relay-team-parity
```

The same Oestersund shadow compared all 10,258 aggregate rows: 4,666 men's
relay, 3,409 women's relay, 1,346 mixed relay, and 837 single mixed relay rows.
Every key, team member list, scalar field, and rating matched within `1e-9`;
the largest floating-point difference was `1.82e-12`. The harness writes only
event-specific shadow candidates and its report outside the accepted tree.
Accepted data was not mutated and publication remained disabled.

## Guarded runtime shadow

`biathlon_runtime_shadow.py` connects all four proven boundaries into one
sequential, fail-closed command. Every run uses a new directory, records the
exact checked-out commit, fingerprints all accepted Biathlon files before and
after execution, retains every stage report, and stops at the first failed
gate. The runner contains no promotion or publication operation:

```bash
python biathlon_runtime_shadow.py \
  --season-id 2526 \
  --event-id BT2526SWRLCP01 \
  --race-id BT2526SWRLCP01SWRL \
  --race-id BT2526SWRLCP01SMRL \
  --race-id BT2526SWRLCP01MXSR \
  --race-id BT2526SWRLCP01MXRL \
  --race-id BT2526SWRLCP01SWIN \
  --race-id BT2526SWRLCP01SMIN \
  --race-id BT2526SWRLCP01SWSP \
  --race-id BT2526SWRLCP01SMSP \
  --race-id BT2526SWRLCP01SWPU \
  --race-id BT2526SWRLCP01SMPU \
  --output-root /tmp/biathlon-runtime-shadow \
  --run-id ostersund-2526-shadow
```

The historical Oestersund operational proof completed all four stages in one
run: official evidence capture, compatibility adaptation, twelve-stream athlete
Elo replay, and four-format relay-team aggregation. The accepted roots contained
90 files and 773,041,694 bytes; their before/after SHA-256 fingerprints were
identical. Promotion and publication remained disabled.

The next slice can package this command as a retained-artifact, manually
dispatched GitHub Actions shadow. Promotion remains a separate approval after
the hosted operational boundary is proven.
