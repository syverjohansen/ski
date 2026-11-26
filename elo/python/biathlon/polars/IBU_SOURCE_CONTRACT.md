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

## Next boundary

The next slice converts these validated official records into the legacy
Nordic Numbers schedule/result schemas, resolves accepted historical athlete
IDs against `IBUId`, and proves individual and relay Elo parity before any
promotion is enabled.
