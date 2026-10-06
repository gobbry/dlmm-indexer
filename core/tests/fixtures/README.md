# Decoder fixtures

Saved on 2026-10-03 from `api.mainnet-beta.solana.com` with `getTransaction`,
`encoding: json`, `maxSupportedTransactionVersion: 1`, `commitment: finalized`. Each file holds
the `result` object only. Do not edit the files. If you need new copies, refetch them with the
same parameters.

| file | signature | slot | version | failed | `Swap` events (independent count) |
|---|---|---|---|---|---|
| `direct_swap2.json` | `i5A32BcCTCHfhpiDHJXncUxfDPCCLvsxXsuMGcGcCEcjsvr2E7qRwWjZ1GydbV4VPFeHtvGvHKXXYRDfAXKES77` | 452142837 | legacy | no | 1 (top-level `swap2`) |
| `jupiter_route_one_swap.json` | `3Pn3nn4pWAdYiqrUubC6jS29p1CTgSSfsEjiFBsWCE4Bji7v37HyUyHQTQTWmCzqEGVYFiBWVujkRAqEXapY5bY7` | 452139025 | 0 | no | 1 (inside Jupiter, stack height 2, events at 3) |
| `aggregator_two_swaps.json` | `yiaAsmFhSzHqCnSbLzWKaAjVaYqvYDkfnvNfvimEna8BrbQYEYvkJV2Yn5LCYSXA6rMgmJZvQfpHvTmG3P5BvXw` | 452139026 | 0 | no | 2 (`swap` then `swap2`) |
| `deep_nesting_two_swap2.json` | `3kytARpRou4qM9w7UMSwqpbCpHjvr4cXpU3fFrJZ83pNXhgD9tJMLcH2ZVTCUVRxZ97BrmFaJQesjRtKVU36WA56` | 452139025 | 0 | no | 2 (`swap2` at stack height 3, events at 4) |
| `failed_with_swap_event.json` | `3FpiEUCx7KDjLT2jKFsiVhy5xoUtresc6TwpJtg3AzCoqvqdje2KTj7CvheHXnvkVYX56eEPUoRT7o22DJ9Ad6XY` | 452146301 | 0 | yes, `InstructionError [2, Custom 1]` | 1 present in inner instructions. The decoder must yield 0. |

The event count comes from a 20-line independent parser, not from the decoder under test.
That parser matches a DLMM self-CPI with data prefix `e445a52e51cb9a1d` then
`516ce3becdd00ac4`. Read the amounts, mints, users and fees for the assertions from an
explorer (Solscan or Solana FM) by signature. Note that `version` is the string `"legacy"` or
the number `0`.
