# France DVF Import – Row Count & Data Quality

## Why 1.2M vs 3.5M rows?

| Factor | Effect |
|--------|--------|
| **Single year** | ValeursFoncieres-2024.txt contains one year of data. Full DVF history would have more rows. |
| **Vente only** | We filter `natureMutation === "Vente"`. Echange, donation, expropriation, etc. are skipped. |
| **Deduplication** | We keep one row per `(address, lot_number)` – the most recent transaction. Multiple sales of the same unit are merged. |
| **With lot_number** | After mapping 1er lot → lot_number, multi-unit buildings produce multiple rows. Row count increases vs address-only merge. |

**Result**: ~1.2M rows when merging by address only; with lot_number, expect more rows (one per apartment in multi-unit buildings).

## Lot number (1er lot)

The DVF column **1er lot** (column 24, 0-indexed) is imported into `lot_number`. This enables apartment-level search when multiple units share the same address.

## Re-import with lot numbers

1. Ensure migration `20250318000000_add_lot_number_france.sql` has been applied (adds `lot_number`, composite PK).
2. Run: `npx ts-node scripts/import-france-properties.ts [path-to-ValeursFoncieres-*.txt]`
3. The script uses `onConflict: "address,lot_number"` for upsert.
