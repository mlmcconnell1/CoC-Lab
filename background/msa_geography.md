# MSA Geography Guide

This note explains when to use `coc`, `metro`, or `msa`, how MSA artifacts are
named, and how CoC-native PIT counts are allocated into Census MSAs.

## Choosing a Geography

| Target | Use when | Identifier contract | Key artifacts |
| --- | --- | --- | --- |
| `coc` | You need official HUD Continuum of Care units and explicit boundary vintages. | `coc_id` + `boundary_vintage` | `coc__B<year>`, `xwalk__B<year>xT<year>`, `xwalk__B<year>xC<year>`, `panel__Y...@B<year>` |
| `metro` | You need the custom researcher-defined Glynn/Fox metro set. | `metro_id` + `definition_version` (for example `glynn_fox_v1`) | `metro_definitions__...`, `metro_coc_membership__...`, `metro_county_membership__...`, `panel__metro__...` |
| `msa` | You need Census Metropolitan Statistical Areas keyed to the official CBSA/MSA delineation. | `msa_id` = 5-digit CBSA/MSA code + `definition_version` (for example `census_msa_2023`) | `msa_definitions__...`, `msa_county_membership__...`, `msa_coc_xwalk__...`, `pit__msa__...`, `panel__msa__...` |

The important distinction is that `metro` and `msa` are not aliases:

- `metro` is the project's custom analysis geography with hand-curated membership.
- `msa` is the Census delineation surface and uses Census CBSA/MSA identifiers.
- Code and artifacts should never reuse `metro_id` for an MSA output or `msa_id`
  for a Glynn/Fox output.

## MSA Artifact Families

The MSA workflow introduces three curated artifact families:

- `data/curated/msa/msa_definitions__<definition>.parquet`
  Canonical list of MSAs with stable identifiers and names.
- `data/curated/msa/msa_county_membership__<definition>.parquet`
  Official MSA-to-county membership from the Census delineation workbook.
- `data/curated/xwalks/msa_coc_xwalk__B<boundary>xM<definition>xC<counties>.parquet`
  Auditable CoC-to-MSA PIT allocation crosswalk derived from CoC boundaries,
  county geometry, and MSA county membership.

Derived outputs keep the same distinction:

- `pit__msa__P<year>@M<definition>xB<boundary>xC<counties>.parquet`
- `panel__msa__Y<start>-<end>@M<definition>.parquet`

## CoC-to-MSA PIT Allocation Method

PIT counts are published natively at CoC geography, not MSA geography. HHP-Lab
therefore allocates PIT counts into MSAs through the stored CoC-to-MSA
crosswalk:

1. Build the CoC-to-county overlay for the chosen CoC boundary vintage.
2. Keep only counties that belong to each MSA according to the curated MSA
   county-membership artifact.
3. Sum CoC/county intersections to CoC/MSA intersections.
4. Compute `allocation_share = intersection_area / coc_area`.
5. Multiply each CoC PIT measure by `allocation_share`, then sum by `msa_id`.

This is an area-weighted allocation rule. It is explicit in the crosswalk
artifact via:

- `allocation_method = "area"`
- `share_column = "allocation_share"`
- `share_denominator = "coc_area"`

## Prerequisites

For an MSA workflow that consumes CoC PIT, the minimum prerequisites are:

- curated CoC boundary artifact for the boundary vintage
- county geometry for the county vintage used by the crosswalk
- MSA definitions and county membership for the selected `definition_version`
- PIT input at CoC geography

Typical commands:

```bash
hhplab generate msa --definition-version census_msa_2023
hhplab generate msa-xwalk --boundary 2020 --definition-version census_msa_2023 --counties 2020
hhplab build recipe-preflight --recipe recipes/examples/msa-census-pit-acs-pep-2020-2021.yaml --json
hhplab build recipe --recipe recipes/examples/msa-census-pit-acs-pep-2020-2021.yaml --json
```

## Limitations

- PIT is not published natively for MSAs, so MSA PIT values depend on the
  allocation rule and crosswalk vintage.
- CoCs can straddle counties that are outside any MSA. In that case the
  crosswalk can leave an explicit unallocated share rather than forcing a full
  allocation.
- `msa` does not imply the Glynn/Fox `metro` set, even though both ultimately
  rely on CBSA-related concepts in some source products.
