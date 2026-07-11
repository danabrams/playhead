# Corpus Quarantine

Files below this directory are evidence artifacts, not canonical labels. No
loader or evaluation harness may discover them by directory scan.

## `fresh-b-coordinate-annotations/`

These 36 annotations were generated from temporary fresh-download (B)
coordinates, but were written with retained snapshot (A) fingerprints. The B
assets were deleted, so the coordinates cannot be transformed or verified.
They are preserved only for forensic comparison and are deliberately absent
from `Annotations/_canonical-manifest.json`.

## `assetless-audit-evidence/`

These are the original assetless July audit and reject ledgers. The 70 audit
rows were played against retained A by episode ID. Their active copies under
`Audits/` are a one-time migration bound to hashes recomputed from those exact
A files. The nine corresponding active vetoes are likewise A-specific. The 13
older reject rows had no traceable listening asset and remain quarantined.
