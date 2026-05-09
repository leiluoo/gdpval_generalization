"""
Pipeline A — Step 2: Collect generated files from sandbox outputs into Pool C.

After the sandbox framework runs all Pipeline A configs, this script:
  1. Reads configs/pipeline_a_manifest.json to know what was expected
  2. Looks in <sandbox_output_root>/{output_folder}/ for the generated file
  3. Records each found file in data/pool_c/pool_c_metadata.json

Each Pool C entry tracks:
  - Where the file lives locally
  - Which original GDPVal task it came from (for use in Pipeline B)
  - The diversity spec used to generate it

Usage:
    python collect_pool_c.py --output_root /path/to/sandbox/outputs
    python collect_pool_c.py --output_root ./sandbox_outputs --copy_to data/pool_c/files
"""

import json
import shutil
import argparse
from pathlib import Path

MANIFEST_PATH  = Path("configs/pipeline_a_manifest.json")
POOL_C_META    = Path("data/pool_c/pool_c_metadata.json")
POOL_C_DIR     = Path("data/pool_c/files")


def collect(output_root: Path, copy_to: Path | None) -> list:
    if not MANIFEST_PATH.exists():
        raise FileNotFoundError(f"Manifest not found: {MANIFEST_PATH}. Run gen_synth_file_configs.py first.")

    manifest = json.loads(MANIFEST_PATH.read_text())

    existing = json.loads(POOL_C_META.read_text()) if POOL_C_META.exists() else []
    done_keys = {(e["task_id"], e["variant_idx"]) for e in existing}

    new_entries = []
    missing     = []

    for entry in manifest:
        key = (entry["task_id"], entry["variant_idx"])
        if key in done_keys:
            continue

        # Claude names the file itself — scan for any file of the right extension.
        # Try both direct layout and workspace/ subdirectory.
        ext = entry["file_type"]
        search_dirs = [
            output_root / entry["output_folder"],
            output_root / entry["output_folder"] / "workspace",
        ]
        found = None
        for d in search_dirs:
            if d.exists():
                hits = sorted(d.glob(f"*.{ext}"))
                if hits:
                    found = hits[0]
                    break

        if found is None:
            missing.append(entry["output_folder"])
            continue

        dest = found
        if copy_to is not None:
            copy_to.mkdir(parents=True, exist_ok=True)
            dest = copy_to / f"{entry['config_idx']}_{entry['expected_output']}"
            shutil.copy2(found, dest)

        pool_c_entry = {
            "config_idx":         entry["config_idx"],
            "task_id":            entry["task_id"],
            "occupation":         entry["occupation"],
            "sector":             entry["sector"],
            "original_ref_obs":   entry["original_ref_obs"],
            "file_type":          entry["file_type"],
            "variant_idx":        entry["variant_idx"],
            "diversity_spec":     entry["diversity_spec"],
            "pool_c_file":        str(dest.resolve()),
        }
        new_entries.append(pool_c_entry)
        existing.append(pool_c_entry)
        POOL_C_META.parent.mkdir(parents=True, exist_ok=True)
        POOL_C_META.write_text(json.dumps(existing, indent=2, ensure_ascii=False))
        print(f"  Collected: config_{entry['config_idx']} → {dest.name}")

    if missing:
        print(f"\n  {len(missing)} output folders not found (sandbox may not have finished):")
        for m in missing[:10]:
            print(f"    {m}")
        if len(missing) > 10:
            print(f"    ... and {len(missing)-10} more")

    print(f"\nPool C: {len(existing)} total entries ({len(new_entries)} new).")
    return existing


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_root", required=True,
                        help="Root directory where sandbox framework places outputs")
    parser.add_argument("--copy_to", default=None,
                        help="If set, copy files here (default: reference in-place)")
    args = parser.parse_args()

    collect(
        output_root=Path(args.output_root),
        copy_to=Path(args.copy_to) if args.copy_to else None,
    )


if __name__ == "__main__":
    main()
