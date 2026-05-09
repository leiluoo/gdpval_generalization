"""
Pipeline B — Generate sandbox configs for task prompt + rubric production.

For each Pool C file, produces one config_{i}.json:
  - query:        Claude Code instruction to read the Pool C file and write task.txt + rubric.txt
  - resource_dir: OBS path(s) to the Pool C file
  - output:       sandbox output folder name

The query embeds the original GDPVal task's prompt and rubric as style references.
Outputs written to configs/pipeline_b/ and configs/pipeline_b_manifest.json.

Usage:
    python gen_task_configs.py
    python gen_task_configs.py --task_id 83d10b06
    python gen_task_configs.py --limit 10
    python gen_task_configs.py --pool_c_obs_prefix obs://my-bucket/pool_c/
"""

import json
import argparse
from pathlib import Path

TASKS_PATH    = Path("data/single_ref_tasks.json")
POOL_C_META   = Path("data/pool_c/pool_c_metadata.json")
CONFIGS_DIR   = Path("configs/pipeline_b")
MANIFEST_PATH = Path("configs/pipeline_b_manifest.json")

# OBS prefix where Pool C files will be stored after Pipeline A completes
DEFAULT_POOL_C_OBS = "obs://bucket-pangu-green-guiyang/qianmin/gdpval_generalization/pool_c/"

SKILL_DIR = [
    "obs://bucket-pangu-green-guiyang/z00935217/data/developing/skills/common_skills/spreadsheet",
    "obs://bucket-pangu-green-guiyang/z00935217/data/developing/skills/common_skills/pdf",
    "obs://bucket-pangu-green-guiyang/z00935217/data/developing/skills/common_skills/doc-coauthoring",
    "obs://bucket-pangu-green-guiyang/z00935217/data/developing/skills/common_skills/xlsx",
    "obs://bucket-pangu-green-guiyang/z00935217/data/developing/skills/common_skills/docx",
    "obs://bucket-pangu-green-guiyang/z00935217/data/developing/skills/common_skills/internal-comms",
    "obs://bucket-pangu-green-guiyang/z00935217/data/developing/skills/common_skills/pptx",
    "obs://bucket-pangu-green-guiyang/z00935217/data/developing/skills/common_skills/csv-data-summarizer",
    "obs://bucket-pangu-green-guiyang/z00935217/data/developing/skills/common_skills/pdf-processing-pro",
    "obs://bucket-pangu-green-guiyang/z00935217/data/developing/skills/common_skills/markitdown",
    "obs://bucket-pangu-green-guiyang/z00935217/data/developing/skills/common_skills/markdown-formatter",
]

CONFIGS_DIR.mkdir(parents=True, exist_ok=True)


# ── query builder ─────────────────────────────────────────────────────────────

def build_query(pool_c_entry: dict, original_task: dict) -> str:
    pool_c_fname = Path(pool_c_entry["pool_c_file"]).name
    file_type    = pool_c_entry["file_type"]
    occupation   = original_task["occupation"]
    deliverables = original_task["deliverable_files"]
    deliv_types  = list({f.split(".")[-1].lower() for f in deliverables})
    div_spec     = pool_c_entry.get("diversity_spec", {})
    diversity_note = "\n".join(
        f"  • {k.replace('_', ' ').upper()}: {v}" for k, v in div_spec.items()
    )

    original_prompt = original_task["prompt"][:2000]
    original_rubric = original_task["rubric_pretty"][:1500]

    return f"""You are creating a synthetic professional task specification for AI training data.

The reference file for this task is in the `reference_files/` folder: reference_files/{pool_c_fname}

═══════════════════════════════════════════════════
ORIGINAL TASK CONTEXT  (style and format reference — do NOT copy content)
═══════════════════════════════════════════════════
Occupation: {occupation}
Expected deliverable types: {deliv_types}

Original prompt (first ~2000 chars):
---
{original_prompt}
---

Original rubric:
---
{original_rubric}
---

═══════════════════════════════════════════════════
DIVERSITY SPEC  (what makes this variant different from the original)
═══════════════════════════════════════════════════
{diversity_note}

═══════════════════════════════════════════════════
YOUR TASK
═══════════════════════════════════════════════════
1. Read the reference file at reference_files/{pool_c_fname}
2. Write TWO files in the current workspace:

   task.txt
   --------
   A complete, self-contained professional task prompt that:
   • Establishes a clear role for the occupation: {occupation}
   • Is grounded in the ACTUAL content of the reference file
     (reference specific column names, sheet names, entity names, and numeric values)
   • Asks for the same TYPE of deliverable as the original: {deliv_types}
   • Includes all constraints needed to produce and evaluate the deliverable
     (exact output filenames, sheet names, column names, required calculations, formats)
   • Follows the same level of specificity and professional tone as the original
   • Is completable using only the reference file and the instructions in task.txt

   rubric.txt
   ----------
   10–14 scoring criteria, each on its own line, in the format:
   [+N] Criterion description   (N is 1 or 2)

   Criteria must:
   • Cover deliverable format/naming, computation correctness, and content accuracy
   • Reference specific values from the reference file (e.g. exact numbers, column names)
   • Be objectively verifiable — no vague or subjective criteria
   • Sum to at least 20 points total

RULES:
- Do NOT reuse company names, metric names, or scenario details from the original task
- Do NOT invent data that contradicts the reference file — ground everything in what you read
- Save both task.txt and rubric.txt as plain UTF-8 text in the workspace root
"""


# ── config generation ─────────────────────────────────────────────────────────

def generate_configs(pool_c: list, task_by_id: dict, pool_c_obs_prefix: str) -> None:
    manifest   = json.loads(MANIFEST_PATH.read_text()) if MANIFEST_PATH.exists() else []
    done_keys  = {(e["task_id"], e["variant_idx"]) for e in manifest}
    config_idx = max((e["config_idx"] for e in manifest), default=-1) + 1

    new_count = 0

    for entry in pool_c:
        tid = entry.get("task_id") or entry.get("original_task_id")
        entry = {**entry, "task_id": tid}

        key = (tid, entry["variant_idx"])
        if key in done_keys:
            continue

        original = task_by_id.get(tid)
        if not original:
            print(f"  SKIP (original task not found): {tid[:8]}")
            continue

        pool_c_fname = Path(entry["pool_c_file"]).name
        obs_uri      = pool_c_obs_prefix.rstrip("/") + "/" + pool_c_fname

        config = {
            "query":        build_query(entry, original),
            "resource_dir": [obs_uri],
            "output":       f"task_gen_{config_idx}",
        }

        (CONFIGS_DIR / f"config_{config_idx}.json").write_text(
            json.dumps(config, indent=2, ensure_ascii=False)
        )

        manifest_entry = {
            "config_idx":    config_idx,
            "task_id":       tid,
            "occupation":    original["occupation"],
            "sector":        original["sector"],
            "file_type":     entry["file_type"],
            "variant_idx":   entry["variant_idx"],
            "diversity_spec": entry.get("diversity_spec", {}),
            "pool_c_file":   entry["pool_c_file"],
            "pool_c_obs":    obs_uri,
            "output_folder": f"task_gen_{config_idx}",
        }
        manifest.append(manifest_entry)
        new_count += 1
        config_idx += 1

        if config_idx % 50 == 0:
            MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
            print(f"  ... {config_idx} configs generated")

    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
    print(f"\nDone. {new_count} new configs written (total {len(manifest)}).")
    print(f"Configs: {CONFIGS_DIR}/   Manifest: {MANIFEST_PATH}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--task_id",          help="Filter by original task ID prefix")
    parser.add_argument("--limit",            type=int, default=None)
    parser.add_argument("--pool_c_obs_prefix", default=DEFAULT_POOL_C_OBS,
                        help="OBS prefix where Pool C files are stored")
    args = parser.parse_args()

    if not POOL_C_META.exists():
        raise FileNotFoundError(f"{POOL_C_META} not found — run collect_pool_c.py first.")

    pool_c = json.loads(POOL_C_META.read_text())

    with open(TASKS_PATH) as f:
        task_by_id = {t["task_id"]: t for t in json.load(f)}

    if args.task_id:
        pool_c = [e for e in pool_c if (e.get("task_id") or e.get("original_task_id", "")).startswith(args.task_id)]
    if args.limit:
        pool_c = pool_c[: args.limit]

    print(f"Generating Pipeline B configs for {len(pool_c)} Pool C entries ...")
    generate_configs(pool_c, task_by_id, args.pool_c_obs_prefix)


if __name__ == "__main__":
    main()
