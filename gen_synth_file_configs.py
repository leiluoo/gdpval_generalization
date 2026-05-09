"""
Pipeline A — Generate sandbox configs for synthetic reference file production.

For each (original_task × variant), produces one config_{i}.json:
  - query:        Claude Code instruction to read the original file and produce a variant
  - resource_dir: OBS path(s) to the original GDPVal reference file
  - output:       sandbox output folder name

Usage:
    python gen_synth_file_configs.py --all --variants 100
    python gen_synth_file_configs.py --task_id 83d10b06 --variants 3   # test
"""

import json
import argparse
from pathlib import Path

from diversity_spec import build_diversity_spec, diversity_instruction

TASKS_PATH    = Path("data/single_ref_tasks.json")
CONFIGS_DIR   = Path("configs/pipeline_a")
MANIFEST_PATH = Path("configs/pipeline_a_manifest.json")

OBS_PREFIX = "obs://bucket-pangu-green-guiyang/qianmin/opensource_dataset/gdpval/"

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


def obs_path(task: dict) -> str:
    """Construct OBS path from task's reference_files field."""
    rel = task["reference_files"][0]   # e.g. "reference_files/abc123/Population v2.xlsx"
    return OBS_PREFIX + rel


# ── query builder ─────────────────────────────────────────────────────────────

def build_query(task: dict, file_type: str, div_spec: dict) -> str:
    orig_fname = task["reference_files"][0].split("/")[-1]
    constraints = diversity_instruction(div_spec, file_type)

    return f"""You are creating a synthetic reference file for AI training data.

The original file `{orig_fname}` is in the `reference_files/` folder.
Read it carefully to understand its structure, column layout, data types, and conventions.

Your job is to create a NEW {file_type.upper()} file with a different scenario but the same structural pattern. Follow ALL of these constraints exactly:

{constraints}

INSTRUCTIONS:
1. Open and examine `reference_files/{orig_fname}`
2. Create a new {file_type.upper()} file that:
   - Follows the same structural pattern as the original (same number of sheets / same table layout / same document genre)
   - Adapts content and column semantics to the DOMAIN constraint above
   - Implements the DATA STRUCTURE, COMPUTATION, and TIME DIMENSION constraints
   - Uses completely fictional entity names (companies, people, organisations, places)
   - Has realistic numeric values appropriate for the specified domain
3. Choose a descriptive filename that reflects the fictional organisation and data content
   (e.g. `Meridian_Sales_Pipeline_Q3.xlsx`, `Thornfield_HR_Attrition_2024.docx`).
   The name must end with `.{file_type}` and contain no spaces.
   Save the file with that name in the current workspace directory.

RULES:
- Do NOT copy any actual data values, company names, or metric names from the original file
- Do NOT reuse the original filename or any part of it in your chosen filename
- All content must be internally consistent and realistic
- The output must be a valid, well-formed {file_type.upper()} file
- If the original has multiple sheets, create the same number of sheets with matching purposes
"""


# ── config generation ─────────────────────────────────────────────────────────

def generate_configs(tasks: list, variants: int) -> None:
    # Load existing manifest for incremental runs
    manifest = json.loads(MANIFEST_PATH.read_text()) if MANIFEST_PATH.exists() else []
    done_keys  = {(e["task_id"], e["variant_idx"]) for e in manifest}
    config_idx = max((e["config_idx"] for e in manifest), default=-1) + 1

    new_entries = []

    for task in tasks:
        fname     = task["reference_files"][0].split("/")[-1]
        file_type = fname.rsplit(".", 1)[-1].lower() if "." in fname else "bin"
        task_seed = hash(task["task_id"]) & 0xFFFF

        for v in range(variants):
            if (task["task_id"], v) in done_keys:
                continue

            div_spec = build_diversity_spec(file_type, v, task_seed)

            config = {
                "query":        build_query(task, file_type, div_spec),
                "resource_dir": [obs_path(task)],
                "skill_dir":    SKILL_DIR,
                "output":       f"synth_file_{config_idx}",
            }

            (CONFIGS_DIR / f"config_{config_idx}.json").write_text(
                json.dumps(config, indent=2, ensure_ascii=False)
            )

            entry = {
                "config_idx":       config_idx,
                "task_id":          task["task_id"],
                "occupation":       task["occupation"],
                "sector":           task["sector"],
                "original_ref_obs": obs_path(task),
                "original_fname":   fname,
                "file_type":        file_type,
                "variant_idx":      v,
                "diversity_spec":   div_spec,
                "output_folder":    f"synth_file_{config_idx}",
            }
            new_entries.append(entry)
            manifest.append(entry)
            config_idx += 1

            if config_idx % 50 == 0:
                # Flush manifest periodically
                MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
                print(f"  ... {config_idx} configs generated")

    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
    print(f"\nDone. {len(new_entries)} new configs written (total {len(manifest)}).")
    print(f"Configs: {CONFIGS_DIR}/   Manifest: {MANIFEST_PATH}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--task_id",    help="Task ID prefix (for testing)")
    parser.add_argument("--all",        action="store_true")
    parser.add_argument("--variants",   type=int, default=100)
    parser.add_argument("--file_types", default="xlsx,docx,pdf")
    args = parser.parse_args()

    with open(TASKS_PATH) as f:
        all_tasks = json.load(f)

    allowed  = set(args.file_types.split(","))
    eligible = [
        t for t in all_tasks
        if t["reference_files"][0].rsplit(".", 1)[-1].lower() in allowed
    ]

    if args.task_id:
        eligible = [t for t in eligible if t["task_id"].startswith(args.task_id)]
    elif not args.all:
        eligible = eligible[:2]

    total = len(eligible) * args.variants
    print(f"Generating {len(eligible)} tasks × {args.variants} variants = {total} configs ...")
    generate_configs(eligible, args.variants)


if __name__ == "__main__":
    main()
