"""
Scan Pipeline A OBS outputs and generate Pipeline B (task+rubric) configs.

For each synth_file_{i}/ folder in obs_root:
  1. List the folder via moxing; find the .xlsx/.docx/.pdf Claude generated
  2. Look up the corresponding original GDPVal task via the Pipeline A manifest
  3. Write configs/pipeline_b/config_{j}.json whose query embeds the original
     task prompt and rubric as style/format references

Standalone — no Python imports from other modules in this repo.
OBS folder scanning (not manifest) is the source of truth for enumeration.
The Pipeline A manifest is used only as a lookup table for task metadata.

Usage:
    python collect_pool_c_obs.py
    python collect_pool_c_obs.py \
        --obs_root   obs://bucket-pangu-green-guiyang/o00853405/agent/trajs/extracted_gdpval_pipeline_a \
        --obs_output obs://bucket-pangu-green-guiyang/o00853405/agent/trajs/extracted_gdpval_pipeline_b \
        --manifest_a configs/pipeline_a_manifest.json \
        --tasks_json data/single_ref_tasks.json \
        --n_folders  6600 \
        --limit      10
"""

import json
import argparse
from pathlib import Path

import moxing as mox


# ── constants ─────────────────────────────────────────────────────────────────

DEFAULT_OBS_ROOT      = "obs://bucket-pangu-green-guiyang/o00853405/agent/trajs/extracted_gdpval_pipeline_a"
DEFAULT_OBS_OUTPUT    = "obs://bucket-pangu-green-guiyang/o00853405/agent/trajs/extracted_gdpval_pipeline_b"
DEFAULT_N_FOLDERS     = 6600
DEFAULT_MANIFEST_A    = Path("configs/pipeline_a_manifest.json")
DEFAULT_TASKS_JSON    = Path("data/single_ref_tasks.json")
MANIFEST_B_PATH       = Path("configs/pipeline_b_manifest.json")

SUPPORTED_TYPES = {"xlsx", "docx", "pdf"}

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


# ── OBS helper ────────────────────────────────────────────────────────────────

def find_generated_file(obs_folder: str) -> tuple[str, str] | tuple[None, None]:
    """
    List obs_folder and return (obs_uri, file_type) if exactly one supported
    reference file (.xlsx/.docx/.pdf) is found.

    Returns (None, None) if the folder is absent, empty, or contains more than
    one supported file (ambiguous output — skip to avoid picking the wrong one).

    mox.file.list_directory returns basenames only (not full paths).
    """
    try:
        names = mox.file.list_directory(obs_folder, recursive=False)
    except Exception:
        return None, None

    hits = [
        (obs_folder.rstrip("/") + "/" + name, name.rsplit(".", 1)[-1].lower())
        for name in names
        if "." in name and name.rsplit(".", 1)[-1].lower() in SUPPORTED_TYPES
    ]

    if len(hits) != 1:
        return None, None

    return hits[0]


# ── Pipeline B query builder ──────────────────────────────────────────────────

def build_query(
    pool_c_fname: str,
    occupation: str,
    deliv_types: list[str],
    diversity_spec: dict,
    original_prompt: str,
    original_rubric: str,
) -> str:
    diversity_note = "\n".join(
        f"  • {k.replace('_', ' ').upper()}: {v}" for k, v in diversity_spec.items()
    )

    return f"""You are creating a synthetic professional task specification for AI training data.

The reference file for this task is in the `reference_files/` folder: reference_files/{pool_c_fname}

═══════════════════════════════════════════════════
ORIGINAL TASK CONTEXT  (style and difficulty reference — do NOT copy content)
═══════════════════════════════════════════════════
Original occupation (for seniority/specificity reference only): {occupation}
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
DIVERSITY SPEC  (the generated reference file was built to this spec)
═══════════════════════════════════════════════════
{diversity_note}

═══════════════════════════════════════════════════
YOUR TASK
═══════════════════════════════════════════════════
1. Read the reference file at reference_files/{pool_c_fname}
2. Determine the appropriate professional occupation for the task by looking at
   the file's actual content and domain — do NOT default to "{occupation}" if
   the file's subject matter belongs to a different field.
3. Write TWO files in the current workspace:

   task.txt
   --------
   A complete, self-contained professional task prompt that:
   • Opens with "You are a [X]." where [X] is the occupation that naturally
     works with the content of the reference file
   • Is grounded in the ACTUAL content of the reference file
     (reference specific column names, sheet names, entity names, and numeric values)
   • Asks for the same TYPE of deliverable as the original: {deliv_types}
   • Includes all constraints needed to produce and evaluate the deliverable
     (exact output filenames, sheet names, column names, required calculations, formats)
   • Matches the same level of specificity and professional tone as the original
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


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--obs_root",   default=DEFAULT_OBS_ROOT,
                        help="OBS prefix containing synth_file_0 … synth_file_N folders")
    parser.add_argument("--manifest_a", type=Path, default=DEFAULT_MANIFEST_A,
                        help="Pipeline A manifest JSON (used as lookup table for task metadata)")
    parser.add_argument("--tasks_json", type=Path, default=DEFAULT_TASKS_JSON,
                        help="GDPVal single-ref tasks JSON")
    parser.add_argument("--n_folders",  type=int, default=DEFAULT_N_FOLDERS,
                        help="Total number of synth_file_* folders to scan (0 … N-1)")
    parser.add_argument("--obs_output", default=DEFAULT_OBS_OUTPUT,
                        help="OBS prefix to write Pipeline B config JSONs")
    parser.add_argument("--limit",      type=int, default=None,
                        help="Stop after processing this many folders (for testing)")
    args = parser.parse_args()

    obs_root   = args.obs_root.rstrip("/")
    obs_output = args.obs_output.rstrip("/")
    MANIFEST_B_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Load Pipeline A manifest as a lookup table: config_idx → entry
    manifest_a_by_idx: dict[int, dict] = {}
    if args.manifest_a.exists():
        for entry in json.loads(args.manifest_a.read_text()):
            manifest_a_by_idx[entry["config_idx"]] = entry
    else:
        print(f"WARNING: {args.manifest_a} not found — task metadata will be unavailable")

    # Load original tasks: task_id → task
    task_by_id: dict[str, dict] = {}
    if args.tasks_json.exists():
        with open(args.tasks_json) as f:
            task_by_id = {t["task_id"]: t for t in json.load(f)}
    else:
        print(f"WARNING: {args.tasks_json} not found — original prompts/rubrics unavailable")

    # Load existing Pipeline B manifest (incremental: skip already-written configs)
    manifest_b = json.loads(MANIFEST_B_PATH.read_text()) if MANIFEST_B_PATH.exists() else []
    done_ids = {e["idx"] for e in manifest_b}

    n = args.limit if args.limit is not None else args.n_folders
    written = 0
    missing = []   # no folder or empty
    ambiguous = [] # more than one supported file

    for i in range(n):
        if i in done_ids:
            continue

        # ── find the generated file on OBS ───────────────────────────────────
        obs_folder = f"{obs_root}/synth_file_{i}"

        # Check for ambiguous output before calling the combined helper
        try:
            all_names = mox.file.list_directory(obs_folder, recursive=False)
            hits = [fname for fname in all_names
                    if "." in fname and fname.rsplit(".", 1)[-1].lower() in SUPPORTED_TYPES]
        except Exception:
            hits = []

        if len(hits) > 1:
            ambiguous.append(i)
            continue

        obs_file, file_type = find_generated_file(obs_folder)
        if obs_file is None or file_type is None:
            missing.append(i)
            continue

        pool_c_fname = obs_file.rsplit("/", 1)[-1]

        # ── look up original task metadata ───────────────────────────────────
        a_entry = manifest_a_by_idx.get(i, {})
        task_id = a_entry.get("task_id", "")
        original = task_by_id.get(task_id, {})

        occupation   = original.get("occupation", "professional")
        deliverables = original.get("deliverable_files", [f"output.{file_type}"])
        deliv_types  = list({f.split(".")[-1].lower() for f in deliverables})
        diversity_spec = a_entry.get("diversity_spec", {})
        original_prompt = original.get("prompt", "")[:2000]
        original_rubric = original.get("rubric_pretty", "")[:1500]

        # ── write Pipeline B config (index aligned with synth_file index) ─────
        config_b = {
            "query": build_query(
                pool_c_fname,
                occupation,
                deliv_types,
                diversity_spec,
                original_prompt,
                original_rubric,
            ),
            "resource_dir": [obs_file],
            "skill_dir":    SKILL_DIR,
            "output":       f"task_gen_{i}",
        }
        mox.file.write(
            f"{obs_output}/config_{i}.json",
            json.dumps(config_b, indent=2, ensure_ascii=False),
        )

        manifest_b.append({
            "idx":          i,          # aligned with synth_file_{i} and config_{i}
            "task_id":      task_id,
            "occupation":   occupation,
            "file_type":    file_type,
            "diversity_spec": diversity_spec,
            "pool_c_obs":   obs_file,
            "output_folder": f"task_gen_{i}",
        })
        done_ids.add(i)
        written += 1

        if written % 50 == 0:
            MANIFEST_B_PATH.write_text(json.dumps(manifest_b, indent=2, ensure_ascii=False))
            print(f"  ... {written} Pipeline B configs written")

    MANIFEST_B_PATH.write_text(json.dumps(manifest_b, indent=2, ensure_ascii=False))

    print(f"\nDone.")
    print(f"  Pipeline B configs written : {written}")
    print(f"  Missing / empty folders    : {len(missing)}")
    print(f"  Skipped (multiple files)   : {len(ambiguous)}")
    print(f"  Total in manifest          : {len(manifest_b)}")

    if missing:
        preview = missing[:10]
        print(f"\n  Missing synth_file_* indices (first {len(preview)} of {len(missing)}):")
        for idx in preview:
            print(f"    synth_file_{idx}")
        if len(missing) > 10:
            print(f"    ... and {len(missing) - 10} more")

    if ambiguous:
        preview = ambiguous[:10]
        print(f"\n  Ambiguous (multiple files) synth_file_* indices (first {len(preview)} of {len(ambiguous)}):")
        for idx in preview:
            print(f"    synth_file_{idx}")
        if len(ambiguous) > 10:
            print(f"    ... and {len(ambiguous) - 10} more")

    print(f"\n  Configs  : {obs_output}/")
    print(f"  Manifest : {MANIFEST_B_PATH}")


if __name__ == "__main__":
    main()
