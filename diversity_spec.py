"""
Principled diversity dimensions derived from analysis of the GDPVal dataset.

Dimensions are chosen based on what changes the MODEL'S TASK, not just the surface content:
  - XLSX: data_structure × computation × time_dimension × domain
  - DOCX: doc_type × content_generation_need × structural_complexity × domain
  - PDF:  doc_type × content_density × domain

Each axis is grounded in what the rubric actually tests and what skills the model exercises.
"""

# ── XLSX axes ─────────────────────────────────────────────────────────────────
# Derived from: 90.6% have time series, 96.9% have formulas,
# 31.2% are multi-sheet, 75% are matrix/cross-tab structured.

XLSX_AXES = {
    # Changes the information-extraction pattern the model must use
    "data_structure": [
        "single flat table — all data in one sheet, each row is an independent record",
        "hierarchical columns — parent/child groupings in columns (e.g. Division > Sub-Division > Entity), rows represent leaf-level records",
        "multi-sheet with cross-references — data split across 2-3 sheets; downstream sheets reference upstream ones",
        "pivot/cross-tab matrix — categories on both row and column axes; cells are intersection values",
    ],

    # Changes the computation the model must perform
    "computation": [
        "raw data only — no formulas; model must compute everything from scratch",
        "simple aggregation — SUM/COUNT/AVERAGE formulas present as scaffolding",
        "conditional logic — IF/IFS/VLOOKUP conditions that require understanding thresholds or lookup rules",
        "multi-step derived columns — intermediate computed columns that feed into final summary columns",
    ],

    # Changes whether and how the model handles time
    "time_dimension": [
        "no time dimension — static snapshot, no periods",
        "two-period side-by-side — two columns for two quarters/years; model must compute variance",
        "multi-period time series — 4+ consecutive periods for trend or YTD analysis",
        "rolling or cumulative — requires understanding window logic (e.g. trailing 3-month average)",
    ],

    # Surface-level content domain — affects vocabulary only, not task logic
    "domain": [
        "financial risk and compliance metrics (KRIs, VaR, regulatory breach counts)",
        "sales and revenue performance (pipeline, bookings, revenue by product/region)",
        "operational efficiency (throughput, cycle time, defect rate, equipment utilization)",
        "HR and workforce analytics (headcount, attrition, training completion, grade distribution)",
        "supply chain and procurement (inventory turns, lead time, supplier scorecard, PO fulfillment)",
        "customer experience metrics (NPS, CSAT, ticket volume, resolution SLA adherence)",
    ],
}

# ── DOCX axes ─────────────────────────────────────────────────────────────────
# Derived from: 100% are template/form-fill, 81% require narrative generation,
# 95.2% have multi-level headings, 76.2% embed tables.

DOCX_AXES = {
    # Changes the genre and therefore the schema of the document
    "doc_type": [
        "vendor quotation comparison — multiple supplier quotes with pricing tables per vendor",
        "technical specification — product/system specs with requirement tables and tolerance values",
        "policy or procedure reference — rules with applicability conditions and exception tables",
        "performance or status report — period metrics with narrative commentary sections",
        "work plan or project schedule — tasks, owners, deadlines in tabular or list form",
    ],

    # Changes what the model must generate vs. extract
    "content_generation_need": [
        "data extraction only — model reads numbers from tables and computes; no narrative to write",
        "template fill — structured document with explicit gaps (placeholders) to fill in",
        "comparative analysis — model must evaluate multiple options from tables and write a recommendation",
        "narrative synthesis — model must turn tabular data into cohesive prose paragraphs",
    ],

    # Changes the structural parsing difficulty
    "structural_complexity": [
        "flat: one main table plus a short narrative — minimal heading hierarchy",
        "moderate: 2-3 tables with section headings and cross-references between them",
        "complex: multi-level headings (H1/H2/H3) with embedded tables, lists, and footnotes",
    ],

    # Surface-level domain
    "domain": [
        "automotive components and manufacturing",
        "pharmaceutical and medical devices",
        "construction materials and civil engineering",
        "software and IT services",
        "consumer electronics and semiconductors",
        "energy equipment and industrial machinery",
    ],
}

# ── PDF axes ──────────────────────────────────────────────────────────────────
# Derived from: PDFs are mostly schedule/policy/report documents.

PDF_AXES = {
    "doc_type": [
        "schedule or calendar — dates, locations, activities in a tabular layout",
        "policy or internal procedure — rules, conditions, and exception handling",
        "summary report — aggregated data with section headings and narrative",
        "form or application template — pre-formatted fields with label-value structure",
    ],

    # Changes how much the model needs to parse vs. generate
    "content_density": [
        "table-heavy — most content is structured tables; little free text",
        "mixed — roughly equal tables and narrative paragraphs",
        "text-heavy — mostly narrative; tables are supplementary",
    ],

    "domain": [
        "government and public administration",
        "healthcare and social services",
        "real estate and facilities management",
        "legal and compliance",
        "education and training",
        "logistics and transportation",
    ],
}


# ── assignment function ───────────────────────────────────────────────────────

def build_diversity_spec(file_type: str, variant_idx: int, task_seed: int = 0) -> dict:
    """
    Deterministically assign a unique, non-repeating combination of axes to each variant.

    Uses independent rotation offsets per axis so that consecutive variants
    differ on all axes simultaneously rather than cycling through one axis at a time.
    """
    if file_type == "xlsx":
        axes = XLSX_AXES
    elif file_type == "docx":
        axes = DOCX_AXES
    else:
        axes = PDF_AXES

    # Prime-based offsets ensure axes rotate at different rates → low collision
    offsets = [1, 3, 7, 13]
    result = {}
    for i, (key, options) in enumerate(axes.items()):
        idx = (variant_idx * offsets[i % len(offsets)] + task_seed) % len(options)
        result[key] = options[idx]

    return result


def diversity_instruction(spec: dict, file_type: str) -> str:
    """
    Format the diversity spec into a concrete instruction block for the LLM.
    """
    lines = ["YOU MUST generate a file that satisfies ALL of the following constraints:"]
    for key, value in spec.items():
        label = key.replace("_", " ").upper()
        lines.append(f"  • {label}: {value}")
    lines.append(
        "\nDo not ignore any constraint above. "
        "If a constraint conflicts with a superficially 'simpler' approach, "
        "follow the constraint."
    )
    return "\n".join(lines)
