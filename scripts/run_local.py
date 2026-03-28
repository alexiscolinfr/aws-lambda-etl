import ast
import contextlib
import importlib
import os
import re
import subprocess
import sys
from pathlib import Path


# Add the src directory to the Python path
ROOT_DIR = Path(__file__).parent.parent.resolve()
SRC_DIR = ROOT_DIR / "src"
sys.path.insert(0, str(SRC_DIR))

# --- Configuration -------------------------------------------------------

CATEGORY_LABELS: dict[str, str] = {
    "c": "Chatbot",
    "e": "Extract",
    "d": "Dimension",
    "f": "Fact",
    "p": "Pricing",
}

# Subdirectories to scan, mapped to their menu key prefix and groups
CATEGORY_SUBDIRS: dict[str, tuple[str, list[str]]] = {
    "chatbot": ("c", []),
    "data_extraction": ("e", ["g_all_extract"]),
    "dimensions": ("d", ["g_all_dims", "g_all_dwh"]),
    "facts": ("f", ["g_all_facts", "g_all_dwh"]),
    "pricing": ("p", []),
}

# Prefixes/suffixes stripped from file stems to generate short menu keys.
# Longer prefixes must come first so they take priority.
STEM_STRIP: dict[str, tuple[list[str], list[str]]] = {
    "chatbot": ([], []),
    "data_extraction": (["tmp_"], ["_extract"]),
    "dimensions": (["scd_", "sd_", "rpd_"], []),
    "facts": (["fact_model_config_", "fact_"], []),
    "pricing": (["model_"], []),
}

# Path to the DAG doc (sibling repo). Order falls back to alphabetical if not found.
DAG_PATH = ROOT_DIR.parent / "docs" / "docs" / "data" / "dag.md"

# --- DAG parsing & topological sort --------------------------------------


def _parse_dag(dag_path: Path) -> dict[str, set[str]]:
    """Parse a Mermaid file and return {node: set_of_its_dependencies}."""
    deps: dict[str, set[str]] = {}
    try:
        text = dag_path.read_text()
    except FileNotFoundError:
        return deps
    for match in re.finditer(r"(\w+)\s*-->\s*(\w+)", text):
        src, dst = match.group(1), match.group(2)
        deps.setdefault(src, set())
        deps.setdefault(dst, set()).add(src)
    return deps


def _topological_sort(class_paths: list[str], dag: dict[str, set[str]]) -> list[str]:
    """Sort class_paths by topological order (Kahn's algorithm).

    DAG node names match class names directly (e.g. RPDDate, FactQuoteItem).
    Pipes absent from the DAG are appended alphabetically at the end.
    """
    in_group = set(class_paths)
    # Index class paths by their bare class name for O(1) DAG lookups
    class_name_to_cp = {cp.rsplit(".", 1)[1]: cp for cp in in_group}

    # Build subgraph adjacency and in-degrees
    graph: dict[str, list[str]] = {cp: [] for cp in in_group}
    in_degree: dict[str, int] = dict.fromkeys(in_group, 0)

    for cp in in_group:
        node = cp.rsplit(".", 1)[1]
        for dep_node in dag.get(node, set()):
            dep_cp = class_name_to_cp.get(dep_node)
            if dep_cp:
                graph[dep_cp].append(cp)
                in_degree[cp] += 1

    # Kahn's algorithm — sort at each step for determinism
    queue = sorted(cp for cp, d in in_degree.items() if d == 0)
    result: list[str] = []
    while queue:
        cp = queue.pop(0)
        result.append(cp)
        for neighbor in sorted(graph[cp]):
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
                queue.sort()

    # Pipes not reached (not in the DAG) — append alphabetically
    result.extend(sorted(cp for cp in in_group if cp not in result))
    return result


_DAG = _parse_dag(DAG_PATH)

# --- Auto-discovery ------------------------------------------------------


def _find_pipe_subclass(filepath: Path) -> str | None:
    """Return the name of the first class inheriting from Pipe in a file."""
    try:
        tree = ast.parse(filepath.read_text())
    except SyntaxError:
        return None
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        for base in node.bases:
            name = base.id if isinstance(base, ast.Name) else getattr(base, "attr", None)
            if name == "Pipe":
                return node.name
    return None


def _shorten_stem(stem: str, subdir: str) -> str:
    """Strip known redundant prefixes and suffixes from a file stem."""
    prefixes, suffixes = STEM_STRIP.get(subdir, ([], []))
    for p in prefixes:
        if stem.startswith(p):
            stem = stem[len(p):]
            break
    for s in suffixes:
        if stem.endswith(s):
            stem = stem[: -len(s)]
            break
    return stem


def _key_variants(stem: str):
    """Yield increasingly longer key candidates for a stem.

    Phase 1 — initials of preceding words + growing prefix of the last word:
        purchase_channel → pc, pch, pcha, ...
    Phase 2 — growing prefix of the compact form (no underscores), used as a
    fallback when multiple stems share the same last word:
        metric_snapshot / mro_snapshot both stall at 'msnapshot' in phase 1,
        then diverge at 'me' / 'mr' in phase 2.
    """
    words = [w for w in stem.split("_") if w]
    compact = stem.replace("_", "")
    prev_init = "".join(w[0] for w in words[:-1])
    last = words[-1] if words else ""
    seen: set[str] = set()

    for i in range(1, len(last) + 1):
        k = prev_init + last[:i]
        if k not in seen:
            seen.add(k)
            yield k

    for i in range(1, len(compact) + 1):
        k = compact[:i]
        if k not in seen:
            seen.add(k)
            yield k


def _resolve_keys(stems: list[str]) -> dict[str, str]:
    """Assign each stem a short unique key (initials + collision resolution)."""
    gen = {s: _key_variants(s) for s in stems}
    keys = {s: next(g) for s, g in gen.items()}

    while True:
        by_key: dict[str, list[str]] = {}
        for s, k in keys.items():
            by_key.setdefault(k, []).append(s)
        conflicts = [group for group in by_key.values() if len(group) > 1]
        if not conflicts:
            break
        for group in conflicts:
            for s in group:
                with contextlib.suppress(StopIteration):
                    keys[s] = next(gen[s])
    return keys


def _discover_options() -> dict[str, tuple[str, str, list[str]]]:
    pipes_dir = SRC_DIR / "pipes"
    options: dict[str, tuple[str, str, list[str]]] = {}
    for subdir, (prefix, groups) in CATEGORY_SUBDIRS.items():
        pipes: list[tuple[Path, str]] = []
        for py_file in sorted((pipes_dir / subdir).glob("*.py")):
            if py_file.stem == "__init__":
                continue
            class_name = _find_pipe_subclass(py_file)
            if class_name is not None:
                pipes.append((py_file, class_name))

        short_stems = [_shorten_stem(f.stem, subdir) for f, _ in pipes]
        stem_to_key = _resolve_keys(short_stems)

        for (py_file, class_name), short_stem in zip(pipes, short_stems, strict=False):
            class_path = f"pipes.{subdir}.{py_file.stem}.{class_name}"
            options[f"{prefix}_{stem_to_key[short_stem]}"] = (
                py_file.stem,
                class_path,
                groups,
            )
    return options


type PipeEntry = tuple[str, str, list[str]]

OPTIONS: dict[str, PipeEntry] = _discover_options()
ALL_GROUPS: set[str] = {group for _, _, groups in OPTIONS.values() for group in groups}

# --- Helpers -------------------------------------------------------------


def toggle_debug_mode(current_debug: bool) -> bool:
    return not current_debug


def clear() -> None:
    subprocess.run(["cls"] if os.name == "nt" else ["clear"], check=True)


def run_pipe(class_path: str, debug: bool) -> None:
    """Lazily imports and executes a pipe."""
    module_path, class_name = class_path.rsplit(".", 1)
    pipe_class = getattr(importlib.import_module(module_path), class_name)
    pipe_class(debug=debug)({}, {})


def _group_class_paths(group: str) -> list[str]:
    """Return class paths for a group, sorted by topological order."""
    class_paths = [cp for _, cp, groups in OPTIONS.values() if group in groups]
    return _topological_sort(class_paths, _DAG)


def display_menu(debug: bool) -> None:
    """Renders the menu grouped by category."""
    status = "\x1b[6;30;42menabled\x1b[0m" if debug else "\x1b[6;30;41mdisabled\x1b[0m"
    print(f"Debug mode: {status}\n")

    grouped: dict[str, list[tuple[str, str]]] = {}
    for key, (label, _, _) in OPTIONS.items():
        prefix = key.split("_")[0]
        category = CATEGORY_LABELS.get(prefix, "other")
        grouped.setdefault(category, []).append((key, label))

    all_keys = list(OPTIONS) + sorted(ALL_GROUPS) + ["d", "x"]
    w = max(len(k) for k in all_keys) + 2

    print(f"  {'KEY':<{w}} DESCRIPTION")
    for category, items in grouped.items():
        print(f"\n  ── {category.upper()} ──")
        for key, label in items:
            print(f"  {key:<{w}} {label}")

    if ALL_GROUPS:
        print("\n  ── GROUPS ──")
        for group in sorted(ALL_GROUPS):
            short = group.removeprefix("g_all_")
            print(f"  {group:<{w}} Run all {short} pipes")

    print(f"\n  {'d':<{w}} Toggle debug mode")
    print(f"  {'x':<{w}} Exit\n")


# --- Entry point ---------------------------------------------------------


def main() -> None:
    clear()
    debug = False

    while True:
        display_menu(debug)

        raw = input("Keys to run (comma or space separated): ").strip()
        if not raw:
            continue

        choices = raw.replace(",", " ").split()

        if choices == ["x"]:
            sys.exit(0)
        if choices == ["d"]:
            debug = toggle_debug_mode(debug)
            clear()
            continue
        if any(c in ("d", "x") for c in choices):
            clear()
            print("\x1b[0;33;40m'debug' and 'exit' must be used alone.\x1b[0m\n")
            continue

        valid = [c for c in choices if c in OPTIONS or c in ALL_GROUPS]
        if not valid:
            clear()
            print("\x1b[0;33;40mInvalid option(s). Please try again.\x1b[0m\n")
            continue

        clear()
        for choice in valid:
            if choice in ALL_GROUPS:
                for class_path in _group_class_paths(choice):
                    key, label = next(
                        (k, label)
                        for k, (label, cp, _) in OPTIONS.items()
                        if cp == class_path
                    )
                    print(f"\nRunning {key} ({label})...")
                    run_pipe(class_path, debug)
            else:
                label, class_path, _ = OPTIONS[choice]
                print(f"\nRunning {choice} ({label})...")
                run_pipe(class_path, debug)

        try:
            input("\n\x1b[0;32mDone! Press Enter to return to the menu.\x1b[0m")
        except KeyboardInterrupt:
            sys.exit(0)
        clear()


if __name__ == "__main__":
    main()
