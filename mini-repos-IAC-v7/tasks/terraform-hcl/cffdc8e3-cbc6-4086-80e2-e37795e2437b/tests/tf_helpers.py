import re
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAIN_TF = ROOT / "main.tf"
VARIABLES_TF = ROOT / "variables.tf"


BLOCK_PATTERNS = {
    "provider": re.compile(r'^\s*provider\s+"(?P<type>[^"]+)"\s*{'),
    "variable": re.compile(r'^\s*variable\s+"(?P<name>[^"]+)"\s*{'),
    "resource": re.compile(r'^\s*resource\s+"(?P<type>[^"]+)"\s+"(?P<name>[^"]+)"\s*{'),
    "data": re.compile(r'^\s*data\s+"(?P<type>[^"]+)"\s+"(?P<name>[^"]+)"\s*{'),
}


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def main_text() -> str:
    return read_text(MAIN_TF)


def combined_text() -> str:
    parts = [main_text()]
    if VARIABLES_TF.exists():
        parts.append(read_text(VARIABLES_TF))
    return "\n".join(parts)


def _heredoc_label(line: str):
    match = re.search(r"<<-?(?P<label>[A-Za-z0-9_]+)", line)
    return match.group("label") if match else None


def _collect_blocks(text: str, kind: str):
    pattern = BLOCK_PATTERNS[kind]
    lines = text.splitlines()
    results = []
    index = 0

    while index < len(lines):
        match = pattern.match(lines[index])
        if not match:
            index += 1
            continue

        metadata = match.groupdict()
        start = index
        depth = lines[index].count("{") - lines[index].count("}")
        heredoc = _heredoc_label(lines[index])
        index += 1

        while index < len(lines) and depth > 0:
            line = lines[index]
            if heredoc:
                if line.strip() == heredoc:
                    heredoc = None
                index += 1
                continue

            heredoc = _heredoc_label(line)
            depth += line.count("{") - line.count("}")
            index += 1

        results.append(
            {
                "type": metadata.get("type"),
                "name": metadata.get("name"),
                "text": "\n".join(lines[start:index]),
            }
        )

    return results


def blocks(kind: str, type_name=None, name=None):
    source_text = read_text(VARIABLES_TF) if kind == "variable" else main_text()
    matches = _collect_blocks(source_text, kind)
    if type_name is not None:
        matches = [item for item in matches if item["type"] == type_name]
    if name is not None:
        matches = [item for item in matches if item["name"] == name]
    return [item["text"] for item in matches]


def block(kind: str, type_name=None, name=None) -> str:
    matches = blocks(kind, type_name=type_name, name=name)
    assert len(matches) == 1, f"Expected exactly one {kind} block for {type_name}.{name}, found {len(matches)}"
    return matches[0]


def resource_blocks(type_name: str):
    return blocks("resource", type_name=type_name)


def resource_block(type_name: str, name: str) -> str:
    return block("resource", type_name=type_name, name=name)


def only_resource_block(type_name: str) -> str:
    matches = resource_blocks(type_name)
    assert len(matches) == 1, f"Expected exactly one resource block for {type_name}, found {len(matches)}"
    return matches[0]


def data_block(type_name: str, name: str) -> str:
    return block("data", type_name=type_name, name=name)


def variable_block(name: str) -> str:
    return block("variable", name=name)


def variable_names():
    return {item["name"] for item in _collect_blocks(read_text(VARIABLES_TF), "variable")}


def assignment(block_text: str, attr: str):
    match = re.search(rf"^\s*{re.escape(attr)}\s*=\s*(?P<value>.+)$", block_text, re.MULTILINE)
    if not match:
        return None
    value = match.group("value").strip()
    return re.sub(r"\s+#.*$", "", value).strip()


def contains(block_text: str, pattern: str) -> bool:
    return re.search(pattern, block_text, re.MULTILINE | re.DOTALL) is not None


def run_terraform(*args):
    return subprocess.run(
        ["terraform", *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
