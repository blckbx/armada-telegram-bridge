"""Generate a source-graph CycloneDX SBOM from locked Cargo metadata.

No credentials, runtime state, absolute local paths, or environment are emitted.
This is not an image SBOM or provenance attestation.
"""
import hashlib
import json
from pathlib import Path
import subprocess
import tomllib


def build_bom(metadata, lock_bytes):
    lock = tomllib.loads(lock_bytes.decode())
    checksums = {(p["name"], p["version"]): p.get("checksum") for p in lock["package"]}
    refs = {p["id"]: f"pkg:cargo/{p['name']}@{p['version']}" for p in metadata["packages"]}
    components = []
    for package in sorted(metadata["packages"], key=lambda p: (p["name"], p["version"])):
        component = {"type": "library", "name": package["name"], "version": package["version"],
                     "bom-ref": refs[package["id"]], "purl": refs[package["id"]]}
        if package.get("license"):
            component["licenses"] = [{"expression": package["license"]}]
        checksum = checksums.get((package["name"], package["version"]))
        if checksum:
            component["hashes"] = [{"alg": "SHA-256", "content": checksum}]
        components.append(component)
    return {"bomFormat": "CycloneDX", "specVersion": "1.5", "version": 1,
            "metadata": {"properties": [{"name": "bridge:Cargo.lock:sha256", "value": hashlib.sha256(lock_bytes).hexdigest()},
                                        {"name": "bridge:scope", "value": "Cargo source graph; includes dev/build/platform dependencies; not runtime image"}]},
            "components": components,
            "dependencies": [{"ref": refs[node["id"]], "dependsOn": sorted({refs[dep] for dep in node["dependencies"]})}
                             for node in sorted(metadata["resolve"]["nodes"], key=lambda n: refs[n["id"]])]}


if __name__ == "__main__":
    metadata = json.loads(subprocess.check_output(["cargo", "metadata", "--locked", "--offline", "--format-version", "1"]))
    output = Path("release-artifacts")
    output.mkdir(mode=0o700, exist_ok=True)
    bom = build_bom(metadata, Path("Cargo.lock").read_bytes())
    (output / "cargo-source.cdx.json").write_text(json.dumps(bom, indent=2) + "\n")
    notices = []
    for package in sorted(metadata["packages"], key=lambda p: (p["name"], p["version"])):
        directory = Path(package["manifest_path"]).parent
        notices.append(f"\n=== {package['name']} {package['version']} ({package.get('license') or 'unspecified'}) ===\n")
        for candidate in sorted(directory.iterdir()):
            if candidate.is_file() and candidate.name.upper().startswith(("LICENSE", "LICENCE", "COPYING", "NOTICE", "UNLICENSE")):
                notices.append(candidate.read_text(errors="replace"))
    (output / "THIRD_PARTY_NOTICES.txt").write_text("\n".join(notices))
    print(f"Generated source SBOM: {len(bom['components'])} components; no image attestation claimed")
