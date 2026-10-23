import json
import unittest
from release_artifacts import build_bom


class ArtifactTests(unittest.TestCase):
    def test_sbom_has_graph_checksums_and_no_local_paths(self):
        packages = [{"id": "root", "name": "bridge", "version": "0.1.0", "license": "MIT", "manifest_path": "/private/secret/Cargo.toml"},
                    {"id": "dependency", "name": "dep", "version": "1.0.0", "license": "MIT OR Apache-2.0"}]
        metadata = {"packages": packages, "resolve": {"nodes": [{"id": "root", "dependencies": ["dependency"]}, {"id": "dependency", "dependencies": []}]}}
        lock = b'[[package]]\nname = "dep"\nversion = "1.0.0"\nchecksum = "abcd"\n'
        bom = build_bom(metadata, lock)
        self.assertEqual(bom["bomFormat"], "CycloneDX")
        self.assertEqual(bom["components"][1]["hashes"][0]["content"], "abcd")
        self.assertNotIn("/private", json.dumps(bom))
        self.assertEqual(bom["dependencies"][0]["dependsOn"], ["pkg:cargo/dep@1.0.0"])


if __name__ == "__main__":
    unittest.main()
