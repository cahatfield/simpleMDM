import tempfile
import unittest
from pathlib import Path

from domain_contract_generator import contract
from domain_schema_generator import domain


class MutationBoundaryTests(unittest.TestCase):
    def test_domain_requires_save(self):
        root = Path(tempfile.mkdtemp())
        d = domain("BoundaryDomain", destination=root)
        d.create().add_key("code")

        schema_path = root / "BoundaryDomain" / "BoundaryDomain.schema.json"
        self.assertFalse(schema_path.exists())
        self.assertTrue(d.is_dirty)

        d.save()

        self.assertTrue(schema_path.exists())
        self.assertFalse(d.is_dirty)

    def test_contract_requires_save(self):
        root = Path(tempfile.mkdtemp())
        cwd = Path.cwd()
        try:
            # Contract generator currently uses domains/ relative to CWD.
            # Switch to temp directory to isolate test artifacts.
            import os

            os.chdir(root)
            c = contract("BoundaryDomain")
            c.create().set_status("active")

            contract_path = root / "domains" / "BoundaryDomain" / "BoundaryDomain.contract.json"
            self.assertFalse(contract_path.exists())
            self.assertTrue(c.is_dirty)

            c.save()

            self.assertTrue(contract_path.exists())
            self.assertFalse(c.is_dirty)
        finally:
            import os

            os.chdir(cwd)


if __name__ == "__main__":
    unittest.main()
