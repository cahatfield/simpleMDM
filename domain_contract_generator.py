
import json
import pathlib
from datetime import datetime


class contract:
    def __init__(self, name: str):
        self.name = name

    def _contract_file(self) -> pathlib.Path:
        return pathlib.Path("domains") / self.name / f"{self.name}.contract.json"

    def _read(self) -> dict:
        return json.loads(self._contract_file().read_text())

    def _write(self, data: dict):
        self._contract_file().write_text(json.dumps(data, indent=2))

    def create(self):
        """Initialise a new data contract for the domain."""
        folder = pathlib.Path("domains") / self.name
        folder.mkdir(parents=True, exist_ok=True)

        data = {
            "contract_version": "1.0.0",
            "domain": self.name,
            "schema_ref": f"{self.name}.schema.json",
            "status": "draft",
            "owner": {
                "name": "",
                "team": "",
                "email": "",
            },
            "consumers": [],
            "sla": {
                "freshness": "",
                "availability": "",
            },
            "quality_rules": {},
            "changelog": [
                {
                    "version": "1.0.0",
                    "date": datetime.utcnow().strftime("%Y-%m-%d"),
                    "description": f"Initial contract for {self.name} domain",
                }
            ],
        }

        self._write(data)
        return self

    def set_owner(self, name: str, team: str, email: str):
        """Set the data owner for this domain.

        Args:
            name:  Full name of the data owner.
            team:  Team responsible for this domain.
            email: Contact email for the owner.
        """
        data = self._read()
        data["owner"] = {"name": name, "team": team, "email": email}
        self._write(data)
        return self

    def set_sla(self, freshness: str, availability: str):
        """Define the SLA for this domain.

        Args:
            freshness:    How often data is updated (e.g. 'daily', 'hourly', 'realtime').
            availability: Uptime target (e.g. '99.9%').
        """
        data = self._read()
        data["sla"] = {"freshness": freshness, "availability": availability}
        self._write(data)
        return self

    def add_consumer(self, name: str, team: str):
        """Register a consumer of this domain's data.

        Args:
            name: Name of the consuming system or person.
            team: Team that owns the consumer.
        """
        data = self._read()
        consumer = {"name": name, "team": team}
        if consumer not in data["consumers"]:
            data["consumers"].append(consumer)
        self._write(data)
        return self

    def add_quality_rule(self, table: str, field: str, **rules):
        """Add a quality rule for a field on a specific table.

        Args:
            table:   Table name (e.g. 'Customer', 'Customer_Status').
            field:   Field name the rule applies to.
            **rules: One or more rule constraints, e.g.:
                     pattern="^[\\w.-]+@[\\w.-]+\\.[a-z]{2,}$"
                     min_length=1, max_length=100
                     min_value=0, max_value=999
                     not_null=True
                     unique=True
                     allowed_values=["active", "inactive"]
        """
        data = self._read()
        data["quality_rules"].setdefault(table, {}).setdefault(field, {}).update(rules)
        self._write(data)
        return self

    def set_status(self, status: str):
        """Set the contract lifecycle status.

        Args:
            status: One of 'draft', 'active', 'deprecated'.
        """
        valid = ("draft", "active", "deprecated")
        if status not in valid:
            raise ValueError(f"status must be one of {valid}, got {status!r}")
        data = self._read()
        data["status"] = status
        self._write(data)
        return self

    def add_changelog(self, version: str, description: str):
        """Append a changelog entry.

        Args:
            version:     Semantic version string (e.g. '1.1.0').
            description: What changed in this version.
        """
        data = self._read()
        data["contract_version"] = version
        data["changelog"].append({
            "version": version,
            "date": datetime.utcnow().strftime("%Y-%m-%d"),
            "description": description,
        })
        self._write(data)
        return self
