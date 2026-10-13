import os
from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader

_TARGET_CONNECTIONS = Path(
    os.environ.get("OGG_TARGET_CONNECTIONS") or
    Path(__file__).parent.parent.parent / "cdc-controller" / "config" / "inventory_connections" / "target_connections.yaml"
)

# Maps target_connections.yaml field names → template variable names per template.
# Fields absent from the map are passed through unchanged.
_TARGET_FIELD_MAP = {
    "s3": {
        "sts_assume_role":      "s3_sts_assume_role",
        "sts_session_name":     "s3_sts_session_name",
        "ogg_alias_credential": "s3_credentials_alias",
        "s3_bucket_path":       "bucket_path_prefix",
    },
    "kafka_producer":    {},  # all fields already match target_connections field names
    "kafka_confluent":   {},  # cluster_name, identity_pool come through unchanged
    "kafka_client_jaas": {},  # client_id, api_scope come through unchanged
}

# ---- Template registry ----

TEMPLATE_MAP = {
    "extract": {
        "oracle_integrated":   "oracle_integrated.prm.j2",
        "oracle_classic":      "oracle_classic.prm.j2",
        "oracle_initial_load": "oracle_initial_load.prm.j2",
        "mongo_classic":       "mongo_classic.prm.j2",
    },
    "replicat": {
        "oracle_classic":      "replicat_classic.prm.j2",
        "oracle_coordinated":  "replicat_coordinated.prm.j2",
    },
}

PROPERTIES_TEMPLATE_MAP = {
    # Per-replicat handler properties (replicat properties set --template / replicat create)
    "kafka_confluent":   "replicat/kafka_confluent.properties.j2",
    "s3":                "replicat/s3.properties.j2",
    # Shared config files (config-file create --template — one per cluster+identity, shared by replicats)
    "kafka_producer":    "replicat/kafka_producer.properties.j2",
    "kafka_client_jaas": "replicat/kafka_client_jaas.conf.j2",
}

# Variable contract for each properties template.
# Used for discoverability (`templates show <name>`) and fail-fast validation
# at render time (clearer than a Jinja UndefinedError on a missing variable).
TEMPLATE_VARS = {
    "kafka_confluent": {
        "kind":        "replicat properties",
        "description": "Confluent Kafka Connect handler — sends operations to a Kafka topic via the kafkaconnect handler",
        "required":    ["kafka_topic_prefix", "database", "schema", "cluster_name", "identity_pool"],
        "optional":    {
            "ogg_home":      "/u01/ogg/ogg23ai",
            "ogg_data_root": "/u01/ogg/data",
        },
        "auto_injected": ["replicat_name", "deployment_name", "env", "db_engine"],
    },
    "s3": {
        "kind":        "replicat properties",
        "description": "AWS S3 event handler — writes Parquet files to an S3 bucket via STS-assumed role",
        "required":    [
            "s3_bucket_name", "s3_sts_assume_role", "s3_sts_session_name",
            "s3_credentials_alias", "database", "bucket_path_prefix",
        ],
        "optional":    {
            "aws_region":            "eu-west-1",
            "s3_credentials_domain": "OracleGoldenGate",
            "ogg_staging_root":      "/u01/ogg/data_staging",
            "ogg_home":              "/u01/ogg/ogg23ai",
        },
        "auto_injected": ["replicat_name"],
    },
    "kafka_producer": {
        "kind":        "config file",
        "description": "Kafka producer + Schema Registry config (broker connection, OAuth, converters)",
        "required":    [
            "bootstrap_servers", "oauth_endpoint_url", "api_scope",
            "schema_registry_url", "cluster_id", "identity_pool",
            "ogg_alias_credential",
        ],
        "optional":    {
            "kafka_oauth_domain": "OracleGoldenGate",
        },
        "auto_injected": ["env"],
    },
    "kafka_client_jaas": {
        "kind":        "config file",
        "description": "JAAS config for Kafka OAUTHBEARER client-credentials flow (clientSecret is plaintext — product limitation)",
        "required":    ["client_id", "client_secret", "api_scope"],
        "optional":    {},
        "auto_injected": [],
    },
}


def validate_template_vars(template_name: str, context: dict):
    """Raise ValueError if any required variable is missing from context.
    Auto-injected variables count as 'present'."""
    if template_name not in TEMPLATE_VARS:
        return  # no contract registered — skip
    spec = TEMPLATE_VARS[template_name]
    missing = [k for k in spec["required"] if k not in context or context.get(k) in (None, "")]
    if missing:
        raise ValueError(
            f"Template '{template_name}' is missing required variable(s): {', '.join(missing)}. "
            f"Pass them via --vars-json or --target-vars-json."
        )

def load_target_connection(target_name: str) -> dict:
    """Return the merged (defaults + entry) dict for a target from target_connections.yaml."""
    if not _TARGET_CONNECTIONS.exists():
        raise ValueError(
            f"Target connections file not found at {_TARGET_CONNECTIONS}. "
            "Set OGG_TARGET_CONNECTIONS env var to override."
        )
    data = yaml.safe_load(_TARGET_CONNECTIONS.read_text())
    defaults = data.get("defaults", {})
    if target_name not in data:
        available = ", ".join(k for k in data if k != "defaults")
        raise ValueError(f"Unknown target '{target_name}'. Available: {available}")
    return {**defaults, **data[target_name]}


def apply_target_field_map(template_name: str, target_data: dict) -> dict:
    """Translate target_connections field names to the variable names expected by the template."""
    field_map = _TARGET_FIELD_MAP.get(template_name, {})
    return {field_map.get(k, k): v for k, v in target_data.items()}


# ---- Jinja environment ----

_TEMPLATES_DIR = Path(__file__).parent.parent / "templates"

_env = Environment(
    loader=FileSystemLoader(str(_TEMPLATES_DIR)),
    trim_blocks=True,
    lstrip_blocks=True,
)

# ---- Public API ----

def build_prm_lines(resource: str, profile: str, context: dict):
    if resource not in TEMPLATE_MAP:
        raise ValueError(f"Unknown resource '{resource}'")

    if profile not in TEMPLATE_MAP[resource]:
        raise ValueError(f"Unknown profile '{profile}' for resource '{resource}'")

    template_name = TEMPLATE_MAP[resource][profile]
    template_path = f"{resource}/{template_name}"

    template = _env.get_template(template_path)
    rendered = template.render(context)

    return [line.rstrip() for line in rendered.splitlines()]


def build_properties_lines(template_name: str, context: dict):
    """Render a Big Data properties template (s3, kafka_confluent, ...) and return its lines."""
    if template_name not in PROPERTIES_TEMPLATE_MAP:
        available = ", ".join(sorted(PROPERTIES_TEMPLATE_MAP))
        raise ValueError(f"Unknown properties template '{template_name}'. Available: {available}")

    validate_template_vars(template_name, context)

    template_path = PROPERTIES_TEMPLATE_MAP[template_name]
    template = _env.get_template(template_path)
    rendered = template.render(context)

    return [line.rstrip() for line in rendered.splitlines()]

# ---- Helpers ----

def get_useridalias_from_prm(prm_lines):
    for line in prm_lines:
        line = line.strip()
        if line.upper().startswith("USERIDALIAS"):
            parts = line.split()
            if len(parts) >= 2:
                return parts[1]
    return None


def get_sourcecatalog_from_prm(prm_lines):
    for line in prm_lines:
        line = line.strip()
        if line.upper().startswith("SOURCECATALOG"):
            parts = line.split()
            if len(parts) >= 2:
                return parts[1]
    return None
