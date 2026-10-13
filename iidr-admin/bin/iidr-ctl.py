#!/usr/bin/python3.12
"""
iidr-ctl.py — Wrapper Python para iidr-admin CLI Java.

Modos de uso:
  (A) CLI directo    iidr-ctl.py <entity> <operation> [--opt val ...]
  (B) JSON inline    iidr-ctl.py --json '{"command":"subscription.start","params":{...}}'
  (C) JSON stdin     echo '{"command":"datastore.list"}' | iidr-ctl.py --stdin
  (D) JSON fichero   iidr-ctl.py --json-file /path/payload.json

Salida: siempre JSON por stdout, logs por stderr. Exit code 0 o 1.
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Resolución de rutas
# ---------------------------------------------------------------------------

def resolve_root() -> Path:
    script_dir = Path(__file__).resolve().parent
    candidate = script_dir.parent
    # Modo NFS: lib/iidr-admin.jar
    if (candidate / "lib" / "iidr-admin.jar").exists():
        return candidate
    # Modo desarrollo: target/iidr-admin-1.0-SNAPSHOT.jar
    if (candidate / "target" / "iidr-admin-1.0-SNAPSHOT.jar").exists():
        return candidate
    return candidate


def find_jar(root: Path) -> Path:
    for candidate in [
        root / "lib" / "iidr-admin.jar",
        root / "target" / "iidr-admin-1.0-SNAPSHOT.jar",
    ]:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"No se encontró iidr-admin.jar ni iidr-admin-1.0-SNAPSHOT.jar bajo {root}. "
        "Compila con: mvn -DskipTests package"
    )


def build_classpath(root: Path, iidr_lib: Path) -> str:
    jar_app = find_jar(root)
    jar_server = iidr_lib / "server.jar"
    jar_chcclp = iidr_lib / "chcclp.jar"

    for jar in [jar_server, jar_chcclp]:
        if not jar.exists():
            raise FileNotFoundError(
                f"IIDR jar no encontrado: {jar}. "
                "Comprueba IIDR_HOME."
            )

    parts = [str(jar_app), str(jar_server), str(jar_chcclp)]

    # Capa 1: NFS — deps copiados por deploy.sh
    deps_dir = root / "lib" / "deps"
    if deps_dir.is_dir():
        for dep in sorted(deps_dir.glob("*.jar")):
            parts.append(str(dep))
        return ":".join(parts)

    # Capa 2: desarrollo — classpath.txt de Maven
    cp_file = root / "target" / "classpath.txt"
    if cp_file.exists():
        maven_cp = cp_file.read_text().strip()
        for entry in maven_cp.split(":"):
            if entry and "server.jar" not in entry and "chcclp.jar" not in entry:
                parts.append(entry)

    return ":".join(parts)


# ---------------------------------------------------------------------------
# Traducción JSON → argumentos CLI Java
# ---------------------------------------------------------------------------

COMMAND_PARAM_MAP = {
    "datastore.list":   [],
    "datastore.show":   ["--name"],
    "datastore.create": ["--name", "--host", "--port", "--description"],
    "datastore.delete": ["--name"],
    "datastore.grant":  ["--name", "--user"],

    "subscription.list":   ["--source"],
    "subscription.show":   ["--source", "--name"],
    "subscription.create": ["--source", "--target", "--name", "--description"],
    "subscription.delete": ["--source", "--target", "--name"],
    "subscription.start":  ["--source", "--name"],
    "subscription.stop":   ["--source", "--name", "--method"],
    "subscription.lock":   ["--source", "--target", "--name"],
    "subscription.unlock": ["--source", "--target", "--name"],
    "subscription.reinit": ["--source", "--name"],
    "subscription.setup-kafka": ["--source", "--target", "--name", "--tables",
                                 "--commit-topic", "--properties-path",
                                 "--kcop-classname"],

    "mapping.list":          ["--source", "--target", "--subscription"],
    "mapping.add":           ["--source", "--target", "--subscription",
                              "--sourceSchema", "--sourceTable",
                              "--targetSchema", "--targetTable"],
    "mapping.remove":        ["--source", "--target", "--subscription",
                              "--sourceSchema", "--sourceTable"],
    "mapping.find-by-table": ["--table", "--live-check"],
    "mapping.refresh":       ["--source", "--target", "--subscription",
                              "--sourceSchema", "--sourceTable",
                              "--action", "--method"],

    "composite.setup":        ["--config"],
    "composite.teardown":     ["--subscription", "--source", "--target",
                               "--delete-datastores"],
    "composite.restart":      ["--subscription", "--source", "--timeout"],
    "composite.health-check": ["--source"],
}

BOOLEAN_FLAGS = {"--live-check", "--delete-datastores"}


def _flag_to_key(flag: str) -> str:
    """'--sourceSchema' → 'sourceSchema', '--delete-datastores' → 'delete-datastores'"""
    return flag.lstrip("-")


def _kebab_to_camel(s: str) -> str:
    parts = s.split("-")
    return parts[0] + "".join(p.capitalize() for p in parts[1:])


def json_to_cli_args(payload: dict) -> list:
    command = payload.get("command", "").strip()
    if not command or "." not in command:
        raise ValueError(
            f"'command' inválido o ausente: {command!r}. "
            "Formato esperado: 'entity.operation'"
        )
    if command not in COMMAND_PARAM_MAP:
        valid = ", ".join(sorted(COMMAND_PARAM_MAP.keys()))
        raise ValueError(f"Comando desconocido: {command!r}. Válidos:\n  {valid}")

    entity, operation = command.split(".", 1)
    args = [entity, operation]

    params = payload.get("params") or {}
    if not isinstance(params, dict):
        raise ValueError("'params' debe ser un objeto JSON")

    for flag in COMMAND_PARAM_MAP[command]:
        key = _flag_to_key(flag)
        value = params.get(key)
        if value is None:
            value = params.get(_kebab_to_camel(key))
        if value is None:
            continue
        if flag in BOOLEAN_FLAGS:
            if value:
                args.append(flag)
        else:
            args.extend([flag, str(value)])

    return args


# ---------------------------------------------------------------------------
# Traductor de payload "Zeus / cdc-controller" → flujo Kafka IIDR
#
# La plataforma Zeus de Mapfre pide replicar unas tablas de una BD hacia
# Kafka. La petición llega a cdc-controller como payload JSON; cdc-controller
# despacha por bbdd_engine (ORACLE/MONGO → OGG, DB2 → IIDR = nosotros).
# Este traductor recibe ese mismo payload y lo convierte en la secuencia de
# comandos iidr.sh que arranca la replicación:  create → setup-kafka → start.
#
# Resolución de config (calcada del patrón OGG en cdc-controller/utils.py):
#   - conexión origen : clave directa  <BBDD_NAME>_<ENV>  en db_connections.yaml
#   - datastore IIDR  : filtro env+db_engine=DB2 sobre inventory.yaml (como
#                       get_matching_deployments de OGG; 'project' NO es pivote)
#   - target Kafka    : entrada KAFKA_*_<ENV> en target_connections.yaml
#                       (SR url/id, bootstrap_servers, identity_pool, OAuth...)
# Los inventarios de cdc-controller son SOLO LECTURA para nosotros.
# ---------------------------------------------------------------------------

# Ruta a los inventarios de cdc-controller (hermano en el mismo repo).
# Override para pruebas: IIDR_CDC_CONFIG_DIR apunta a un config alternativo.
def cdc_config_dir(root: Path) -> Path:
    override = os.environ.get("IIDR_CDC_CONFIG_DIR")
    if override:
        return Path(override)
    # root = iidr-admin/ ; el repo es su padre; cdc-controller/config al lado.
    return root.parent / "cdc-controller" / "config"


def _load_yaml(path: Path) -> dict:
    try:
        import yaml
    except ImportError:
        die("PyYAML no instalado (necesario para leer inventarios). pip install pyyaml")
    if not path.is_file():
        die(f"Inventario no encontrado: {path}")
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _merge_defaults(entry: dict, doc: dict) -> dict:
    """Fusiona la sección 'defaults' del YAML en la entrada (entry gana)."""
    defaults = doc.get("defaults") or {}
    if not isinstance(defaults, dict):
        defaults = {}
    return {**defaults, **entry}


def get_db_connection(config_dir: Path, name: str):
    """<BBDD_NAME>_<ENV> → dict de db_connections.yaml (defaults fusionados)."""
    if name == "defaults":
        return None
    doc = _load_yaml(config_dir / "inventory_connections" / "db_connections.yaml")
    entry = doc.get(name)
    return _merge_defaults(entry, doc) if isinstance(entry, dict) else None


def get_target_connection(config_dir: Path, name: str):
    """<TARGET>_<ENV> → dict de target_connections.yaml (defaults fusionados)."""
    if name == "defaults":
        return None
    doc = _load_yaml(config_dir / "inventory_connections" / "target_connections.yaml")
    entry = doc.get(name)
    return _merge_defaults(entry, doc) if isinstance(entry, dict) else None


def get_matching_deployments(config_dir: Path, env: str, db_engine: str,
                             proc_types=None):
    """Filtra inventory.yaml por env + db_engine (+ proc_type), patrón OGG.

    Devuelve lista de (nombre, dict). 'project' no es pivote: un deployment
    puede servir a varios proyectos del mismo entorno.
    """
    doc = _load_yaml(config_dir / "inventory_deployments" / "inventory.yaml")
    wanted_pt = set(proc_types) if proc_types else None

    def _to_set(raw):
        if raw is None:
            return set()
        if isinstance(raw, str):
            return {raw}
        if isinstance(raw, (list, tuple, set)):
            return {str(x) for x in raw}
        return set()

    matches = []
    for name, dep in doc.items():
        if name == "defaults" or not isinstance(dep, dict):
            continue
        if dep.get("env") != env or dep.get("db_engine") != db_engine:
            continue
        if wanted_pt is not None and not (wanted_pt & _to_set(dep.get("proc_type"))):
            continue
        matches.append((name, dep))
    return matches


def parse_tables(raw_tables) -> list:
    """['REAL.TB030T001(*)', 'REAL.TB030T002(COL1,COL2)'] → ['REAL.TB030T001', ...]

    Esta iteración mapea la TABLA ENTERA; las columnas explícitas del payload
    se ignoran (TODO: soporte de columnas vía 'map column' de CHCCLP).
    """
    if not raw_tables:
        return []
    out = []
    for item in raw_tables:
        name = str(item).strip()
        # quitar el sufijo de columnas '(...)' si viene
        paren = name.find("(")
        if paren != -1:
            name = name[:paren].strip()
        if not name:
            continue
        if "." not in name:
            die(f"Tabla sin esquema (esperado ESQUEMA.TABLA): {item!r}")
        out.append(name)
    return out


# Prefijo de topic Kafka por defecto (destino bronze del piloto Golden Records).
DEFAULT_TOPIC_PREFIX = "esp.int.cac.bronze.grcdcpiloto"
# Datastore IIDR: convención de fallback si inventory.yaml aún no trae la
# entrada IIDR DB2 (el compañero la mergea; asumimos su esquema).
DEFAULT_KCOP_CLASSNAME = (
    "com.datamirror.ts.target.publication.userexit.sample.kafka."
    "KcopSingleRowAvroAuditIntegrated"
)


def is_zeus_payload(payload: dict) -> bool:
    """Distingue el payload de Zeus del formato {command,params}.

    Zeus trae bbdd_engine/tables/target_engine y NO trae 'command'.
    """
    if not isinstance(payload, dict) or payload.get("command"):
        return False
    return bool(payload.get("bbdd_engine") or payload.get("tables")
                or payload.get("target_engine"))


def resolve_zeus_config(payload: dict, config_dir: Path) -> dict:
    """Del payload Zeus + inventarios de cdc-controller → config del flujo.

    Devuelve un dict con todo lo que necesitan create/setup-kafka:
      env, source_ds, target_ds, tables[], topic_prefix, database,
      audit_record_name, kcop_classname y los datos del Schema Registry.
    """
    env = (payload.get("env") or "").upper()
    if env not in ("DEV", "PRE", "PRO"):
        die(f"env inválido o ausente en el payload: {env!r} (esperado DEV/PRE/PRO)")

    bbdd_engine = (payload.get("bbdd_engine") or "").upper()
    if bbdd_engine != "DB2":
        die(f"bbdd_engine {bbdd_engine!r} no es DB2 — este payload no es para IIDR")

    bbdd_name = (payload.get("bbdd_name") or "").upper()
    if not bbdd_name:
        die("bbdd_name ausente en el payload")

    tables = parse_tables(payload.get("tables"))
    if not tables:
        die("El payload no trae tablas ('tables')")

    # --- conexión origen: clave directa <BBDD_NAME>_<ENV> (patrón OGG) -------
    db_conn = get_db_connection(config_dir, f"{bbdd_name}_{env}")
    if db_conn is None:
        die(f"Conexión '{bbdd_name}_{env}' no encontrada en db_connections.yaml")

    # --- datastore IIDR origen: filtro env+db_engine=DB2 en inventory.yaml ---
    # proc_type 'extract' = lado origen (análogo a OGG). Si el compañero aún no
    # ha mergeado la entrada IIDR, esto devuelve [] y avisamos claramente.
    src_deps = get_matching_deployments(config_dir, env, "DB2",
                                        proc_types=["extract"])
    if not src_deps:
        die(f"Sin deployment IIDR DB2 origen (proc_type=extract) para env={env} "
            f"en inventory.yaml. ¿Falta mergear la entrada IIDR DB2?")
    source_ds = src_deps[0][1].get("deployment") or src_deps[0][0]

    # --- datastore IIDR destino (Kafka): proc_type 'replicat' ---------------
    tgt_deps = get_matching_deployments(config_dir, env, "DB2",
                                        proc_types=["replicat"])
    if not tgt_deps:
        die(f"Sin deployment IIDR DB2 destino (proc_type=replicat) para env={env} "
            f"en inventory.yaml. ¿Falta mergear la entrada IIDR DB2?")
    target_ds = tgt_deps[0][1].get("deployment") or tgt_deps[0][0]

    # --- target Kafka: SR + OAuth + bootstrap desde target_connections.yaml -
    target_name = payload.get("target_name") or f"KAFKA_GR_01_{env}"
    tgt_conn = get_target_connection(config_dir, target_name)
    if tgt_conn is None:
        die(f"Target Kafka '{target_name}' no encontrado en target_connections.yaml")

    # Secretos OAuth del Schema Registry: NUNCA del inventario (solo referencia
    # el alias). Vienen por env vars (AWS SM), igual que el resto del flujo.
    sr_client_id     = os.environ.get("KAFKA_CLIENT_ID")     or tgt_conn.get("client_id")
    sr_client_secret = os.environ.get("KAFKA_CLIENT_SECRET")
    sr_pool_id       = os.environ.get("KAFKA_POOL_ID")       or tgt_conn.get("identity_pool")

    return {
        "env":               env,
        "source_ds":         source_ds,
        "target_ds":         target_ds,
        "tables":            tables,
        "topic_prefix":      payload.get("topic_prefix") or DEFAULT_TOPIC_PREFIX,
        "database":          bbdd_name,
        "audit_record_name": payload.get("audit_record_name") or "DB2",
        "kcop_classname":    payload.get("kcop_classname") or DEFAULT_KCOP_CLASSNAME,
        "project":           payload.get("project") or "",
        "request_type":      (payload.get("request_type") or "CREATE").upper(),
        # Schema Registry / OAuth (para render del .properties)
        "sr_url":            tgt_conn.get("schema_registry_url"),
        "sr_issuer_url":     tgt_conn.get("oauth_endpoint_url"),
        "sr_scope":          tgt_conn.get("api_scope"),
        "sr_logical_cluster": tgt_conn.get("schema_registry_id"),
        "sr_client_id":      sr_client_id,
        "sr_client_secret":  sr_client_secret,
        "sr_pool_id":        sr_pool_id,
        "bootstrap_servers": tgt_conn.get("bootstrap_servers"),
    }


def render_kcop_properties(cfg: dict) -> str:
    """Renderiza el <SUB>.properties del KCOP (mismo formato que kcop.properties.j2).

    4 partes: (1) Schema Registry+OAuth, (2) mapeo tabla→topic, (3) envelope
    AVRO + campos journal, (4) transformación de esquema AVRO por tabla.
    """
    kafka_ds_l = cfg["target_ds"].lower()
    sub_l      = cfg["sub_name"].lower()
    lines = []

    lines += [
        "# =====================================================================",
        "# PARTE 1 — Schema Registry (conexión y autenticación OAuth)",
        "# =====================================================================",
        f"schema.registry.url={cfg['sr_url']}",
        "",
        '# El KCOP usa el prefijo "serializer.property." para pasar estas variables',
        "# como propiedad al cliente del Schema Registry.",
        "serializer.property.bearer.auth.credentials.source=OAUTHBEARER",
        f"serializer.property.bearer.auth.issuer.endpoint.url={cfg['sr_issuer_url']}",
        f"serializer.property.bearer.auth.client.id={cfg['sr_client_id']}",
        f"serializer.property.bearer.auth.client.secret={cfg['sr_client_secret']}",
        f"serializer.property.bearer.auth.scope={cfg['sr_scope']}",
        f"serializer.property.bearer.auth.logical.cluster={cfg['sr_logical_cluster']}",
        f"serializer.property.bearer.auth.identity.pool.id={cfg['sr_pool_id']}",
        "",
        "# =====================================================================",
        "# PARTE 2 — Mapeo de tablas de origen a topics de Kafka",
        "# =====================================================================",
    ]
    for t in cfg["tables"]:
        schema, table = t.split(".", 1)
        lines.append(
            f"MAP_TABLE_{kafka_ds_l}.{sub_l}.sourcedb.{schema.lower()}."
            f"{table.lower()}={cfg['topic_prefix']}.{table.lower()}_cdc"
        )

    lines += [
        "",
        "# =====================================================================",
        "# PARTE 3 — Envelope del esquema AVRO y campos de control (journal)",
        "# =====================================================================",
        "audit.jcfs=ENTTYP->opType,CCID->operationId,SEQNO->operationSeq,"
        "CNTRRN->operationPos,UTC_TIMESTAMP->opTs",
        f"audit.record.name={cfg['audit_record_name']}",
        "before.image.field.name=before",
        "after.image.field.name=after",
        "",
        "# =====================================================================",
        "# PARTE 4 — Transformación del esquema AVRO por cada tabla",
        "# =====================================================================",
    ]
    for t in cfg["tables"]:
        schema, table = t.split(".", 1)
        su, tu = schema.upper(), table.upper()
        lines.append(
            f"MODIFY_SCHEMA_value.SOURCEDB.{su}.{tu}_SET_NAMESPACE="
            f"{cfg['database']}.{su}.{tu}"
        )
        lines.append(f"MODIFY_SCHEMA_value.SOURCEDB.{su}.{tu}_SET_NAME=Row")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Ejecución de la JVM
# ---------------------------------------------------------------------------

def _build_java_cmd(java_args: list, root: Path, iidr_lib: Path,
                    conf_dir: Path) -> list:
    # Java del JRE embebido de IIDR, con fallback al del sistema
    java_bin = iidr_lib.parent / "jre64" / "jre" / "bin" / "java"
    if not java_bin.exists():
        java_bin = Path("java")

    classpath = build_classpath(root, iidr_lib)
    (root / "log").mkdir(exist_ok=True)

    trust_store = os.environ.get(
        "TLS_TRUST_STORE",
        str(conf_dir / "cdc_truststore_AccessServer.p12"),
    )
    trust_store_password = os.environ.get("TLS_TRUST_STORE_PASSWORD")
    if not trust_store_password:
        die("TLS_TRUST_STORE_PASSWORD no está definida. Expórtala antes de invocar este script.")
    trust_store_type = os.environ.get("TLS_TRUST_STORE_TYPE", "PKCS12")

    return [
        str(java_bin),
        f"-Dlogback.configurationFile={conf_dir}/logback.xml",
        "-Dlogback.statusListenerClass=ch.qos.logback.core.status.NopStatusListener",
        f"-Djavax.net.ssl.trustStore={trust_store}",
        f"-Djavax.net.ssl.trustStorePassword={trust_store_password}",
        f"-Djavax.net.ssl.trustStoreType={trust_store_type}",
        f"-Diidr.properties={conf_dir}/iidr.properties",
        "-cp", classpath,
        "com.mapfre.cdc.admin.IIDRAdmin",
    ] + java_args


def run_java(java_args: list, root: Path, iidr_lib: Path, conf_dir: Path) -> int:
    """Ejecuta un comando (modo 1-comando): hereda stdout/stderr en directo."""
    cmd = _build_java_cmd(java_args, root, iidr_lib, conf_dir)
    return subprocess.run(cmd, cwd=str(root)).returncode


def run_java_capture(java_args: list, root: Path, iidr_lib: Path,
                     conf_dir: Path) -> tuple:
    """Ejecuta un comando capturando salida (para el orquestador Zeus).

    Devuelve (exit_code, stdout_str, parsed_json_or_None). Los logs (stderr)
    se propagan a nuestro stderr para no perder trazabilidad.
    """
    cmd = _build_java_cmd(java_args, root, iidr_lib, conf_dir)
    proc = subprocess.run(cmd, cwd=str(root), text=True,
                          stdout=subprocess.PIPE, stderr=None)
    parsed = None
    if proc.stdout:
        try:
            parsed = json.loads(proc.stdout)
        except json.JSONDecodeError:
            parsed = None
    return proc.returncode, proc.stdout or "", parsed


# ---------------------------------------------------------------------------
# Orquestación del flujo Zeus → secuencia de comandos IIDR
# ---------------------------------------------------------------------------

# Cada petición de Zeus crea una suscripción nueva (autoincremental). En DEV
# ya hay subs manuales (DESACDC1..3) → arrancamos el contador en 6 por margen.
SUB_SEQ_START = {"DEV": 6, "PRE": 1, "PRO": 1}


def _next_subscription_name(cfg: dict, run_ctx: dict) -> str:
    """Calcula el siguiente nombre de sub libre: <BBDD_NAME>_<ENV>_<N>.

    Consulta 'subscription list' en el AS y busca el menor N libre a partir
    del arranque por entorno (SUB_SEQ_START). En dry-run no consulta el AS.
    """
    base = f"{cfg['database']}_{cfg['env']}"
    start = SUB_SEQ_START.get(cfg["env"], 1)

    if run_ctx.get("dry_run"):
        return f"{base}_{start}"

    rc, _out, parsed = run_java_capture(
        ["subscription", "list", "--source", cfg["source_ds"]],
        run_ctx["root"], run_ctx["iidr_lib"], run_ctx["conf_dir"],
    )
    existing = set()
    if parsed and parsed.get("status") == "OK":
        data = parsed.get("data") or []
        for sub in data:
            nm = sub.get("name") if isinstance(sub, dict) else str(sub)
            if nm:
                existing.add(nm)
    n = start
    while f"{base}_{n}" in existing:
        n += 1
    return f"{base}_{n}"


def build_zeus_sequence(cfg: dict) -> list:
    """cfg resuelta → lista de pasos (command, params) del flujo Kafka.

    Solo request_type=CREATE en esta iteración. Cada paso reutiliza un comando
    Java existente vía el formato {command, params}.
    """
    if cfg["request_type"] != "CREATE":
        die(f"request_type {cfg['request_type']!r} no soportado todavía "
            "(esta iteración solo cubre CREATE)")

    sub    = cfg["sub_name"]
    tables = ",".join(cfg["tables"])
    commit_topic = f"{cfg['topic_prefix']}.{sub}-commitstream"

    return [
        {"command": "subscription.create", "params": {
            "source": cfg["source_ds"], "target": cfg["target_ds"],
            "name": sub,
            "description": f"Zeus {cfg['project']} {cfg['env']} → Kafka",
        }},
        {"command": "subscription.setup-kafka", "params": {
            "source": cfg["source_ds"], "target": cfg["target_ds"],
            "name": sub, "tables": tables,
            "commit-topic": commit_topic,
            "properties-path": cfg["properties_path"],
            "kcop-classname": cfg["kcop_classname"],
        }},
        {"command": "subscription.start", "params": {
            "source": cfg["source_ds"], "name": sub,
        }},
    ]


def run_zeus_payload(payload: dict, run_ctx: dict) -> int:
    """Traduce y orquesta el flujo completo. Devuelve exit code (0 OK / 1 ERROR).

    Salida: un JSON agregado con el resultado de cada paso. Para en el primer
    ERROR. Con dry_run solo imprime la secuencia (no toca AS ni ficheros).
    """
    config_dir = cdc_config_dir(run_ctx["root"])
    cfg = resolve_zeus_config(payload, config_dir)

    # Nombre de sub autoincremental (consulta AS salvo en dry-run).
    cfg["sub_name"] = _next_subscription_name(cfg, run_ctx)

    # Ruta del <SUB>.properties en el motor Kafka (parametrizable; por defecto
    # el conf del motor Kafka donde corre iidr-ctl.py en el flujo cdc-controller).
    kafka_conf_dir = Path(os.environ.get(
        "IIDR_KAFKA_CONF_DIR",
        f"/u01/IIDR/ReplicationEngineforKafka/instance/{cfg['target_ds']}/conf",
    ))
    cfg["properties_path"] = str(kafka_conf_dir / f"{cfg['sub_name']}.properties")

    sequence = build_zeus_sequence(cfg)
    props_content = render_kcop_properties(cfg)

    if run_ctx.get("dry_run"):
        plan = {
            "status": "OK", "operation": "zeus.translate", "message": "dry-run",
            "data": {
                "env": cfg["env"], "subscription": cfg["sub_name"],
                "source_ds": cfg["source_ds"], "target_ds": cfg["target_ds"],
                "tables": cfg["tables"], "properties_path": cfg["properties_path"],
                "sequence": [s["command"] for s in sequence],
                "steps": sequence,
                "properties_preview": props_content,
            },
        }
        print(json.dumps(plan, ensure_ascii=False, indent=2))
        return 0

    # --- ejecución real -----------------------------------------------------
    # 1) depositar el <SUB>.properties en el motor Kafka (antes de setup-kafka)
    steps_result = []
    try:
        kafka_conf_dir.mkdir(parents=True, exist_ok=True)
        Path(cfg["properties_path"]).write_text(props_content, encoding="utf-8")
        steps_result.append({"step": "render-properties", "status": "OK",
                             "path": cfg["properties_path"]})
    except OSError as e:
        die(f"No se pudo escribir el .properties en {cfg['properties_path']}: {e}")

    # 2) ejecutar la secuencia; parar en el primer ERROR
    overall = "OK"
    for step in sequence:
        java_args = json_to_cli_args(step)
        rc, out, parsed = run_java_capture(
            java_args, run_ctx["root"], run_ctx["iidr_lib"], run_ctx["conf_dir"])
        status = (parsed or {}).get("status") or ("OK" if rc == 0 else "ERROR")
        steps_result.append({
            "step": step["command"], "status": status,
            "result": parsed if parsed is not None else out,
        })
        if status == "ERROR" or rc != 0:
            overall = "ERROR"
            break

    agg = {
        "status": overall, "operation": "zeus.apply",
        "data": {"subscription": cfg["sub_name"], "steps": steps_result},
        "message": ("flujo Kafka completado" if overall == "OK"
                    else "flujo detenido en el primer error"),
    }
    print(json.dumps(agg, ensure_ascii=False, indent=2))
    return 0 if overall == "OK" else 1


# ---------------------------------------------------------------------------
# Lectura del payload JSON
# ---------------------------------------------------------------------------

def read_json_payload(args: argparse.Namespace) -> dict:
    if args.json:
        try:
            return json.loads(args.json)
        except json.JSONDecodeError as e:
            die(f"JSON inválido en --json: {e}")
    if args.json_file:
        try:
            with open(args.json_file) as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            die(f"Error leyendo --json-file {args.json_file}: {e}")
    if args.stdin:
        try:
            return json.load(sys.stdin)
        except json.JSONDecodeError as e:
            die(f"JSON inválido en stdin: {e}")
    return {}


def die(msg: str) -> None:
    print(json.dumps({
        "status": "ERROR",
        "operation": "iidr-ctl.wrapper",
        "data": None,
        "message": msg,
    }, ensure_ascii=False))
    sys.exit(1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="iidr-ctl: wrapper Python para iidr-admin CLI Java",
        add_help=True,
    )

    json_group = parser.add_mutually_exclusive_group()
    json_group.add_argument("--json",      metavar="PAYLOAD", help="Payload JSON inline")
    json_group.add_argument("--json-file", metavar="FILE",    help="Fichero JSON con el payload")
    json_group.add_argument("--stdin",     action="store_true", help="Leer payload JSON desde stdin")

    parser.add_argument(
        "--iidr-home", metavar="PATH",
        default=os.environ.get("IIDR_HOME", "/u01/IIDR/AccessServer"),
        help="Ruta al Access Server IIDR (default: $IIDR_HOME)",
    )
    parser.add_argument(
        "--conf-dir", metavar="PATH",
        help="Directorio de configuración (default: <project_root>/conf)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Payload Zeus: imprime la secuencia de comandos sin ejecutar "
             "(no toca el AS ni escribe ficheros)",
    )

    parser.add_argument("cli_args", nargs=argparse.REMAINDER,
                        help="entity operation [--opt val ...] (modo CLI)")

    args = parser.parse_args()

    root     = resolve_root()
    iidr_lib = Path(args.iidr_home) / "lib"
    conf_dir = Path(args.conf_dir) if args.conf_dir else root / "conf"

    is_json_mode = args.json or args.json_file or args.stdin

    if is_json_mode:
        payload = read_json_payload(args)
        # Payload de Zeus/cdc-controller (replicación tabla→Kafka): orquesta el
        # flujo completo (create → setup-kafka → start) en vez de un comando.
        if is_zeus_payload(payload):
            run_ctx = {"root": root, "iidr_lib": iidr_lib, "conf_dir": conf_dir,
                       "dry_run": args.dry_run}
            try:
                sys.exit(run_zeus_payload(payload, run_ctx))
            except FileNotFoundError as e:
                die(str(e))
        try:
            java_args = json_to_cli_args(payload)
        except ValueError as e:
            die(str(e))
    else:
        cli = args.cli_args
        if cli and cli[0] == "--":
            cli = cli[1:]
        if not cli:
            parser.print_help(sys.stderr)
            sys.exit(1)
        java_args = cli

    try:
        exit_code = run_java(java_args, root, iidr_lib, conf_dir)
    except FileNotFoundError as e:
        die(str(e))

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
