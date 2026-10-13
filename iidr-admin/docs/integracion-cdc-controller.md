# Integración cdc-controller → iidr-admin (payload Zeus → flujo Kafka IIDR)

Este documento describe cómo `iidr-admin` recibe una petición de replicación
originada en la plataforma **Zeus** de Mapfre y la traduce al flujo completo
de IIDR/CDC hacia Kafka. Es el **punto de integración** entre el
meta-controlador `cdc-controller` (que despacha peticiones OGG e IIDR) y
nuestro wrapper `bin/iidr-ctl.py`.

> **Alcance de esta iteración**: `request_type=CREATE` (crear + configurar +
> arrancar una suscripción Kafka). `UPDATE`/`DELETE`/`STATUS` y el mapeo de
> columnas explícitas quedan para iteraciones siguientes (ver §7).

---

## 1. Cómo encaja en cdc-controller

```
Zeus  ──payload JSON──▶  cdc-controller  ──despacha por bbdd_engine──┐
                                                                     │
                    ORACLE / MONGO ─▶ ogg-ctl   (OGG)                │
                    DB2            ─▶ iidr-admin (IIDR)  ◀───────────┘
                                        │
                                        ▼
                            bin/iidr-ctl.py  (este wrapper)
                                        │
                    create ─▶ setup-kafka ─▶ start   (iidr.sh / JVM)
```

`cdc-controller` decide el motor por el campo `bbdd_engine` del payload
(ORACLE/MONGO → OGG; **DB2 → IIDR**). Cuando integre IIDR, `cdc-controller`
invocará `bin/iidr-ctl.py` pasándole el **mismo payload JSON** que recibe de
Zeus. `iidr-ctl.py` lo detecta, resuelve la configuración desde los
inventarios compartidos y orquesta el flujo Kafka.

> **Estado (2026-07):** la rama de `cdc-controller` en `main` aún tiene el
> despacho DB2 como *placeholder* (solo ORACLE tiene flujo real). La
> integración por el lado IIDR (este wrapper) ya está lista para recibir el
> payload; la llamada desde `cdc-controller` la añadirán los compañeros de OGG.

---

## 2. Contrato del payload Zeus

Mismo esquema que los payloads OGG (`cdc-controller/input/*.json`), con
`bbdd_engine: "DB2"`. Ejemplo (payload de prueba en
`cdc-controller/input/rep_GR_DEV_DB2DSNT.json`):

```json
{
  "env": "DEV",
  "request_type": "CREATE",
  "project": "GOLDENRECORDS",
  "bbdd_engine": "DB2",
  "bbdd_name": "DB2DSNT",
  "tables": ["REAL.TB030T001(*)", "REAL.TB030T002(*)"],
  "replication_type": "ONLINE",
  "target_engine": "Kafka",
  "target_info": "Confluent Kafka DEV"
}
```

| Campo | Uso en iidr-admin |
|---|---|
| `env` | Entorno (`DEV`/`PRE`/`PRO`). Selecciona entradas de inventario y secreto AWS. |
| `request_type` | `CREATE` (default). Otros valores → error controlado (aún no soportados). |
| `project` | Solo para la descripción de la suscripción. **No** es pivote de resolución. |
| `bbdd_engine` | Debe ser `DB2` (si no, el payload no es para IIDR → error). |
| `bbdd_name` | Clave de la conexión origen (`<BBDD_NAME>_<ENV>`) y base del nombre de sub. |
| `tables` | Lista `ESQUEMA.TABLA(cols)`. El sufijo `(cols)` se **ignora** por ahora (tabla entera). |
| `target_engine` | `Kafka` (informativo; el target real se resuelve por inventario). |
| `target_name` | *(opcional)* clave explícita en `target_connections.yaml`. Default `KAFKA_GR_01_<ENV>`. |
| `topic_prefix` | *(opcional)* prefijo de topics. Default `esp.int.cac.bronze.grcdcpiloto`. |

**Sin credenciales**: el payload nunca lleva secretos; se resuelven por
entorno vía AWS Secret Manager (env vars, ver §5).

---

## 3. Resolución de configuración (patrón OGG)

`iidr-ctl.py` lee los **mismos inventarios** que OGG en
`cdc-controller/config/` (solo lectura para nosotros), replicando la lógica
de `cdc-controller/utils.py`:

| Dato | Fuente | Mecanismo |
|---|---|---|
| Conexión origen | `inventory_connections/db_connections.yaml` | Clave directa `<BBDD_NAME>_<ENV>` (ej. `DB2DSNT_DEV`). |
| Datastore IIDR **origen** | `inventory_deployments/inventory.yaml` | Filtro `env` + `db_engine=DB2` + `proc_type=extract`. |
| Datastore IIDR **destino** (Kafka) | `inventory_deployments/inventory.yaml` | Filtro `env` + `db_engine=DB2` + `proc_type=replicat`. |
| Schema Registry + OAuth + bootstrap | `inventory_connections/target_connections.yaml` | Entrada `KAFKA_GR_01_<ENV>` (o `target_name`). |

> **Unión inventory ↔ db_connections**: igual que en OGG, **no hay foreign
> key** entre ambos ficheros. `db_connections.yaml` se resuelve por clave
> directa desde el payload; `inventory.yaml` por **filtro** `env`+`db_engine`
> (el `project` no es pivote — un deployment sirve a varios proyectos).

### Requisito pendiente: entrada IIDR DB2 en `inventory.yaml`

La sección `# IIDR DB2` de `inventory.yaml` debe contener las entradas del
datastore IIDR origen y destino. Esquema esperado (calcado de las entradas
OGG existentes):

```yaml
# IIDR DB2 :
IIDR_GR_DB2_E01_AWS_DEV:
  deployment: "IIDR_GR_DB2_E01_AWS_DEV"   # nombre del datastore IIDR origen
  env:        "DEV"
  load_type:  "online"
  proc_type:  "extract"
  db_engine:  "DB2"

IIDR_GR_KFK_R01_AWS_DEV:
  deployment: "IIDR_GR_KFK_R01_AWS_DEV"   # datastore Kafka destino
  env:        "DEV"
  load_type:  "online"
  proc_type:  "replicat"
  db_engine:  "DB2"
```

Si la entrada no existe, `iidr-ctl.py` falla con un mensaje claro
(*"¿Falta mergear la entrada IIDR DB2?"*) en vez de inventar un nombre.

---

## 4. Flujo orquestado

`iidr-ctl.py` traduce el payload a una **secuencia de comandos** IIDR y la
ejecuta parando en el primer error:

1. **Render `<SUB>.properties`** — genera el fichero del KCOP (mismo formato
   de 4 partes que `ansible/templates/kcop.properties.j2`) y lo deposita en
   el motor Kafka (ruta parametrizable con `IIDR_KAFKA_CONF_DIR`).
2. **`subscription create`** — crea la suscripción (origen → destino).
3. **`subscription setup-kafka`** — mapea tablas, configura kafka properties,
   registra el user exit (KCOP) apuntando al `.properties`, con el
   commit-topic `<topic_prefix>.<SUB>-commitstream_cdc`.
4. **`subscription start`** — arranca la replicación (Mirror Continuous).

> **Nombre de la suscripción — autoincremental.** Cada petición de Zeus crea
> una sub nueva: `<BBDD_NAME>_<ENV>_<N>`. `iidr-ctl.py` consulta
> `subscription list` en el AS y usa el menor `N` libre a partir del arranque
> por entorno (DEV empieza en `6` para dejar margen a las subs manuales).

La salida es un **JSON agregado** con el resultado de cada paso:

```json
{
  "status": "OK",
  "operation": "zeus.apply",
  "data": { "subscription": "DB2DSNT_DEV_6", "steps": [ ... ] },
  "message": "flujo Kafka completado"
}
```

---

## 5. Ejecución

### Dry-run (traducción sin tocar nada)

Imprime la secuencia + el `.properties` renderizado, **sin** consultar el AS
ni escribir ficheros. Ideal para validar la resolución de config:

```bash
python3 bin/iidr-ctl.py --json-file cdc-controller/input/rep_GR_DEV_DB2DSNT.json --dry-run
```

### Ejecución real

Requiere las env vars de credenciales (AWS SM), igual que el resto de
comandos:

```bash
TLS_TRUST_STORE_PASSWORD='<pwd>' \
KAFKA_CLIENT_ID='<id>' KAFKA_CLIENT_SECRET='<secret>' KAFKA_POOL_ID='<pool>' \
  python3 bin/iidr-ctl.py --json-file cdc-controller/input/rep_GR_DEV_DB2DSNT.json
```

| Env var | Uso |
|---|---|
| `TLS_TRUST_STORE_PASSWORD` | Password del truststore (JVM). Obligatoria. |
| `KAFKA_CLIENT_ID` / `KAFKA_CLIENT_SECRET` / `KAFKA_POOL_ID` | Credenciales OAuth del Schema Registry (render del `.properties`). Del secreto AWS SM. |
| `IIDR_KAFKA_CONF_DIR` | *(opcional)* dónde depositar el `<SUB>.properties`. Default: conf del motor Kafka. |
| `IIDR_CDC_CONFIG_DIR` | *(opcional)* apunta a un `config/` alternativo (pruebas). Default: `cdc-controller/config`. |

> Las 3 claves del Schema Registry **no** llevan prefijo `IIDR_` (se llaman
> `KAFKA_CLIENT_ID`/`KAFKA_CLIENT_SECRET`/`KAFKA_POOL_ID`, compartidas con
> otros equipos). El resto de claves del secreto sí llevan `IIDR_`.

---

## 6. Relación con los playbooks (proyecto A)

Este traductor **no** sustituye a los playbooks (`ansible/playbooks/
subscriptions.yml`). Conviven:

- **Playbooks** — para operar y probar por nuestra cuenta desde AAP y línea
  de comando (proyecto A). El `.properties` lo genera el template `.j2`.
- **Traductor Zeus** — punto de integración con `cdc-controller` (proyecto B).
  El `.properties` lo genera `iidr-ctl.py` en Python (mismo formato).

Ambos reutilizan los mismos comandos Java (`create`, `setup-kafka`, `start`).

---

## 7. Fuera de alcance / pendientes

- **`request_type` UPDATE/DELETE/STATUS**: solo `CREATE` en esta iteración.
- **Columnas explícitas** (`TABLA(COL1,COL2)`): se mapea la tabla entera; las
  columnas del payload se ignoran (soporte de columnas = iteración propia,
  CHCCLP tiene `map column`).
- **Topic de commit**: la creación del topic en Kafka sigue **bloqueada
  externamente** (permisos). El `bootstrap_servers` ya está en
  `target_connections.yaml` para cuando se conceda. Hasta entonces, `start`
  falla en ese punto y el JSON agregado lo reporta.
- **Llamada desde `cdc-controller`**: la añadirán los compañeros de OGG; este
  wrapper ya está listo para recibir el payload.
