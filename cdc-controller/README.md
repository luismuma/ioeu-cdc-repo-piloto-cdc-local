# CDC Controller

Herramienta de orquestación para gestionar procesos de **Change Data Capture (CDC)** desde bases de datos Oracle y MongoDB hacia destinos cloud (Confluent Kafka, AWS S3) mediante Oracle GoldenGate Microservices.

Se ejecuta principalmente desde **Ansible Automation Platform (AAP)** a través de los playbooks `auto_cdc-controller_validate.yml` y `auto_cdc-controller_apply.yml`, aunque también puede invocarse directamente desde línea de comandos para pruebas locales.

---

## Índice

- [Descripción general](#descripción-general)
- [Arquitectura del proyecto](#arquitectura-del-proyecto)
- [Requisitos](#requisitos)
- [Fichero de configuración JSON](#fichero-de-configuración-json)
- [Comandos disponibles](#comandos-disponibles)
- [Ejecución desde AAP](#ejecución-desde-aap)
- [Inventario de conexiones](#inventario-de-conexiones)
- [Gestión de secretos](#gestión-de-secretos)
- [Flujo de trabajo típico](#flujo-de-trabajo-típico)
- [Sistema de logs](#sistema-de-logs)
- [Estado de implementación](#estado-de-implementación)
- [-config_trandata — Configuración de TRANDATA](#-config_trandata--configuración-de-trandata)

---

## Descripción general

`cdc-controller` es un framework en Python que orquesta replicaciones CDC end-to-end. Dado un fichero JSON que describe la replicación deseada, el controlador valida todos los prerequisitos del entorno y, si la validación pasa, crea y arranca los procesos OGG (Extract, Replicat) contra los deployments de Oracle GoldenGate Microservices vía REST API.

```
┌─────────────────────────────────────────────────────┐
│              AAP Survey (JSON de entrada)            │
└──────────────────────┬──────────────────────────────┘
                       │
         ┌─────────────▼─────────────┐
         │     cdc-controller.py     │  dispatcher principal
         └──┬──────────┬─────────────┘
            │          │
     ┌──────▼──┐  ┌────▼──────┐
     │validate │  │  apply    │
     │  .py    │  │  .py      │
     └──┬──────┘  └────┬──────┘
        │              │
        ▼              ▼
  utils.py        ogg-ctl API
  (config,        (Extract / DP /
   checks,        Replicat via
   inventory)     REST Microservices)
```

---

## Arquitectura del proyecto

```
cdc-controller/
├── cdc-controller.py              # Punto de entrada y dispatcher
├── validate.py                    # Lógica -validate: checks de fuente, destino y OGG
├── apply.py                       # Lógica -apply / -plan: creación de procesos OGG
├── config_trandata.py             # Lógica -config_trandata: gestión de TRANDATA OGG
├── utils.py                       # Logger, carga JSON, validación, helpers de inventario
│
├── config/
│   ├── inventory_connections/
│   │   ├── db_connections.yaml    # Conexiones a bases de datos fuente (DSN, host, port...)
│   │   └── target_connections.yaml# Conexiones a sistemas destino (Kafka cluster, S3 bucket...)
│   └── inventory_deployments/
│       └── ...                    # Inventario de deployments OGG por entorno
│
├── input/                         # Ejemplos de ficheros JSON de replicación
│   ├── rep_DEV_ORACLE_ACNPDB.json
│   ├── rep_DEV_MONGO_CDCTEST.json
│   ├── rep_PRE_MP01.json
│   └── ...
│
└── logs/                          # Auto-creado; un fichero .log por ejecución
```

---

## Requisitos

### Python

- Python 3.11 o superior (el EE de AAP usa Python 3.12)

### Dependencias Python

Definidas en `requirements.txt` (raíz del repositorio). Las principales:

| Paquete            | Versión   | Uso                                      |
|--------------------|-----------|------------------------------------------|
| `oracledb`         | 3.4.2     | Conexión a bases de datos Oracle         |
| `pymongo`          | 4.6.3     | Conexión a MongoDB                       |
| `ibm_db`           | 3.2.3     | Conexión a DB2                           |
| `confluent-kafka`  | 2.14.2    | Validación de topics Kafka (AdminClient) |
| `boto3`            | 1.35.42   | Acceso a AWS S3 y STS AssumeRole         |
| `PyYAML`           | 6.0.3     | Lectura del inventario de conexiones     |
| `requests`         | 2.33.1    | OAuth token Entra ID / Schema Registry   |
| `Jinja2`           | 3.1.6     | Renderizado de templates PRM y properties|

### Herramientas externas

- **Oracle Instant Client** (requerido por `oracledb` en Linux) — ruta configurada vía `LD_LIBRARY_PATH`
- **ogg-ctl** — CLI Python para Oracle GoldenGate Microservices REST API (incluido en el repositorio bajo `ogg-ctl/`)

---

## Fichero de configuración JSON

Cada replicación se describe en un fichero `.json`. Es el único parámetro que reciben todos los comandos. Los campos se auto-normalizan a mayúsculas donde procede.

### Campos siempre requeridos

| Campo              | Valores válidos                          | Descripción                                  |
|--------------------|------------------------------------------|----------------------------------------------|
| `env`              | `DEV`, `PRE`, `PRO`                      | Entorno de ejecución                         |
| `request_type`     | `CREATE`, `DELETE`, `UPDATE`, `STATUS`   | Tipo de operación                            |
| `project`          | `DATAIA`, `GOLDENRECORDS`, ...           | Proyecto al que pertenece la replicación     |
| `bbdd_engine`      | `ORACLE`, `MONGO`, `DB2`                 | Motor de la base de datos fuente             |
| `bbdd_name`        | (string libre, p.ej. `MD01`)             | Nombre de la base de datos fuente            |
| `tables`           | Ver formato más abajo                    | Tablas a replicar con sus columnas           |
| `replication_type` | `FULL`, `ONLINE`, `INITIAL_LOAD`         | Tipo de replicación                          |
| `target_engine`    | `AWS-S3`, `KAFKA`                        | Motor de destino                             |
| `target_info`      | (string libre)                           | Descripción del destino (informativo)        |
| `user_name`        | (string libre)                           | Usuario solicitante (se registra en los PRM) |
| `cost_center`      | (string libre)                           | Centro de coste (se registra en los PRM)     |
| `change_number`    | (string libre, **obligatorio**)          | Número de cambio/ticket (se registra en los PRM) |

### Campos requeridos para target_engine `KAFKA`

| Campo                | Descripción                                                              |
|----------------------|--------------------------------------------------------------------------|
| `kafka_cluster_name` | Nombre del clúster; debe coincidir con `cluster_name` en `target_connections.yaml` |
| `kafka_identity_pool`| Identity Pool de Confluent Cloud; usado para disambiguar entradas        |
| `kafka_topic_prefix` | Prefijo para construir el nombre del topic por tabla                     |

### Campos requeridos para target_engine `AWS-S3`

| Campo            | Descripción                                                                |
|------------------|----------------------------------------------------------------------------|
| `s3_bucket_name` | Nombre del bucket S3; localiza la entrada en `target_connections.yaml`    |
| `s3_bucket_path` | Path/prefijo dentro del bucket donde se escribirán los datos replicados   |

### Campos opcionales

| Campo         | Descripción                                                                 |
|---------------|-----------------------------------------------------------------------------|
| `target_name` | Clave explícita en `target_connections.yaml` (se deriva automáticamente si se omite) |
| `comments`    | Nota libre; ignorado por la lógica de validación                            |

> Las claves que empiezan por `//` se tratan como comentarios y son ignoradas.

### Formato del campo `tables`

```
"SCHEMA.TABLA(COL1,COL2)"   → replica solo las columnas indicadas (columnas de PK obligatorias)
"SCHEMA.TABLA(*)"            → replica todas las columnas
```

### Ejemplo completo (Oracle → Kafka)

```json
{
  "env": "DEV",
  "request_type": "CREATE",
  "project": "GOLDENRECORDS",
  "bbdd_engine": "ORACLE",
  "bbdd_name": "MD01",
  "tables": [
    "RIC_LD.RIC_DAT_CLIENTES(COD_CLIENTE_RIC,FEC_BAJA)",
    "RIC_LD.RIC_CAN_EMAIL(COD_EMAIL)"
  ],
  "replication_type": "ONLINE",
  "target_engine": "KAFKA",
  "kafka_cluster_name": "IClusterPEI",
  "kafka_identity_pool": "pool-y95PX",
  "kafka_topic_prefix": "esp.int.cac.bronze.cdccac",
  "target_info": "Confluent Kafka DEV",
  "user_name": "nombre.apellido@empresa.com",
  "cost_center": "MAPFRE",
  "change_number": "CHG0012345"
}
```

---

## Comandos disponibles

```bash
python cdc-controller.py <COMANDO> <fichero.json>
```

### `-validate`

Verifica que todos los prerequisitos están en orden **antes de realizar ningún cambio**. Es un comando de solo lectura.

```bash
python cdc-controller.py -validate rep_DEV_MD01.json
```

**Checks realizados:**

#### Base de datos fuente
| Check | Oracle | MongoDB |
|---|---|---|
| Conectividad (primary) | ✅ | ✅ |
| Conectividad (standby) | ✅ (si configurado) | — |
| Versión mínima | ✅ (≥ 19) | — |
| ARCHIVELOG mode | ✅ | — |
| FORCE LOGGING | ✅ | — |
| Supplemental Log MIN | ✅ | — |
| enable_goldengate_replication | ✅ | — |
| Streams Pool Size ≥ 1000 MB | ✅ | — |

#### Tablas
| Check | Descripción |
|---|---|
| Existencia en BD | Las tablas del JSON existen en la base de datos |
| Columnas válidas | Todas las columnas del JSON existen en cada tabla |
| PK cubierta por COLMAP | Las columnas de PK están incluidas en la selección |
| Modo soporte OGG | Las tablas no tienen tipos incompatibles con GoldenGate |
| TRANDATA habilitado | Supplemental logging activado a nivel de tabla |
| Tipos LONG | Sin columnas LONG (no soportadas por OGG) |
| Nested tables | Sin nested tables |

#### Sistema destino (Kafka)
| Check | Descripción |
|---|---|
| Entrada en `target_connections.yaml` | Existe entrada para el cluster/identity_pool/env indicado |
| Longitud alias OGG (≤ 30 chars) | El `ogg_alias_credential` no supera el límite del wallet OGG |
| TCP broker reachable | Socket connect al bootstrap server en puerto 9092 |
| Bootstrap reachable | Todos los brokers del bootstrap responden a TCP |
| Schema Registry HTTP | URL del Schema Registry devuelve respuesta HTTP |
| OAuth authentication | Se obtiene token Entra ID para el Identity Pool |
| Schema Registry auth | El token es válido para el Schema Registry |
| Topics en cluster | Los topics esperados (`<prefix>.<schema>.<tabla>`) existen en Kafka |

#### Sistema destino (AWS S3)
| Check | Descripción |
|---|---|
| Entrada en `target_connections.yaml` | Existe entrada para el bucket indicado |
| Longitud alias OGG (≤ 30 chars) | El `ogg_alias_credential` no supera el límite del wallet OGG |
| S3 endpoint HTTP reachable | El endpoint regional de S3 responde |
| STS AssumeRole | Se asume el rol IAM con las credenciales long-term |
| HeadBucket | El bucket existe y es accesible con las credenciales temporales |
| ListObjectsV2 (path prefix) | El path indicado es accesible dentro del bucket |

#### Deployments OGG
| Check | Descripción |
|---|---|
| Deployments matching | Existen deployments OGG en el inventario para el entorno/motor |
| TCP puerto 443 | El deployment OGG Microservices responde en HTTPS |
| Estado deployment | El deployment está en estado `running` |
| Managed-process profile | Existe el profile `Critical` en el deployment |
| DB connection | El deployment OGG tiene configurada la conexión a la base de datos fuente |

#### Procesos CDC existentes
| Check | Descripción |
|---|---|
| Conflicto de extract | Ninguna de las tablas está ya en otro extract activo |
| Conflicto de replicat | Ninguna de las tablas está ya en otro replicat activo |

---

### `-apply`

Crea y arranca todos los procesos OGG necesarios para la replicación. Ejecuta previamente los mismos checks que `-validate`.

```bash
python cdc-controller.py -apply rep_DEV_MD01.json
```

**Procesos creados según tipo de replicación:**

| `replication_type` | Procesos creados |
|--------------------|-----------------|
| `ONLINE`           | Extract integrado + Replicat |
| `FULL`             | Extract inicial + Extract online + Replicat |
| `INITIAL_LOAD`     | Extract de carga inicial + Replicat de carga |

Para target `KAFKA`: crea el credential alias OGG, el fichero `kafka_producer.properties` y el fichero JAAS en cada deployment antes de crear el Replicat.

Para target `AWS-S3`: crea el credential alias OGG (AWS access key) en cada deployment antes de crear el Replicat.

---

### `-plan`

Dry-run de `-apply`: muestra qué procesos se crearían y qué comandos se ejecutarían, sin realizar ningún cambio real.

```bash
python cdc-controller.py -plan rep_DEV_MD01.json
```

---

### `-config_trandata`

Configura el supplemental logging a nivel de tabla (**TRANDATA**) en Oracle a través del deployment OGG Microservices del entorno. Solo requiere los 5 campos básicos del JSON.

```bash
python cdc-controller.py -config_trandata trandata_DEV_ACNPDB.json
```

**Valores de `request_type` soportados:**

| `request_type` | Acción |
|---|---|
| `CREATE` | Activa TRANDATA con `--all-columns` + verifica con INFO |
| `DELETE` | Desactiva TRANDATA + verifica con INFO |
| `STATUS` | Muestra el estado actual de TRANDATA (solo lectura) |

**Campos requeridos** (únicamente estos; el resto del JSON se ignora):

| Campo | Descripción |
|---|---|
| `env` | `DEV`, `PRE`, `PRO` |
| `request_type` | `CREATE`, `DELETE`, `STATUS` |
| `bbdd_engine` | `ORACLE`, `MONGO`, `DB2` |
| `bbdd_name` | Nombre de la base de datos (p.ej. `ACNPDB`) |
| `tables` | Lista de tablas en formato `SCHEMA.TABLE` o `SCHEMA.TABLE(*)` |

**Ejemplo de JSON mínimo:**

```json
{
  "env": "DEV",
  "request_type": "CREATE",
  "bbdd_engine": "ORACLE",
  "bbdd_name": "ACNPDB",
  "tables": [
    "SOE.LOGON(*)",
    "SOE.ORDERS(*)"
  ]
}
```

**Ejemplo de salida (CREATE):**

```
====== config_trandata — CREATE (2 table(s)) ======
─── ACNPDB.SOE.LOGON ───
  trandata add    : OK     (ACNPDB.SOE.LOGON)
  trandata info   : OK     (ACNPDB.SOE.LOGON)
    table             : ACNPDB.SOE.LOGON
    enabled           : True
    primaryKey        : True
    schedulingColumns : True
    allColumns        : True
    columns           : (none specified)
─── ACNPDB.SOE.ORDERS ───
  trandata add    : OK     (ACNPDB.SOE.ORDERS)
  trandata info   : OK     (ACNPDB.SOE.ORDERS)
    ...
config_trandata result         : OK  (2/2 OK, 0 error(s))
```

**Resolución automática de deployment y alias:**
- El **deployment OGG** se selecciona del inventario (`inventory.yaml`) filtrando por `env` + `bbdd_engine` + `load_type=online` + `proc_type=extract`.
- El **alias de conexión** y el **prefijo PDB** se obtienen de `db_connections.yaml` a partir de `<bbdd_name>_<env>` → campo `conn_name_prim` y `pdb`.

---

### `-help`

Muestra el mensaje de ayuda con todos los comandos disponibles.

```bash
python cdc-controller.py -help
```

---

## Ejecución desde AAP

El modo de ejecución habitual es a través de los **Job Templates de AAP**:

| Job Template | Playbook | Descripción |
|---|---|---|
| `cdc-controller-validate` | `auto_cdc-controller_validate.yml` | Ejecuta `-validate` |
| `cdc-controller-apply` | `auto_cdc-controller_apply.yml` | Ejecuta `-apply` |

**Variables de Survey (AAP):**

| Variable | Tipo | Descripción |
|---|---|---|
| `ogg_env` | choice (`dev`/`pre`/`pro`) | Entorno de ejecución |
| `cdc_input_json_content` | textarea | JSON de la replicación (pegado directamente) |
| `cdc_input_file` | text | Ruta alternativa a un fichero JSON en disco |

El JSON se materializa en un fichero temporal dentro del EE (`/tmp/cdc_input_*.json`, mode `0600`) y se elimina al finalizar.

**Códigos de salida:**

| Código | Significado |
|--------|-------------|
| `0` | Ejecución correcta (validate OK / apply completado) |
| `1` | Errores encontrados en los checks |
| `2` | Precondición del playbook fallida |

---

## Inventario de conexiones

### `config/inventory_connections/target_connections.yaml`

Define las conexiones a los sistemas destino. Cada entrada se identifica por una clave `<ENGINE>_<NAME>_<ENV>`.

**Para Kafka (Confluent Cloud + Entra ID OAUTHBEARER):**

```yaml
KAFKA_GR_CANALES_CLIENTES_01_DEV:
  env:                       "DEV"
  target_engine:             "KAFKA"
  cluster_name:              "IClusterPEI"
  cluster_id:                "lkc-mr7wq7"
  bootstrap_servers:         "pkc-xxxx.eu-west-1.aws.confluent.cloud:9092"
  schema_registry_url:       "https://psrc-xxxx.eu-west-1.aws.confluent.cloud"
  security_protocol:         "SASL_SSL"
  sasl_mechanism:            "OAUTHBEARER"
  identity_pool:             "pool-qBk96"
  api_scope:                 "api://xxxx/.default"
  client_id:                 "xxxx"
  client_secret_credential:  "KAFKA_GR_CANALES_CLIENTES_01_SECRET_DEV"  # nombre del env var
  ogg_alias_credential:      "KAFKA_GR_CAC_01_DEV"                      # ≤ 30 chars
```

**Para AWS S3:**

```yaml
S3_DA_PRE:
  env:                              "PRE"
  target_engine:                    "AWS-S3"
  s3_bucket_name:                   "mi-bucket-pre"
  aws_account_id:                   "123456789"
  aws_region:                       "eu-west-1"
  sts_assume_role:                  "arn:aws:iam::123456789:role/mi-role"
  sts_session_name:                 "ogg-pre-session"
  aws_access_key_id:                "AKIAXXXX"
  aws_secret_access_key_credential: "AWS_SECRET_ACCESS_KEY_PRE"  # nombre del env var
  ogg_alias_credential:             "S3_OAUTH_01_PRE"             # ≤ 30 chars
```

> **Nota:** Los campos `*_credential` no contienen el valor del secreto, sino el **nombre de la variable de entorno** que lo transporta. El valor real proviene de AWS Secrets Manager vía AAP.

### `config/inventory_connections/db_connections.yaml`

Define las cadenas de conexión a las bases de datos fuente, indexadas por `<BBDD_NAME>_<ROLE>_<ENV>` (p.ej. `MD01_PRIM_DEV`).

---

## Gestión de secretos

Los secretos se almacenan en **AWS Secrets Manager** con un secret por entorno:

| Secret name | Entorno |
|---|---|
| `CDC-VAULT-DEV` | Desarrollo |
| `CDC-VAULT-PRE` | Preproducción |
| `CDC-VAULT-PRO` | Producción |

Cada secret es un JSON con claves con el formato:
- `ORACLE-<BBDD>-<USER>` → contraseña de usuario Oracle
- `OGG-<USER>` → contraseña del usuario OGG admin (`OGG-OGGADMIN`)
- `KAFKA-<CLUSTER>-SECRET` → client secret de Entra ID para el cluster Kafka
- `AWS-<NAME>-SECRET` → AWS secret access key para el usuario IAM de S3

El playbook de AAP inyecta automáticamente como variables de entorno del subproceso todas las claves del secret cuyo nombre empiece por `ORACLE_`, `MONGO_`, `KAFKA_`, `S3_` o `AWS_`.

---

## Flujo de trabajo típico

```
1. Preparar el JSON de replicación
         │
         ▼
2. Ejecutar Job Template "cdc-controller-validate" en AAP
   └─ Verificar: BD fuente, tablas, target Kafka/S3, deployments OGG
         │
         ├─ Si hay errores → corregir configuración y repetir paso 2
         │
         ▼
3. Resolver prerequisitos bloqueantes:
   ├─ TRANDATA no habilitado → ejecutar ADD TRANDATA en Oracle
   └─ Topics Kafka no existen → crearlos en Confluent Cloud
         │
         ▼
4. Ejecutar Job Template "cdc-controller-apply" en AAP
   └─ Crear procesos OGG: Extract  + Replicat
         │
         ▼
5. Verificar estado de los procesos en OGG Microservices UI
```

---

## Sistema de logs

Cada ejecución crea automáticamente un fichero de log en `logs/`:

```
logs/cdc-controller_exec_20260825_065138.log
```

**Formato:**

```
25-08-2026 06:51:38 - INFO:  json config file validation           : OK
25-08-2026 06:51:39 - INFO:  connected to Oracle Database          : OK
25-08-2026 06:51:41 - ERROR: tables with TRANDATA enabled         : Error  (0/10 found)
25-08-2026 06:51:57 - ERROR: validate result                      : ERROR  (10 error(s) found)
```

Los logs se generan en paralelo a la salida por consola. Las contraseñas y secretos **nunca** se escriben en los logs.

---

## Estado de implementación

| Módulo                       | Estado         | Descripción                                                     |
|------------------------------|----------------|-----------------------------------------------------------------|
| Dispatcher / CLI             | ✅ Completo    | Routing de comandos, banner, timing, validación                 |
| Validación JSON              | ✅ Completo    | Todos los campos, tipos y formatos validados                    |
| Sistema de logging           | ✅ Completo    | Logs con timestamp a fichero y consola simultáneamente          |
| `-validate` Oracle           | ✅ Completo    | BD, tablas, TRANDATA, OGG deployments, Kafka/S3, conflictos     |
| `-validate` MongoDB          | ✅ Completo    | BD, colecciones, OGG deployments, Kafka/S3, conflictos          |
| `-apply` Oracle ONLINE       | ✅ Completo    | Extract + DP + Replicat vía OGG Microservices REST              |
| `-apply` Oracle FULL         | ✅ Completo    | Extract inicial + online + DP + Replicat                        |
| `-apply` MongoDB             | ✅ Completo    | Extract + DP + Replicat                                         |
| `-apply` target Kafka        | ✅ Completo    | Credential alias + producer.properties + JAAS + Replicat        |
| `-apply` target AWS-S3       | ✅ Completo    | Credential alias + Replicat con handler S3                      |
| `-plan` (dry-run)            | ✅ Completo    | Simula `-apply` sin ejecutar cambios reales                     |
| `-config_trandata` Oracle    | ✅ Completo    | ADD / DELETE / INFO de TRANDATA vía OGG REST; CREATE + DELETE + STATUS |
| `-validate` DB2              | ⚠️ Parcial    | Conectividad básica; checks específicos de IIDR pendientes      |

---

---

## `-config_trandata` — Configuración de TRANDATA

Módulo independiente (`config_trandata.py`) para gestionar el supplemental logging Oracle a nivel de tabla sin necesidad de ejecutar un flujo CDC completo.

### Flujo de trabajo típico previo a un `-apply`

```
1. Crear JSON mínimo con env + bbdd_engine + bbdd_name + tables
         │
         ▼
2. python cdc-controller.py -config_trandata trandata_DEV_ACNPDB.json
   request_type: STATUS   → revisar estado actual
         │
         ▼
3. Si alguna tabla tiene enabled=False:
   request_type: CREATE   → activa TRANDATA (all columns) + verifica
         │
         ▼
4. Ejecutar -validate / -apply con normalidad
```

### Equivalencia de comandos ogg-ctl

| `-config_trandata` request_type | oggctl equivalente |
|---|---|
| `CREATE` | `python oggctl.py trandata add --deployment DEP --connection CONN --table PDB.SCHEMA.TABLE --all-columns` |
| `DELETE` | `python oggctl.py trandata delete --deployment DEP --connection CONN --table PDB.SCHEMA.TABLE` |
| `STATUS` | `python oggctl.py trandata info --deployment DEP --connection CONN --table PDB.SCHEMA.TABLE` |

El deployment y el alias de conexión se resuelven automáticamente desde el inventario en función de `env` y `bbdd_name`.

---

## Licencia

Uso interno — Accenture Data & AI.
