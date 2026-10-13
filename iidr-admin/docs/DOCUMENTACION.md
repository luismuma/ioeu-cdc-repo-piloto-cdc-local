# iidr-admin — Documentación técnica

## Índice

1. [Descripción general](#1-descripción-general)
2. [Arquitectura](#2-arquitectura)
3. [Estructura de ficheros](#3-estructura-de-ficheros)
4. [Componentes del código](#4-componentes-del-código)
   - 4.1 [IIDRAdmin — Punto de entrada CLI](#41-iidradmin--punto-de-entrada-cli)
   - 4.2 [IIDRConnection — Conexión al Access Server](#42-iidrconnection--conexión-al-access-server)
   - 4.3 [Command e CommandResult — Modelo de comandos](#43-command-y-commandresult--modelo-de-comandos)
   - 4.4 [OutputFormatter — Serialización JSON](#44-outputformatter--serialización-json)
   - 4.5 [TableSubscriptionRegistry — Registro CSV](#45-tablesubscriptionregistry--registro-csv)
5. [Comandos disponibles](#5-comandos-disponibles)
   - 5.1 [Datastores](#51-datastores)
   - 5.2 [Suscripciones](#52-suscripciones)
   - 5.3 [Mappings](#53-mappings)
   - 5.4 [Operaciones compuestas](#54-operaciones-compuestas)
6. [Configuración](#6-configuración)
   - 6.1 [iidr.properties](#61-iidrproperties)
   - 6.2 [tls.properties y truststore](#62-tlsproperties-y-truststore)
   - 6.3 [logback.xml](#63-logbackxml)
7. [Ejecución](#7-ejecución)
   - 7.1 [Requisitos previos](#71-requisitos-previos)
   - 7.2 [Compilación](#72-compilación)
   - 7.3 [Uso del CLI](#73-uso-del-cli)
   - 7.4 [Perfil motor (host sin Access Server)](#74-perfil-motor-host-sin-access-server)
8. [Integración con Ansible](#8-integración-con-ansible)
   - 8.1 [Contrato de salida JSON](#81-contrato-de-salida-json)
   - 8.2 [Exit codes](#82-exit-codes)
   - 8.3 [Ejemplo de invocación desde playbook](#83-ejemplo-de-invocación-desde-playbook)
   - 8.4 [Contrato AAP para `deploy.sh`](#84-contrato-aap-para-deploysh)
9. [Idempotencia](#9-idempotencia)
10. [Provisioning desde `iidr-install`](#10-provisioning-desde-iidr-install)
    - 10.1 [Patrón APP-cdcadmin → APP-cdcauto](#101-patrón-app-cdcadmin--app-cdcauto)
    - 10.2 [Comando `datastore grant`](#102-comando-datastore-grant)
    - 10.3 [Flujo completo desde `iidr-install`](#103-flujo-completo-desde-iidr-install)
11. [Formato JSON para operaciones compuestas](#11-formato-json-para-operaciones-compuestas)
12. [TLS — Notas importantes](#12-tls--notas-importantes)
13. [Tecnologías y dependencias](#13-tecnologías-y-dependencias)

---

## 1. Descripción general

**iidr-admin** es una herramienta CLI Java que automatiza las operaciones de administración de **IBM InfoSphere Data Replication (IIDR/CDC)**: creación y gestión de datastores, suscripciones y mappings de tablas.

### ¿Qué hace?

- Proporciona un CLI con el patrón `iidr.sh <entidad> <operación> [opciones]`
- Todas las operaciones son **idempotentes**: si el estado deseado ya existe, retorna `SKIPPED` sin error
- La salida es siempre **JSON por stdout**, para consumo directo por Ansible
- Los logs de aplicación van a **stderr**, para no contaminar la salida JSON
- Sale con **exit code 0** (OK o SKIPPED) o **exit code 1** (error), para control de flujo en Ansible

### Caso de uso en Mapfre

Automatizar desde Ansible la provisión completa de una replicación CDC: crear los datastores de origen (DB2) y destino (Kafka), crear la suscripción, añadir los mappings de tablas y arrancar la replicación. Un solo playbook puede ejecutar todo el ciclo usando las operaciones compuestas.

---

## 2. Arquitectura

```
Ansible playbook
      │
      └─► bin/iidr.sh <entidad> <operacion> [opciones]
               │
               ├─► IIDRAdmin.main()        ← dispatcher de comandos
               ├─► IIDRConnection          ← abre EmbeddedScript → Access Server
               ├─► Comando.execute()       ← lógica CRUD con idempotencia
               │     ├─► SKIPPED si estado deseado ya existe
               │     └─► OK si se ejecutó la operación
               ├─► OutputFormatter         ← serializa CommandResult a JSON → stdout
               └─► System.exit(0 o 1)
```

### Patrón Command

Cada operación es una clase Java que implementa la interfaz `Command`:

```
Command (interfaz)
   └─► CommandResult execute(IIDRConnection, String[] args)

Implementaciones:
   datastore/   DatastoreList, DatastoreShow, DatastoreCreate, DatastoreDelete
   subscription/ SubscriptionList, SubscriptionShow, SubscriptionCreate,
                 SubscriptionDelete, SubscriptionStart, SubscriptionStop,
                 SubscriptionLock, SubscriptionUnlock, SubscriptionReinit
   mapping/     MappingAdd, MappingRemove, MappingList, MappingFindByTable
   composite/   CompositeSetup, CompositeTeardown, CompositeRestart, CompositeHealthCheck
```

---

## 3. Estructura de ficheros

```
iidr-admin/
│
├── bin/
│   ├── iidr.sh                           Script de entrada del CLI
│   ├── iidr-ctl.py                       Wrapper Python (compatibilidad legado)
│   ├── deploy.sh                         Despliega al NFS (/u01/repo/automation/iidr-ctl/)
│   ├── iidr-encrypt.sh                   Cifra/descifra passwords con IBM Encryptor
│   └── iidr-build-truststore.sh          Construye truststore PKCS12 desde certs PKI
│
├── conf/
│   ├── iidr.properties.template          Plantilla — deploy.sh renderiza iidr.properties
│   ├── iidr.properties                   Conexión al AS (NO en git; render por deploy)
│   ├── logback.xml                       Configuración de logging (SLF4J/Logback)
│   ├── cdc_truststore_AccessServer.p12   Truststore (NO en git; construido por deploy)
│   └── table_subscription_registry.csv   Registro local tabla→suscripción (auto-gestionado)
│
├── lib/
│   └── IIDRCrypt.java                    Fuente que compila iidr-encrypt.sh on-the-fly
│
├── src/main/java/com/mapfre/cdc/admin/
│   ├── IIDRAdmin.java                    Punto de entrada y dispatcher de comandos
│   ├── IIDRConnection.java               Wrapper sobre EmbeddedScript de IBM IIDR
│   ├── OutputFormatter.java              Serializa CommandResult a JSON en stdout
│   ├── commands/
│   │   ├── Command.java                  Interfaz que implementan todos los comandos
│   │   ├── CommandResult.java            Modelo de resultado (OK / SKIPPED / ERROR)
│   │   ├── datastore/                    Comandos de datastore
│   │   ├── subscription/                 Comandos de suscripción
│   │   ├── mapping/                      Comandos de mapping
│   │   └── composite/                    Operaciones compuestas (setup, teardown…)
│   └── registry/
│       └── TableSubscriptionRegistry.java  Gestión del CSV tabla→suscripción
│
├── target/
│   ├── iidr-admin-1.0-SNAPSHOT.jar       JAR compilado
│   └── classpath.txt                     Classpath Maven (auto-generado)
│
├── log/
│   └── iidr-admin.log                    Log de aplicación
│
└── pom.xml                               Configuración Maven
```

---

## 4. Componentes del código

### 4.1 `IIDRAdmin` — Punto de entrada CLI

**Fichero:** `src/main/java/com/mapfre/cdc/admin/IIDRAdmin.java`

Punto de entrada del programa. Mantiene un mapa estático de todos los comandos registrados (`"entidad.operacion"` → clase Command) y los despacha en función de los dos primeros argumentos CLI.

El fichero de properties se puede sobreescribir mediante la propiedad del sistema:
```bash
java -Diidr.properties=/otro/path/iidr.properties ...
```

---

### 4.2 `IIDRConnection` — Conexión al Access Server

**Fichero:** `src/main/java/com/mapfre/cdc/admin/IIDRConnection.java`

Wrapper sobre el `EmbeddedScript` de IBM IIDR. Implementa `AutoCloseable` para garantizar el cierre limpio de la conexión con `try-with-resources`.

#### Métodos principales

| Método | Descripción |
|--------|-------------|
| `connect()` | Conecta al Access Server usando las credenciales de `iidr.properties` |
| `execute(command)` | Ejecuta un comando CHCCLP y retorna el resultado bruto |
| `executeForTable(command)` | Ejecuta y retorna `ResultStringTable` |
| `executeForMessage(command)` | Ejecuta y retorna el mensaje de texto |
| `executeForKeyValues(command)` | Ejecuta y retorna un mapa clave→valor (para `show` commands) |
| `disconnect()` | Desconecta del Access Server |
| `close()` | Cierra el EmbeddedScript (AutoCloseable) |
| `getSettings()` | Retorna el objeto Settings (para acceso a propiedades por los comandos) |

#### Encriptación de contraseñas

Las contraseñas en `iidr.properties` se almacenan encriptadas usando el `Encryptor` de IBM CDC. Si se introduce una contraseña en texto plano, la clase la detecta, la encripta y sobreescribe el fichero automáticamente.

#### TLS

La librería IIDR **no usa** las propiedades estándar `javax.net.ssl.*`. `iidr.sh` pasa el truststore como propiedades del sistema y también apunta el directorio de configuración al Access Server, donde la librería lee su propio `tls.properties`. Ver sección [11](#11-tls--notas-importantes).

---

### 4.3 `Command` y `CommandResult` — Modelo de comandos

**Ficheros:** `src/main/java/com/mapfre/cdc/admin/commands/Command.java` y `CommandResult.java`

`Command` es una interfaz con un único método:
```java
CommandResult execute(IIDRConnection connection, String[] args) throws Exception;
```

`CommandResult` encapsula el resultado de cualquier operación:

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `status` | `OK / SKIPPED / ERROR` | Resultado de la operación |
| `operation` | `String` | Nombre de la operación (ej. `subscription.create`) |
| `data` | `Object` | Payload de la respuesta (se serializa a JSON) |
| `message` | `String` | Mensaje descriptivo (especialmente en SKIPPED y ERROR) |
| `exitCode` | `int` | 0 para OK/SKIPPED, 1 para ERROR |

---

### 4.4 `OutputFormatter` — Serialización JSON

**Fichero:** `src/main/java/com/mapfre/cdc/admin/OutputFormatter.java`

Serializa el `CommandResult` a JSON usando Jackson `ObjectMapper` y lo imprime por **stdout**. Los logs de aplicación van por **stderr** (Logback), lo que garantiza que Ansible siempre recibe JSON limpio en stdout.

---

### 4.5 `TableSubscriptionRegistry` — Registro CSV

**Fichero:** `src/main/java/com/mapfre/cdc/admin/registry/TableSubscriptionRegistry.java`

Mantiene un fichero CSV local (`conf/table_subscription_registry.csv`) con la relación entre tablas y suscripciones. Permite responder rápidamente a la pregunta "¿en qué suscripciones está esta tabla?" sin tener que consultar el Access Server.

#### Formato del CSV

```csv
table,subscription,datastore
SCHEMA.CLIENTES,SUB_DB2_KAFKA,IIDR_GR_DB2_E01_AWS_DEV
SCHEMA.PEDIDOS,SUB_DB2_KAFKA,IIDR_GR_DB2_E01_AWS_DEV
SCHEMA.PRODUCTOS,SUB_DB2_KAFKA_2,IIDR_GR_DB2_E01_AWS_DEV
```

#### Métodos

| Método | Descripción |
|--------|-------------|
| `add(table, subscription, datastore)` | Añade una entrada (si no existe ya) |
| `remove(table, subscription)` | Elimina una entrada |
| `findByTable(table)` | Devuelve lista de suscripciones donde está la tabla |
| `syncSubscription(subscription, tables, datastore)` | Reemplaza todas las entradas de una suscripción |
| `findAll()` | Devuelve todas las entradas |

Todos los métodos son `synchronized` para seguridad ante concurrencia. Las búsquedas son case-insensitive.

---

## 5. Comandos disponibles

### 5.1 Datastores

| Comando | Argumentos | Descripción |
|---------|-----------|-------------|
| `datastore list` | — | Lista datastores **visibles para el usuario autenticado** (`list datastores` de CHCCLP — solo devuelve los que tienen access connection) |
| `datastore show` | `--name <nombre>` | Detalle completo de un datastore |
| `datastore create` | `--name <n> --host <h> --port <p> [--description <d>]` | Registra un nuevo datastore (idempotente). Provisioning — ejecutar como **APP-cdcadmin** |
| `datastore delete` | `--name <nombre>` | Elimina un datastore (idempotente). Provisioning — ejecutar como **APP-cdcadmin** |
| `datastore grant` | `--name <n> --user <u>` + env `DB_USER_ENC` + `DB_PASSWORD_ENC` | Concede access connection sobre `<n>` al usuario `<u>` del AS. Idempotente. Provisioning — ejecutar como **APP-cdcadmin**. Ver sección [10.2](#102-comando-datastore-grant) para detalles |

---

### 5.2 Suscripciones

| Comando | Argumentos | Descripción |
|---------|-----------|-------------|
| `subscription list` | `--source <datastore>` | Lista suscripciones con su estado de replicación |
| `subscription show` | `--name <n> --source <ds>` | Detalle completo + estado actual |
| `subscription create` | `--name <n> --source <ds> --target <dt> [--description <d>]` | Crea suscripción (idempotente) |
| `subscription delete` | `--name <n> --source <ds>` | Para (si activa) y elimina (idempotente) |
| `subscription start` | `--name <n> --source <ds>` | Arranca replicación continua (SKIPPED si ya en Mirror) |
| `subscription stop` | `--name <n> --source <ds> [--method normal\|immediate\|abort]` | Para replicación (SKIPPED si ya parada) |
| `subscription lock` | `--name <n> --source <ds> --target <dt>` | Bloquea para edición exclusiva (idempotente) |
| `subscription unlock` | `--name <n> --source <ds> --target <dt>` | Desbloquea (idempotente) |
| `subscription reinit` | `--name <n> --source <ds>` | Reinicializa (refresh completo de datos) |

---

### 5.3 Mappings

| Comando | Argumentos | Descripción |
|---------|-----------|-------------|
| `mapping list` | `--name <sub> --source <ds>` | Lista tablas mapeadas en la suscripción |
| `mapping add` | `--name <sub> --source <ds> --table <schema.tabla> [--target-table <schema.tabla>]` | Añade mapping de tabla (idempotente) |
| `mapping remove` | `--name <sub> --source <ds> --table <schema.tabla>` | Elimina mapping (idempotente) |
| `mapping find-by-table` | `--table <schema.tabla> [--live-check]` | Busca en qué suscripciones está la tabla |

> **`--live-check`**: si se especifica, consulta el Access Server para sincronizar el CSV local antes de responder. Sin él, responde directamente del registro CSV (más rápido).

---

### 5.4 Operaciones compuestas

Todas son idempotentes: verifican el estado actual en cada paso y retornan `SKIPPED` para los pasos que ya están completados.

#### `composite setup`

Crea en un solo paso: datastores + suscripción + mappings de tablas.

```bash
bin/iidr.sh composite setup --config conf/setup.json
```

Ver sección [10](#10-formato-json-para-operaciones-compuestas) para el formato del JSON.

---

#### `composite teardown`

Para y elimina una suscripción (y opcionalmente los datastores).

```bash
bin/iidr.sh composite teardown \
  --name SUB_DB2_KAFKA \
  --source IIDR_GR_DB2_E01_AWS_DEV \
  --target IIDR_GR_KAFKA_E01_AWS_DEV \
  [--delete-datastores]
```

Orden de ejecución: stop → delete subscription → delete datastores (opcional).

---

#### `composite restart`

Para la suscripción, espera a que esté completamente parada y la vuelve a arrancar. Usa polling con timeout configurable.

```bash
bin/iidr.sh composite restart \
  --name SUB_DB2_KAFKA \
  --source IIDR_GR_DB2_E01_AWS_DEV \
  [--timeout 120]
```

Polling cada 3 segundos. Timeout por defecto: 60 segundos.

---

#### `composite health-check`

Verifica el estado de todos los datastores y suscripciones. Retorna `OK` si todo está en `Mirror Continuous`, `ERROR` si alguno no está saludable.

```bash
bin/iidr.sh composite health-check [--source IIDR_GR_DB2_E01_AWS_DEV]
```

---

## 6. Configuración

### 6.1 `iidr.properties`

```properties
# Access Server
asHostName=lesawsew131211.es.mapfre.net
asPort=10101
asUserName=APP-cdcauto
asPassword=<encriptado por IBM Encryptor>

# Registro local de tabla→suscripción
registryFile=conf/table_subscription_registry.csv
```

> **Usuario `APP-cdcauto`**: usuario **operacional**. Día a día de subscriptions, mappings y monitorización. Distinto de `APP-cdcmon` (solo monitorización, en `metricas`).

#### Override por ejecución: `IIDR_PROPERTIES_FILE`

Si esta env var está definida y apunta a un fichero legible, `iidr.sh`
la propaga al JVM como `-Diidr.properties=<ruta>` y `IIDRConnection`
la carga en lugar del `conf/iidr.properties` por defecto. Sin la env
var, comportamiento idéntico al anterior.

Caso de uso: ejecuciones puntuales con otro usuario (típicamente
`APP-cdcadmin` para provisioning), sin tocar el `iidr.properties` de
producción. Ver patrón en sección [10.1 — Patrón APP-cdcadmin → APP-cdcauto](#101-patrón-app-cdcadmin--app-cdcauto).

```bash
IIDR_PROPERTIES_FILE=/tmp/iidr-cdcadmin.properties \
  bin/iidr.sh datastore create --name MY_DS --host ... --port ...
```

Si el fichero no existe o no es legible, `iidr.sh` aborta con exit 10
antes de tocar el JVM.

#### Convención Mapfre de usuarios

| Usuario | Rol | Comandos típicos |
|---|---|---|
| `APP-cdcadmin` | Provisioning — lifecycle del datastore | `datastore create`, `datastore delete`, `datastore grant` |
| `APP-cdcauto`  | Operación — lifecycle de subscriptions | `subscription *`, `mapping *`, `datastore list` |
| `APP-cdcmon`   | Monitorización (proyecto `metricas`) | — |

`iidr.properties` por defecto lleva `APP-cdcauto`. Para usar `APP-cdcadmin`,
el invocador (típicamente `iidr-install` desde AAP) genera un
`.properties` temporal con sus credenciales y lo pasa por
`IIDR_PROPERTIES_FILE`.

---

### 6.2 `tls.properties` y truststore

La librería IIDR no usa `javax.net.ssl.*` estándar. Lee su propio fichero `tls.properties` desde el directorio del Access Server. `iidr.sh` gestiona esto pasando tanto las propiedades estándar del JVM como el `-Duser.folder` necesario.

No es necesario editar `tls.properties` manualmente — está gestionado por `iidr.sh`.

El fichero `cdc_truststore_AccessServer.p12` contiene la CA raíz e intermedia de Mapfre que firmaron el certificado del Access Server.

---

### 6.3 `logback.xml`

Configuración de logging con **SLF4J + Logback** (a diferencia del proyecto `metricas` que usa Log4j2).

- Los logs van a `log/iidr-admin.log` y a consola (stderr)
- Nivel por defecto: `INFO`
- Rotación diaria, retención de 7 días

---

## 7. Ejecución

### 7.1 Requisitos previos

- **Java 17** (se usa el JRE del IIDR en `/u01/IIDR/AccessServer/jre64/`)
- **IBM IIDR Access Server** instalado en `/u01/IIDR/AccessServer/`
- Usuario `APP-cdcauto` con permisos de System Administrator en el Access Server
- El JAR compilado en `target/iidr-admin-1.0-SNAPSHOT.jar`

### 7.2 Compilación

```bash
cd /home/jpfrut2/aplicaciones-iidr/metricas/iidr-admin
mvn -DskipTests package
```

### 7.3 Uso del CLI

```bash
# Listar datastores
bin/iidr.sh datastore list

# Crear un datastore
bin/iidr.sh datastore create --name IIDR_GR_DB2_E01_AWS_DEV --host db2host.es.mapfre.net --port 11701

# Crear una suscripción
bin/iidr.sh subscription create \
  --name SUB_DB2_KAFKA \
  --source IIDR_GR_DB2_E01_AWS_DEV \
  --target IIDR_GR_KAFKA_E01_AWS_DEV

# Añadir una tabla
bin/iidr.sh mapping add \
  --name SUB_DB2_KAFKA \
  --source IIDR_GR_DB2_E01_AWS_DEV \
  --table SCHEMA.CLIENTES

# Arrancar replicación
bin/iidr.sh subscription start \
  --name SUB_DB2_KAFKA \
  --source IIDR_GR_DB2_E01_AWS_DEV

# Ver en qué suscripciones está una tabla
bin/iidr.sh mapping find-by-table --table SCHEMA.CLIENTES

# Setup completo desde JSON
bin/iidr.sh composite setup --config conf/setup.json

# Health check
bin/iidr.sh composite health-check
```

### 7.4 Perfil motor (host sin Access Server)

`iidr-admin` puede ejecutarse desde un host **que sólo tiene motor IIDR** (DB2
z/OS, Kafka, etc.) y **no** tiene Access Server instalado. Caso de uso:
`cdc_instance_mgmt.sh` (servicio systemd que arranca la instancia del motor)
necesita lanzar las subscriptions tras un reboot — para ello invoca a
`iidr-admin` localmente y `iidr-admin` habla por red al AS remoto.

#### Cómo se llega a este estado

El propio `bin/deploy.sh` deja todo lo necesario:

- `${DEPLOY_ROOT}/iidr-cli-libs/lib/` con `server.jar`, `chcclp.jar`, `api.jar`
  (copiados desde el AS donde se lanzó el deploy).
- `${DEPLOY_ROOT}/bin/iidr.sh` con resolución de Java en cascada (ver más
  abajo) y todos los scripts y JARs de la aplicación.
- `${DEPLOY_ROOT}/conf/iidr.properties` ya renderizado con la URL del AS
  del entorno (`asHostName=LESAWSEW141142.es.mapfre.net` para PRE).

Todo eso vive en el NFS `/u01/repo/automation/iidr-ctl/`, por lo que cualquier
host motor con NFS montado lo ve directamente, sin copiar nada al disco local.

#### Resolución de Java en `bin/iidr.sh`

`iidr.sh` resuelve el binario `java` en este orden:

1. `${IIDR_JAVA}` si está exportado y es ejecutable (override explícito).
2. `${IIDR_HOME}/jre64/jre/bin/java` (perfil host AS — JRE del AS).
3. `${JAVA_HOME}/bin/java` (perfil host motor — JDK que `iidr-install`
   ya instala en `/opt/ibm-semeru-jdk-17` en su paso 1).
4. Si ninguno funciona → error con las tres opciones listadas.

Sólo la resolución de Java se desacopla. `IIDR_HOME` sigue siendo la fuente
de los JARs del AS (`lib/server.jar`, `lib/chcclp.jar`).

#### Invocación desde un host motor

```bash
IIDR_HOME=/u01/repo/automation/iidr-ctl/iidr-cli-libs \
JAVA_HOME=/opt/ibm-semeru-jdk-17 \
TLS_TRUST_STORE_PASSWORD='<pwd truststore>' \
/u01/repo/automation/iidr-ctl/bin/iidr.sh subscription list \
  --source IIDR_GR_DB2_DB2_E01_AWS_PRE
```

Devuelve JSON válido (puede ser lista vacía si la instancia todavía no tiene
subscriptions creadas).

---

## 8. Integración con Ansible

### 8.1 Contrato de salida JSON

**Éxito (exit 0, status OK):**
```json
{
  "status": "OK",
  "operation": "subscription.create",
  "data": { "name": "SUB_DB2_KAFKA", "source": "IIDR_GR_DB2_E01_AWS_DEV" },
  "message": null
}
```

**Ya existía (exit 0, status SKIPPED):**
```json
{
  "status": "SKIPPED",
  "operation": "subscription.create",
  "data": null,
  "message": "La suscripción SUB_DB2_KAFKA ya existe"
}
```

**Error (exit 1):**
```json
{
  "status": "ERROR",
  "operation": "subscription.create",
  "data": { "errorCode": -1006 },
  "message": "No se puede conectar al datastore IIDR_GR_DB2_E01_AWS_DEV"
}
```

### 8.2 Exit codes

| Exit code | Significado |
|-----------|-------------|
| `0` | OK o SKIPPED — Ansible lo trata como éxito |
| `1` | ERROR — Ansible puede hacer `failed_when` |

### 8.3 Ejemplo de invocación desde playbook

```yaml
- name: Crear suscripción CDC
  command: >
    /home/jpfrut2/aplicaciones-iidr/metricas/iidr-admin/bin/iidr.sh
    subscription create
    --name {{ subscription_name }}
    --source {{ source_datastore }}
    --target {{ target_datastore }}
  register: result
  changed_when: "'\"status\": \"OK\"' in result.stdout"

- name: Mostrar resultado
  debug:
    msg: "{{ result.stdout | from_json }}"
```

> **Seguridad:** Las contraseñas en `iidr.properties` están encriptadas. Para gestión via Ansible Vault, se puede sobreescribir el path del fichero de properties con `-Diidr.properties`.

### 8.4 Contrato AAP para `deploy.sh`

El despliegue de `iidr-admin` al NFS (`/u01/repo/automation/iidr-ctl/`) está
pensado para ejecutarse desde **Ansible Automation Platform**. El job
template debe inyectar las credenciales del vault corporativo como
variables de entorno antes de invocar `bin/deploy.sh --target <env>`.

#### Variables de entorno esperadas

| Variable | Obligatoria | Origen típico (AAP) | Uso |
|---|---|---|---|
| `AS_PASSWORD_ENC` | Sí¹ | Vault → credencial "APP-cdcauto encrypted" | Cadena hex de la pwd de `APP-cdcauto` ya cifrada con IBM Encryptor. Se mete tal cual en `conf/iidr.properties`. |
| `AS_PASSWORD` | Sí¹ | Vault → credencial "APP-cdcauto plain" | Alternativa a la anterior: pwd en claro. `deploy.sh` la cifra con `bin/iidr-encrypt.sh`. |
| `TLS_TRUST_STORE_PASSWORD` | Sí² | Vault → credencial "truststore" | Password del truststore PKCS12. Sólo se usa cuando hay que **construir** el truststore (no existe ya en destino). |
| `AS_HOSTNAME` | No | Override opcional | Por defecto se deriva del `--target`. |
| `AS_PORT` | No | Override opcional | Por defecto `10101`. |
| `AS_USERNAME` | No | Override opcional | Por defecto `APP-cdcauto`. |

¹ Exactamente **una** de `AS_PASSWORD_ENC` o `AS_PASSWORD`. Si no llega
ninguna, `deploy.sh` aborta con exit `9` y mensaje explicativo. Esto es
intencionado: en AAP, una credencial mal mapeada en el job template debe
marcar el job como `failed`, no producir un despliegue silenciosamente
incompleto.

² Estrictamente sólo si el truststore no existe ya en destino. En la
práctica, expórtala siempre desde AAP para que un primer despliegue sobre
un host limpio no falle por falta de password.

#### Comando equivalente

```bash
AS_PASSWORD_ENC='<hex de vault>' \
TLS_TRUST_STORE_PASSWORD='<pwd truststore de vault>' \
  bin/deploy.sh --target pre
```

#### Flag de escape `AS_RENDER_SKIP=1`

Para refrescar sólo scripts/jars sin tocar las credenciales del destino
(caso de un humano parcheando un bug en `iidr.sh`, no AAP):

```bash
AS_RENDER_SKIP=1 bin/deploy.sh --target pre
```

No debe usarse desde un job template de AAP — la ausencia de credenciales
en AAP debe ser un error, no un fallback.

---

## 9. Idempotencia

Todas las operaciones siguen el patrón **desired-state**: verifican el estado actual antes de actuar.

| Escenario | Comportamiento | Exit code |
|-----------|---------------|-----------|
| Crear algo que no existe | Crea → retorna `OK` | 0 |
| Crear algo que ya existe | No hace nada → retorna `SKIPPED` | 0 |
| Eliminar algo que existe | Elimina → retorna `OK` | 0 |
| Eliminar algo que no existe | No hace nada → retorna `SKIPPED` | 0 |
| Start de suscripción ya en Mirror | No hace nada → retorna `SKIPPED` | 0 |
| Stop de suscripción ya parada | No hace nada → retorna `SKIPPED` | 0 |
| Fallo real | Retorna `ERROR` con mensaje | 1 |

Esto permite ejecutar los mismos playbooks de Ansible repetidamente sin efecto secundario.

---

## 10. Provisioning desde `iidr-install`

`iidr-install` (proyecto hermano) instala el motor IIDR de un datastore
en un host y luego necesita **registrar ese datastore en el Access
Server** y **dar acceso a `APP-cdcauto`** sobre él. Esas dos acciones se
hacen invocando a `iidr-admin`. Esta sección documenta el contrato.

### 10.1 Patrón APP-cdcadmin → APP-cdcauto

El usuario por defecto en `conf/iidr.properties` (`APP-cdcauto`) **no puede**
crear datastores ni conceder access connections — esos son comandos de
administración. Los provisiona `APP-cdcadmin`.

Ver tabla de convención de usuarios en [§6.1](#61-iidrproperties).

Implementación: `iidr-install` genera un `.properties` temporal con
`asUserName=APP-cdcadmin` y su pwd cifrada, lo expone vía
`IIDR_PROPERTIES_FILE`, lanza los comandos de provisioning, y borra el
fichero. El `iidr.properties` de producción (con `APP-cdcauto`) **no se
toca**.

### 10.2 Comando `datastore grant`

Concede "access connection" a un usuario del AS sobre un datastore.
Equivalente CHCCLP:

```
add access connection username <user> datastore "<ds>"
                       dbUser <dbUser> dbPassword "<pwd>"
```

**Argumentos**:

| Flag | Tipo | Descripción |
|---|---|---|
| `--name` | obligatorio | Nombre del datastore que se está concediendo |
| `--user` | obligatorio | Usuario del AS receptor del grant (típicamente `APP-cdcauto`) |

**Env vars adicionales** (obligatorias, **cifradas con IBM Encryptor**):

| Env var | Origen | Significado |
|---|---|---|
| `DB_USER_ENC` | Vault → ya conocido por `iidr-install` | Usuario técnico del motor IIDR (p.ej. `APPCDCL`). Es el `dbUser` del comando CHCCLP. CHCCLP exige este parámetro (rc -2004 si falta). |
| `DB_PASSWORD_ENC` | Vault | Password de `DB_USER_ENC`. |

**Por qué van cifradas**: llegan así desde el vault corporativo (mismo
patrón que `AS_PASSWORD_ENC`). Se descifran en Java con
`Encryptor.decodeAndDecrypt()` (la API del IBM Encryptor de IIDR) justo
antes de pasarlas a CHCCLP. Nunca se loguean en claro.

**Idempotencia**: si el usuario ya tiene access connection al datastore,
devuelve `SKIPPED` sin pedir las env vars (se chequea antes con
`list access connections username <user>`).

**Validaciones**:

- Datastore debe existir en el AS (chequeo con `list access datastores`,
  no `list datastores` — éste último devuelve `[]` para usuarios sin
  access connection y daría falsos negativos).
- El usuario `<user>` **no** se valida explícitamente: el comando
  `list users` de CHCCLP da `EmbeddedScriptException: null` en este
  entorno. Si el usuario no existe, el `add access connection` final
  falla con mensaje semántico claro de CHCCLP que se propaga como
  `ERROR`.

**Alcance actual**: el comando asume datastores con `dbUser` (caso DB2
z/OS y similares). Cuando se incorpore el datastore Kafka, habrá que
ver si CHCCLP exige también dbUser/dbPassword o usa otra autenticación
(SASL/SSL). Posible generalización futura.

### 10.3 Flujo completo desde `iidr-install`

```bash
# 1. Render properties temporal con APP-cdcadmin
cat > /tmp/iidr-cdcadmin.properties <<EOF
asHostName=LESAWSEW141142.es.mapfre.net
asPort=10101
asUserName=APP-cdcadmin
asPassword=<cifrado_cdcadmin>
registryFile=conf/table_subscription_registry.csv
EOF
chmod 600 /tmp/iidr-cdcadmin.properties

# 2. Crear el datastore en el AS (provisioning, como APP-cdcadmin)
IIDR_HOME=/u01/repo/automation/iidr-ctl/iidr-cli-libs \
JAVA_HOME=/opt/ibm-semeru-jdk-17 \
TLS_TRUST_STORE_PASSWORD='<pwd_truststore>' \
IIDR_PROPERTIES_FILE=/tmp/iidr-cdcadmin.properties \
  /u01/repo/automation/iidr-ctl/bin/iidr.sh datastore create \
    --name IIDR_GR_DB2_DB2_E01_AWS_PRE \
    --host LESAWSEW141140 \
    --port 11801

# 3. Conceder access connection a APP-cdcauto (también como APP-cdcadmin)
IIDR_HOME=/u01/repo/automation/iidr-ctl/iidr-cli-libs \
JAVA_HOME=/opt/ibm-semeru-jdk-17 \
TLS_TRUST_STORE_PASSWORD='<pwd_truststore>' \
IIDR_PROPERTIES_FILE=/tmp/iidr-cdcadmin.properties \
DB_USER_ENC='<hex_APPCDCL>' \
DB_PASSWORD_ENC='<hex_pwd_motor>' \
  /u01/repo/automation/iidr-ctl/bin/iidr.sh datastore grant \
    --name IIDR_GR_DB2_DB2_E01_AWS_PRE \
    --user APP-cdcauto

# 4. Limpia el properties temporal
rm -f /tmp/iidr-cdcadmin.properties

# 5. Verificar que APP-cdcauto ya ve el datastore (sin IIDR_PROPERTIES_FILE,
#    usa el conf/iidr.properties de producción con APP-cdcauto)
IIDR_HOME=/u01/repo/automation/iidr-ctl/iidr-cli-libs \
JAVA_HOME=/opt/ibm-semeru-jdk-17 \
TLS_TRUST_STORE_PASSWORD='<pwd_truststore>' \
  /u01/repo/automation/iidr-ctl/bin/iidr.sh datastore list
# → JSON con IIDR_GR_DB2_DB2_E01_AWS_PRE en la lista
```

Todos los pasos son **idempotentes**: relanzar el flujo no rompe nada,
los `create` y `grant` devuelven `SKIPPED` si ya estaban hechos.

---

## 11. Formato JSON para operaciones compuestas

El comando `composite setup` lee un fichero JSON de configuración:

```json
{
  "source_datastore": {
    "name": "IIDR_GR_DB2_E01_AWS_DEV",
    "host": "db2host.es.mapfre.net",
    "port": 11701,
    "description": "DB2 origen AWS DEV"
  },
  "target_datastore": {
    "name": "IIDR_GR_KAFKA_E01_AWS_DEV",
    "host": "kafkahost.es.mapfre.net",
    "port": 11702,
    "description": "Kafka destino AWS DEV"
  },
  "subscription": {
    "name": "SUB_DB2_KAFKA_CLIENTES",
    "description": "Replicación tablas de clientes"
  },
  "tables": [
    {
      "source": "SCHEMA.CLIENTES",
      "target": "SCHEMA.CLIENTES"
    },
    {
      "source": "SCHEMA.PEDIDOS"
    }
  ]
}
```

Notas:
- Si `target` de una tabla se omite, se usa el mismo valor que `source`
- El JSON es extensible: se pueden añadir campos futuros (`columns`, `row_filter`, `transformations`) sin romper la compatibilidad

---

## 12. TLS — Notas importantes

La librería IIDR (`chcclp.jar`) **no usa** las propiedades estándar del JVM `javax.net.ssl.trustStore`. Tiene su propio mecanismo de TLS que lee desde el directorio del Access Server.

### Solución implementada

`iidr.sh` pasa ambos mecanismos en paralelo:

```bash
java \
  -Djavax.net.ssl.trustStore="${TLS_TRUST_STORE}" \
  -Djavax.net.ssl.trustStorePassword="${TLS_TRUST_STORE_PASSWORD}" \
  -Djavax.net.ssl.trustStoreType="${TLS_TRUST_STORE_TYPE}" \
  -Dlogback.configurationFile="${CONF_DIR}/logback.xml" \
  -cp "${CP}" com.mapfre.cdc.admin.IIDRAdmin "$@"
```

El truststore se sobreescribe vía variables de entorno si es necesario:
```bash
export TLS_TRUST_STORE=/ruta/alternativa/truststore.p12
export TLS_TRUST_STORE_PASSWORD=contraseña
```

### Si se renueva el certificado

Solo hay que actualizar si cambia la CA firmante. Si renueva el certificado del servidor pero la CA es la misma, no hay que hacer nada. Si cambia la CA:
1. Infraestructura proporciona nuevo `cdc_truststore_AccessServer.p12`
2. Sustituir en `conf/`
3. Actualizar contraseña en `iidr.sh` si ha cambiado

---

## 13. Tecnologías y dependencias

| Tecnología | Versión | Uso |
|-----------|---------|-----|
| Java | 17 | Lenguaje de programación |
| Maven | 3.x | Build y gestión de dependencias |
| IBM IIDR server.jar | 11.4.0.5-11213 | Librería de conexión al Access Server |
| IBM IIDR chcclp.jar | 11.4.0.5-11213 | API CHCCLP (EmbeddedScript) |
| Apache Commons CLI | 1.11.0 | Parseo de argumentos de línea de comandos |
| Apache Commons Configuration | 2.13.0 | Gestión de ficheros .properties |
| SLF4J | 2.x | Fachada de logging |
| Logback | 1.5.x | Implementación de logging (sustituye a Log4j2) |
| Jackson Databind | 2.x | Serialización JSON |
| JUnit | 4.11 | Tests (sin cobertura actual) |
