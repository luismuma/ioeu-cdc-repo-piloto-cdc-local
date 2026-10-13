# iidr-admin — Contexto para Claude Code

> **Contexto rápido** para agentes Claude Code que aterricen en este
> proyecto. La documentación oficial y exhaustiva vive en
> [`docs/DOCUMENTACION.md`](docs/DOCUMENTACION.md) — usa este `CLAUDE.md`
> como mapa de alto nivel y salta a la sección correcta del DOCUMENTACION
> cuando necesites profundizar.

## 1. Qué es este proyecto

CLI Java que **automatiza la administración de IBM IIDR (InfoSphere
Data Replication)**: alta/baja de datastores, subscriptions y mappings
contra un Access Server. Diseñado para consumirse desde Ansible
Automation Platform (AAP) en MAPFRE.

Contrato de salida estable: **JSON a stdout**, **logs a stderr**, exit
0 (OK / SKIPPED) o 1 (ERROR). Todas las operaciones son **idempotentes**:
crear algo que ya existe → `SKIPPED`, no falla.

## 2. Arquitectura mínima

| Componente | Path | Responsabilidad |
|---|---|---|
| Dispatcher | [src/main/java/com/mapfre/cdc/admin/IIDRAdmin.java](src/main/java/com/mapfre/cdc/admin/IIDRAdmin.java) | Mapea `<entidad>.<operación>` → clase `Command`. Lee `-Diidr.properties` para localizar credenciales. |
| Conexión al AS | [src/main/java/com/mapfre/cdc/admin/IIDRConnection.java](src/main/java/com/mapfre/cdc/admin/IIDRConnection.java) | Wrapper sobre `EmbeddedScript` (chcclp.jar). Métodos `execute`, `executeForKeyValues`, `executeForMessage`. Helper público estático `quoteStringLiteral(String)` para escapar literales CHCCLP. |
| Modelo de comandos | `src/.../commands/Command.java`, `CommandResult.java` | `Command.execute(connection, args) → CommandResult`. Status: OK / SKIPPED / ERROR. |
| Serializador | `src/.../OutputFormatter.java` | Jackson → JSON a stdout. |
| Registro local | `src/.../registry/TableSubscriptionRegistry.java` | CSV en `conf/table_subscription_registry.csv` (mapping tabla→subscription, sincronización local). |

**Comandos** viven en `src/.../commands/{datastore,subscription,mapping,composite}/`. Patrón uniforme:
parsean args con Apache Commons CLI, verifican estado (idempotencia),
ejecutan CHCCLP, devuelven `CommandResult`.

## 3. Comandos disponibles

Lista completa con `bin/iidr.sh --help` (no levanta JVM, no exige TLS).

- **datastore** (5): `list`, `show`, `create`, `delete`, `grant`
- **subscription** (9): `list`, `show`, `create`, `delete`, `start`, `stop`, `lock`, `unlock`, `reinit`
- **mapping** (5): `list`, `add`, `remove`, `find-by-table`, `refresh`
- **composite** (4): `setup`, `teardown`, `restart`, `health-check`

## 4. Patrón Mapfre de usuarios

Los usuarios del Access Server están **integrados con LDAP**: el rol IIDR lo
determina la pertenencia a un grupo LDAP, no la gestión interna de IIDR. Por
política Mapfre todos llevan prefijo **`APP-`**. La integración LDAP del AS la
provee `iidr-install`; iidr-admin **asume** que el AS ya está integrado.

| Usuario | Rol (grupo LDAP) | Comandos típicos |
|---|---|---|
| `APP-cdcadmin` | sysadmin — provisioning (lifecycle del datastore) | `datastore create`, `datastore delete`, `datastore grant` |
| `APP-cdcauto`  | sysadmin — operación/automatismos (lifecycle de subscriptions) | `subscription *`, `mapping *`, `datastore list` |
| `APP-cdcmon`   | monitor (proyecto hermano `metricas`) | — |

El `conf/iidr.properties` desplegado en NFS lleva **siempre `APP-cdcauto`**.
Para invocar comandos de provisioning, el consumidor genera un
`.properties` temporal con `APP-cdcadmin` y lo pasa por
`IIDR_PROPERTIES_FILE=...`. Ver §10.3 de
[docs/DOCUMENTACION.md](docs/DOCUMENTACION.md#103-flujo-completo-desde-iidr-install).

> **Migración por entorno**: PRE ya tiene el AS integrado con LDAP (usuarios
> `APP-*`). DEV y PRO aún no; hasta que `iidr-install` los integre, allí se usa
> el nombre sin prefijo (`cdcauto`). El username es parametrizable por entorno
> (`AS_USERNAME` en `deploy.sh`), no cablear `APP-*` de forma rígida.

## 5. Convenciones de código

- **Idempotencia obligatoria**. Cada `Command` verifica estado actual
  antes de actuar. Si el estado deseado ya existe → `CommandResult.skipped(...)`.
  Ejemplo: `DatastoreCreate` mira `list access datastores` antes de
  `add access datastore`.
- **Literales CHCCLP**: cualquier valor que pueda contener caracteres
  especiales (passwords, descripciones libres) debe pasar por
  `IIDRConnection.quoteStringLiteral(value)`. Devuelve el string
  entrecomillado con `"..."` o `'...'` según contenido — los call-sites
  **no añaden comillas a mano**.
- **Errores**: catch `EmbeddedScriptException`, log a stderr, devolver
  `CommandResult.error(operation, msg)`. RCs 1312 / 1314 / 1322 son
  informativos de IIDR, no son fallos — `IIDRConnection.executeForMessage`
  los absorbe.
- **Sin comentarios obvios**. Comentar solo el *por qué* cuando es no obvio
  (workarounds, gotchas de CHCCLP, etc.). El código habla por sí mismo.

## 6. Build y deploy

```bash
# 1. Compilar
mvn -DskipTests package

# 2. Deploy a PRE (desde host con AS local, p.ej. LESAWSEW141142)
AS_PASSWORD_ENC='<hex>' \
TLS_TRUST_STORE_PASSWORD='<pwd>' \
  bin/deploy.sh --target pre

# Variante: refrescar solo binarios sin tocar credenciales
AS_RENDER_SKIP=1 \
TLS_TRUST_STORE_PASSWORD='<pwd>' \
  bin/deploy.sh --target pre
```

`deploy.sh` hace:

1. `mvn package` (si hace falta).
2. Staging temporal en `/tmp/iidr-admin-deploy-*`.
3. Renderiza `conf/iidr.properties` con credenciales del entorno
   (`cdcauto` cifrada con IBM Encryptor).
4. Empaqueta **los 25 JARs** de `/u01/IIDR/AccessServer/lib/` bajo
   `iidr-cli-libs/lib/` — necesarios para que hosts motor (sin AS)
   puedan invocar `iidr.sh`.
5. Construye el truststore PKCS12 **solo si no existe** ya en destino
   (preserva el que dejara `iidr-install` previamente).
6. rsync a `/u01/repo/automation/iidr-ctl/` (local o vía SSH).

**Destino del deploy**: `/u01/repo/automation/iidr-ctl/` (NFS visible
en todos los hosts del entorno).

## 7. Env vars

### Deploy (consume `bin/deploy.sh`)

| Var | Obligatoria | Propósito |
|---|---|---|
| `AS_PASSWORD_ENC` | Sí¹ | Pwd de APP-cdcauto ya cifrada con IBM Encryptor |
| `AS_PASSWORD` | Sí¹ | Pwd de APP-cdcauto en claro (deploy.sh la cifra) |
| `TLS_TRUST_STORE_PASSWORD` | Sí² | Pwd truststore (solo si hay que construirlo) |
| `AS_RENDER_SKIP=1` | No | Saltar render del iidr.properties (refresco rápido) |
| `AS_HOSTNAME`, `AS_PORT`, `AS_USERNAME`, `AS_CERT_DIR` | No | Overrides — defaults por target |

¹ Exactamente una (`_ENC` o claro). Si faltan ambas y no hay `AS_RENDER_SKIP=1` → `exit 9`.
² Estrictamente solo si truststore no existe en destino. En la práctica, exportar siempre.

### Runtime (consume `bin/iidr.sh`)

| Var | Obligatoria | Propósito |
|---|---|---|
| `TLS_TRUST_STORE_PASSWORD` | Sí | JVM truststore password |
| `IIDR_HOME` | No | Default `/u01/IIDR/AccessServer` (AS) o `/u01/repo/automation/iidr-ctl/iidr-cli-libs` (motor) |
| `JAVA_HOME` / `IIDR_JAVA` | No | Resolución de Java en cascada (3 fallbacks) |
| `IIDR_PROPERTIES_FILE` | No | Override de `conf/iidr.properties` (provisioning con APP-cdcadmin) |
| `DB_USER_ENC` | Solo `datastore grant` | dbUser técnico del motor IIDR, cifrado |
| `DB_PASSWORD_ENC` | Solo `datastore grant` | dbPassword cifrada |

`bin/iidr.sh --help` lista todo lo anterior sin levantar JVM.

## 8. Perfil host AS vs host motor

| Perfil | Hosts (PRE) | `IIDR_HOME` | Java |
|---|---|---|---|
| Host AS | `LESAWSEW141142` | `/u01/IIDR/AccessServer` (default) | JRE del AS (default) |
| Host motor | `LESAWSEW141140`, `LESAWSEW141141` | `/u01/repo/automation/iidr-ctl/iidr-cli-libs` | `/opt/ibm-semeru-jdk-17` |

El **deploy** se lanza siempre desde un host con AS (necesita leer los
25 JARs locales). El **runtime** funciona en cualquiera de los dos perfiles.

Ejemplo desde host motor:

```bash
IIDR_HOME=/u01/repo/automation/iidr-ctl/iidr-cli-libs \
JAVA_HOME=/opt/ibm-semeru-jdk-17 \
TLS_TRUST_STORE_PASSWORD='<pwd>' \
  /u01/repo/automation/iidr-ctl/bin/iidr.sh datastore list
```

## 9. Flujo end-to-end típico (provisioning desde iidr-install)

```bash
# 1. Render properties temporal con APP-cdcadmin
cat > /tmp/iidr-cdcadmin.properties <<EOF
asHostName=LESAWSEW141142.es.mapfre.net
asPort=10101
asUserName=APP-cdcadmin
asPassword=<hex_cdcadmin>
registryFile=conf/table_subscription_registry.csv
EOF
chmod 600 /tmp/iidr-cdcadmin.properties

# 2. Crear datastore (como cdcadmin)
IIDR_PROPERTIES_FILE=/tmp/iidr-cdcadmin.properties \
TLS_TRUST_STORE_PASSWORD='<pwd>' \
  /u01/repo/automation/iidr-ctl/bin/iidr.sh datastore create \
    --name IIDR_GR_DB2_DB2_E01_AWS_PRE --host LESAWSEW141140 --port 11801

# 3. Conceder access a APP-cdcauto (también como APP-cdcadmin)
IIDR_PROPERTIES_FILE=/tmp/iidr-cdcadmin.properties \
DB_USER_ENC='<hex_dbuser>' \
DB_PASSWORD_ENC='<hex_dbpwd>' \
TLS_TRUST_STORE_PASSWORD='<pwd>' \
  /u01/repo/automation/iidr-ctl/bin/iidr.sh datastore grant \
    --name IIDR_GR_DB2_DB2_E01_AWS_PRE --user APP-cdcauto

# 4. Limpiar temporal
rm -f /tmp/iidr-cdcadmin.properties

# 5. Verificar como APP-cdcauto (sin IIDR_PROPERTIES_FILE)
TLS_TRUST_STORE_PASSWORD='<pwd>' \
  /u01/repo/automation/iidr-ctl/bin/iidr.sh datastore list
```

## 10. Gotchas / trampas conocidas

- **`list datastores` devuelve `[]` para usuarios sin grant.** Es
  comportamiento de CHCCLP, no bug. `DatastoreGrant.datastoreExists()`
  usa `list access datastores` para verificación fiable (que sí ve los
  datastores administrativamente).
- **`list users` da `EmbeddedScriptException: null`** en la versión de
  CHCCLP de este entorno. Por eso `datastore grant` **no** valida el
  user previamente — si no existe, el `add access connection` falla
  con mensaje semántico claro y se propaga como `ERROR`.
- **El JAR en NFS no se actualiza solo.** Tras `mvn package` hay que
  relanzar `deploy.sh` (usa `AS_RENDER_SKIP=1` para no re-renderizar
  credenciales). Sin esto, el JAR del NFS sigue siendo el viejo y los
  cambios no se ven.
- **CHCCLP no escapa comillas internas.** Su lexer cambia el
  delimitador: `"..."` por defecto, `'...'` si el valor contiene `"`.
  El helper `IIDRConnection.quoteStringLiteral(value)` lo gestiona.
  Si la cadena lleva **ambas** (`"` y `'`) → lanza
  `IllegalArgumentException` (caso extremo, no hay forma en CHCCLP).
- **JRE del AS no existe en host motor.** Cascada de Java en `iidr.sh`:
  (1) `IIDR_JAVA` env explícito → (2) `${IIDR_HOME}/jre64/jre/bin/java`
  (perfil AS) → (3) `${JAVA_HOME}/bin/java` (perfil motor) → exit 5.
- **`iidr-encrypt.sh` necesita `api.jar`** — solo corre en hosts con AS
  instalado (lo lee de `/u01/IIDR/AccessServer/lib/api.jar` por
  defecto). Hosts motor reciben la cadena **ya cifrada** desde el vault.

## 11. Repos hermanos

- **`iidr-install`** (`../../ioeu-cdc-ogg-install-deploy/iidr-install/`):
  proyecto hermano que instala el motor IIDR en cada host antes de que
  `iidr-admin` lo registre en el AS. Es el **consumidor principal** del
  flujo de provisioning (sección 9). Su agente Claude Code redacta
  "pliegos" formales en:
  ```
  ../../ioeu-cdc-ogg-install-deploy/iidr-install/docs/requirements-to-iidr-admin-*.md
  ```
  Léelos cuando el usuario te diga "tienes un nuevo pliego".
- **`metricas`** (no en este repo): proyecto separado de monitorización
  que usa `cdcmon`. No tocar desde aquí.

## 12. Para profundizar

- **[docs/DOCUMENTACION.md](docs/DOCUMENTACION.md)** — guía completa
  (13 secciones): arquitectura detallada, componentes, comandos
  detalle, configuración, integración con Ansible, contrato AAP del
  deploy, idempotencia, provisioning desde `iidr-install`, formato JSON
  compuesto, TLS, dependencias.
- **Encriptado IBM**: `lib/IIDRCrypt.java` (compilada on-the-fly por
  `bin/iidr-encrypt.sh`).
- **Pliegos del repo hermano** (cronología de cambios pedidos):
  `../../ioeu-cdc-ogg-install-deploy/iidr-install/docs/requirements-to-iidr-admin-*.md`.
