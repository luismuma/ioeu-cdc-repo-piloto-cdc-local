# iidr-admin — Estado de pruebas

> Registro del estado de validación de `iidr-admin`. La QA de este
> proyecto es **manual contra entornos reales** (DEV/PRE): no hay tests
> automatizados (la dependencia JUnit del `pom.xml` está declarada pero
> sin ninguna clase de test). Este fichero es la memoria de qué se ha
> probado, cuándo, y qué queda pendiente.
>
> **Cómo mantenerlo:** cada vez que se valide un comando contra un
> entorno, añadir/actualizar la fila correspondiente con la fecha y el
> resultado. Al cerrar una sesión de trabajo, revisar la sección
> "Pendiente".

Última actualización: 2026-07-29.

## Entornos

| Entorno | Access Server | Hosts motor | Notas |
|---|---|---|---|
| DEV | `LESAWSEW131211` | — | AS de desarrollo. Aquí está el checkout y se cifran passwords (api.jar local). |
| PRE | `LESAWSEW141142` | `LESAWSEW141140` (DB2), `LESAWSEW141141` (Kafka) | AS de preproducción. Los hosts motor no tienen AS: usan `iidr-cli-libs/` del NFS. |

## Leyenda de estado

- ✅ **OK** — probado contra entorno real, resultado esperado.
- ⚠️ **Parcial** — probado indirectamente o solo en un escenario.
- ❌ **Pendiente** — nunca ejecutado contra un entorno real.
- 🔴 **Bloqueado** — se intentó y falló; requiere un fix.

## Matriz de comandos

### Datastore

| Comando | Estado | Entorno / fecha | Notas |
|---|---|---|---|
| `datastore list` | ✅ OK | PRE 2026-05-26; DEV 2026-07-15 | Con APP-cdcauto devuelve `[]` si no tiene grant (comportamiento CHCCLP). En DEV: 2 datastores (DB2 source + Kafka target). |
| `datastore show` | ✅ OK | DEV 2026-07-15 | `--name IIDR_GR_DB2_E01_AWS_DEV` → detalle correcto. |
| `datastore create` | ✅ OK | PRE 2026-05-26 | Con APP-cdcadmin vía `IIDR_PROPERTIES_FILE`. SKIPPED idempotente confirmado. |
| `datastore delete` | ❌ Pendiente | — | No hay registro de ejecución. |
| `datastore grant` (DB2) | ✅ OK | PRE 2026-05-26 | `IIDR_GR_DB2_DB2_E01_AWS_PRE` → APP-cdcauto, dbUser=APPCDCL. OK + SKIPPED (idempotencia) confirmados. Requiere `DB_USER_ENC`/`DB_PASSWORD_ENC`. |
| `datastore grant` (Kafka) | ✅ OK | PRE 2026-06-02 | `IIDR_GR_DB2_KFK_R01_AWS_PRE` → APP-cdcauto. Desbloqueado tras el fix de escape de pwd (`quoteStringLiteral`); la pwd de tsuser contenía `~`, `"` y `|`. **Ver nota de trazabilidad abajo.** |

### Subscription

| Comando | Estado | Entorno / fecha | Notas |
|---|---|---|---|
| `subscription list` | ✅ OK | DEV 2026-07-15 | `--source IIDR_GR_DB2_E01_AWS_DEV` → 2 subs (DESACDC2, DESATST1), ambas en `Mirror Continuous`. |
| `subscription show` | ✅ OK | DEV 2026-07-15 | `--name DESACDC2` → detalle + estado en vivo. Detectó `locked=APP-cdcadmin` (sub bloqueada en edición). |
| `subscription create` | ✅ OK | DEV 2026-07-29 | `DESACDC3` recreada (source `IIDR_GR_DB2_E01_AWS_DEV` → target `IIDR_GR_KFK_R01_AWS_DEV`). |
| `subscription delete` | ✅ OK | DEV 2026-07-29 | `DESACDC3` borrada (hace lock interno antes de delete; requiere `--target`). |
| `subscription start` | ✅ OK | DEV 2026-07-29 | `DESACDC3` → Mirror Continuous. **Arranca también desde `Failed`** (lo resetea: Failed→Starting→Refresh Before Mirror→Mirror Continuous). |
| `subscription stop` | ✅ OK | DEV 2026-07-29 | SKIPPED idempotente. ⚠️ Ante estado `Failed` devuelve SKIPPED y **no** lo resetea a Inactive (ni con `--method immediate`); usar `start` directamente. |
| `subscription lock` | ❌ Pendiente | — | |
| `subscription unlock` | ❌ Pendiente | — | |
| `subscription reinit` | ❌ Pendiente | — | No idempotente por diseño (siempre lanza `start refresh`). |
| `subscription setup-kafka` | ✅ OK | DEV 2026-07-29 | `DESACDC3`: 2 tablas (REAL.TB030T001/002), commit topic, KCOP. ⚠️ `--properties-path` debe ser el **fichero** `.../<SUB>.properties`, no el directorio (si no → `FileNotFoundException (Is a directory)` en runtime → Failed). |

### Mapping

| Comando | Estado | Entorno / fecha | Notas |
|---|---|---|---|
| `mapping add` | ❌ Pendiente | — | |
| `mapping remove` | ❌ Pendiente | — | |
| `mapping list` | ❌ Pendiente | — | |
| `mapping find-by-table` | ❌ Pendiente | — | |
| `mapping refresh` | ❌ Pendiente | — | Único "update" real (park / flag-refresh / set-method). |

### Composite

| Comando | Estado | Entorno / fecha | Notas |
|---|---|---|---|
| `composite setup` | ❌ Pendiente | — | Nunca ejecutado end-to-end. Solo existe el ejemplo de `setup.json` en DOCUMENTACION.md; no hay fixture real. |
| `composite teardown` | ❌ Pendiente | — | |
| `composite restart` | ❌ Pendiente | — | |
| `composite health-check` | ❌ Pendiente | — | |

## Infraestructura probada (transversal)

| Aspecto | Estado | Entorno / fecha | Notas |
|---|---|---|---|
| Classpath en host motor (sin AS) | ✅ OK | PRE 2026-05-25 | Bug `NoClassDefFoundError` resuelto empaquetando los 25 JARs del AS en `iidr-cli-libs/`. Verificado en `LESAWSEW141140`. |
| Resolución de Java en cascada | ✅ OK | PRE 2026-05-25 | Perfil motor usa `JAVA_HOME=/opt/ibm-semeru-jdk-17`. |
| Deploy a NFS (`deploy.sh --target pre`) | ✅ OK | PRE, varias fechas | Render de iidr.properties + truststore + JARs. |
| `IIDR_PROPERTIES_FILE` (override APP-cdcadmin) | ✅ OK | PRE 2026-05-26 | Permite provisioning sin tocar el properties de producción. |
| Escape de literales CHCCLP (`quoteStringLiteral`) | ✅ OK | PRE 2026-06-02 | Passwords con caracteres reservados (`~ " |`). CHCCLP no escapa comillas: cambia delimitador `"`↔`'`. |
| `iidr.sh --help` sin JVM | ✅ OK | local 2026-06-03 | Lista comandos y env vars sin exigir TLS. |
| TLS vía `tls.properties` generado | ✅ OK | DEV 2026-07-15 | **Fix de fondo**: la librería IIDR ignora `-Djavax.net.ssl.*` y lee su propio `tls.properties` del CWD. `iidr.sh` ahora lo genera en `PROJECT_ROOT` (sección cliente: trustStorePath/Password/Type, ruta absoluta al `.p12` del NFS). Sin esto: `PKIX path building failed`. Funciona también en host motor (sin AS). |
| **AAP end-to-end (smoke test)** | ✅ OK | DEV (AAP) 2026-07-17 | Vía táctica validada: EE (AAP) → SSH a la 11 (ec2-user, become cdcuser) → `iidr.sh datastore list` → AS:10101 → JSON OK. Secreto AWS SM (`CDC-VAULT-DEV`) resuelto con `from_json`, pwd truststore de `vault_iidr_as_keystore_password`. Patrón: 2 plays (localhost resuelve secreto+add_host; `hosts: <la 11>` ejecuta por SSH — NO delegate_to). |
| Tests automatizados (JUnit) | ❌ No existen | — | `src/test/` no existe. Toda la QA es manual. |

## Playbooks AAP (Fase 1 — lectura)

Ejecutados en AAP DEV el 2026-07-17, patrón de 2 plays (EE → SSH a la 11 →
iidr.sh). Job Template: EE custom + Machine Credential (ec2-user) + Custom
Credential AWS SM.

| Playbook / caso | Estado | Notas |
|---|---|---|
| `datastores.yml` action=read (sin name) | ✅ OK | Lista los 2 datastores de DEV. |
| `datastores.yml` action=read (con name) | ✅ OK | `datastore show --name` → detalle. El `status: Not connected` del datastore es normal (no hay sesión activa al motor). |
| `subscriptions.yml` action=read (sin name) | ✅ OK | `subscription list --source` → 2 subs. |
| `subscriptions.yml` action=read (con name) | ✅ OK | `subscription show --source --name` → detalle. |

### subscriptions.yml — escritura (construido 2026-07-17, PENDIENTE de probar)

Añadidas a `subscriptions.yml` todas las acciones que `iidr.sh` ya soporta
(usuario APP-cdcauto). Lógica de composición de args validada en local para los
8 casos. **Pruebas pendientes en AAP DEV**, con la subscription de prueba
**`DESACDC3`** (para no tocar las activas DESACDC2/DESATST1):

| Caso | Comando iidr.sh | Estado | Notas |
|---|---|---|---|
| `create` | `subscription create --source S --target T --name DESACDC3 [--description D]` | ✅ OK (DEV AAP 2026-07-17) | DESACDC3 creada, confirmada en Management Console. Crea sub básica (SIN KCOP). Idempotencia OK: relanzar → SKIPPED "La suscripción ya existe: DESACDC3". |
| `delete` | `subscription delete --source S --target T --name DESACDC3` | ✅ OK (DEV AAP 2026-07-20) | 1er intento falló (`CHCCLP rc=-2317 must be locked`): bug en SubscriptionDelete.java (no hacía `lock` antes del delete). **Corregido** (añade `lock subscription` previo, commit 4254e30) + deploy a DEV → re-probado OK: DESACDC3 borrada. Requiere `--target`. |
| `start` | `subscription start --source S --name DESACDC3` | ✅ OK-comando / sub Failed (DEV AAP 2026-07-17) | El COMANDO funciona: transición `Inactive → Starting`, status OK. Pero la sub acabó en **`Failed`** (no Inactive), confirmado en Management Console ("target system failure"). ⚠️ `Starting` NO garantiza éxito: el comando devuelve al iniciar la transición; para confirmar `Mirror Continuous` haría falta show/poll posterior. **Motivo del Failed** (event log): `Failed to start the target pipeline. No Records Found` en `countNumberOfSourceTables`/`getNumberOfTopics` — DESACDC3 no tiene table mappings ni KCOP/topic de commit/properties. Es lo esperado: le falta toda la config Kafka. |
| `stop` | `subscription stop --source S --name N [--method normal\|immediate\|abort]` | ✅ OK (DEV AAP 2026-07-17) | Sobre DESACDC3 en `Failed` → SKIPPED "La suscripción ya está parada: Failed". El CLI trata `Failed` como no-corriendo (correcto). `Failed` es estado terminal: no se limpia con stop, sino arrancando bien o borrando. |
| `lock` | `subscription lock --source S --target T --name N` | ✅ OK (DEV AAP 2026-07-17) | DESACDC3 → "locked by APP-cdcauto" (Management Console). Idempotencia OK (lock sobre bloqueada → SKIPPED; unlock sobre desbloqueada → SKIPPED). |
| `unlock` | `subscription unlock --source S --target T --name N` | ✅ OK (DEV AAP 2026-07-17) | DESACDC3 desbloqueada (confirmado en Management Console). |
| `reinit` | `subscription reinit --source S --name N` | ⏭️ Diferido | NO idempotente (siempre lanza refresh completo). Diferido: una sub sin KCOP/mappings no arranca, así que probarlo ahora no aporta; y sobre una sub real es disruptivo. Se probará cuando haya una sub con KCOP configurado. |

**Plan de prueba seguro en DEV** (sin tocar subs activas):
1. `create` DESACDC3 → OK; relanzar → SKIPPED (idempotencia).
2. `lock`/`unlock` DESACDC3 → OK + idempotencia.
3. `start` DESACDC3 → validar arranque (ojo: sin KCOP no replica).
4. `stop` DESACDC3.
5. `delete` DESACDC3 → limpia. Relanzar delete → SKIPPED.

### subscriptions.yml — action=setup-kafka (KCOP automatizado, 2026-07-21)

Automatización completa de la configuración KCOP: comando compuesto Java
`iidr.sh subscription setup-kafka` (mappings + kafka properties + user exit,
vía API 10101) + plantilla Jinja2 `templates/kcop.properties.j2` que el
playbook renderiza y deposita en el motor Kafka (la 10) por SSH.

| Caso | Estado | Notas |
|---|---|---|
| `create` DESACDC3 | ✅ OK (DEV AAP 2026-07-21) | status OK, NO arranca (correcto). El `Failed` que muestra la MC para una sub Kafka sin configurar es estado inicial/cacheado, no un arranque real (event log sin eventos → nunca se arrancó). |
| `setup-kafka` DESACDC3 | ✅ OK (DEV AAP 2026-07-21) | End-to-end: Play 1b depositó `DESACDC3.properties` en la 10 (**SSH del EE a la 10 validado**); Play 2 ejecutó setup-kafka → OK, `tablesMapped:[TB030T001,TB030T002]`, commit topic, KCOP class y properties path. En la MC: 2 mapeos correctos (Avro JSON, Mirror, Refresh) y la sub pasó de `Failed` a `Inactive` (= configurada, lista para arrancar). El `.properties` generado se ve perfecto. |
| `start` DESACDC3 (tras setup-kafka) | ✅ OK (DEV CLI 2026-07-29) | **DESBLOQUEADO.** Se creó el commit topic (cliente Apache `kafka_2.12-3.7.1` de `/u01/repo/clients`, pool `pool-y95PX` — DEV sí tiene permiso de CREATE) y la sub llegó a **Mirror Continuous**. Ver hito abajo. (Probado por CLI; falta reproducir por AAP con la creación del topic integrada.) |

**Fallo del fix del bug primero** (para el histórico): el 1er intento de `create`
del 2026-07-21 pareció dejar la sub en `Failed`, pero se confirmó que era el
estado cacheado de la MC de un DESACDC3 anterior — el `create` da OK y no
arranca. Sin bug.

### ✅ HITO: flujo Kafka COMPLETO end-to-end (DEV CLI, 2026-07-29)

Primera vez que una sub Kafka llega a **Mirror Continuous** de principio a
fin. Secuencia validada por CLI (`iidr.sh`) con DESACDC3:

1. `subscription delete` (limpiar la DESACDC3 anterior).
2. `subscription create` (source DB2 → target Kafka).
3. **Crear el commit topic** — cliente Apache
   `/u01/repo/clients/kafka_2.12-3.7.1/bin/kafka-topics.sh --create` contra
   Confluent Cloud DEV (bootstrap `pkc-z9doz...:9092`, OAuth OAUTHBEARER con
   `pool-y95PX`). Topic `esp.int.cac.bronze.grcdcpiloto.DESACDC3-commitstream_cdc`.
4. `subscription setup-kafka` (mappings + kafka props + user exit). ⚠️
   `--properties-path` = **fichero** `.../conf/DESACDC3.properties`, no el dir.
5. `subscription start` → `Failed`→`Starting`→`Refresh Before Mirror`→
   **`Mirror Continuous`**.

**Permisos de creación de topics (probado 2026-07-29):** DEV con `pool-y95PX`
= CREATE OK; PRE = sin permiso; `pool-xEYEX` (nuevo) falla con
`CLAIMS_POOL_FILTER_MISMATCH` (rol `ROL_KFK_ES_PEE_CDC_IBM_INT` ausente del
token OAuth). Comunicado a Mapfre.

**2 trampas descubiertas (FIX pendiente en Java):** (a) `setup-kafka` no
valida que `--properties-path` sea un fichero → si es un dir, el KCOP peta en
runtime (`FileNotFoundException ... Is a directory`) y la sub va a `Failed`;
(b) `subscription stop` ante `Failed` devuelve SKIPPED y no lo resetea, pero
`start` sí arranca desde `Failed` (no hace falta parar antes).

**Pendiente en playbooks AAP:**
- **Integrar la CREACIÓN del topic de commit** en `subscriptions.yml` y en
  `iidr-ctl.py` (flujo Zeus): un paso `kafka-topics.sh --create` (cliente
  Apache de `/u01/repo/clients`, ruta parametrizable) antes de setup-kafka.
  El `bootstrap_servers` está en `target_connections.yaml`; el fichero OAuth
  del cliente ya existe en la 10.
- **Reproducir el flujo completo por AAP** (hoy solo validado por CLI).
- Acciones de escritura de **datastore** (create/delete/grant con APP-cdcadmin
  vía IIDR_PROPERTIES_FILE temporal; grant además DB_*_ENC) — siguiente.
- **Reactivar `no_log`** en las tareas con secretos (marcadas `TODO(no_log)`).
- **PRE**: el playbook ya resuelve host+secreto por `iidr_env` (DEV y PRE
  parametrizados, incl. Schema Registry). Pendiente de instalación de PRE
  con **direcciones canónicas HA** (ver abajo).
- **DESACDC3 queda creada y configurada (Inactive) en DEV** al cerrar esta
  sesión (no se pudo arrancar por el bloqueo). Borrar al retomar.

### Condicionante HA / Route53 (PRE y PRO)

Cada servicio IIDR (AS, motor DB2, motor Kafka) tendrá **2 servidores en
AZs distintas** de la misma región. Los comandos CRUD deben ir a la
**dirección canónica** (host virtual), y alguien tiene que resolver quién
es el nodo activo en cada momento. Hoy la resolución es por `/etc/hosts`;
el objetivo es **Route53** (DNS de AWS). **Si la resolución del host
canónico no está en Route53 (o accesible desde el EE), los playbooks contra
PRE/PRO no funcionarán** aunque el código esté listo. A confirmar cuando se
instale PRE con las direcciones canónicas.

## Trazabilidad con los pliegos de iidr-install

Los criterios de aceptación formales están en el repo hermano
`ioeu-cdc-ogg-install-deploy/iidr-install/docs/`:

| Pliego | Fecha | Qué pedía | Estado |
|---|---|---|---|
| `requirements-to-iidr-admin-classpath.md` | 2026-05-25 | Empaquetar todos los JARs del AS (fix NoClassDefFoundError) | ✅ Cerrado |
| `requirements-to-iidr-admin-provisioning.md` | 2026-05-26 | `IIDR_PROPERTIES_FILE` + comando `datastore grant` | ✅ Cerrado |
| `requirements-to-iidr-admin-pwd-escape.md` | 2026-06-02 | Escapar password en comandos CHCCLP | ✅ Cerrado (fix implementado y probado con la pwd de tsuser) |

## Pendiente / próximos pasos

1. **Comandos de escritura de subscription y mapping.** La capa de
   **lectura** (datastore/subscription list+show) está validada en DEV
   (2026-07-15). Falta todo lo que **modifica estado**:
   create/delete/start/stop/reinit/lock/unlock de subscription y
   add/remove/refresh de mapping — sin prueba registrada. Es el mayor
   hueco. **Cuidado en DEV**: hay 2 subs activas en `Mirror Continuous`
   (DESACDC2, DESATST1) — no hacer stop/delete/reinit sobre ellas.
   Mejor crear una sub de prueba dedicada. `mapping list` /
   `mapping find-by-table` son de lectura y se pueden validar ya.
2. **`composite setup` end-to-end.** Crear un `setup.json` real de PRE y
   ejecutarlo; hoy solo existe el ejemplo de la documentación.
3. **Tests automatizados.** No hay ninguno. Como mínimo, tests unitarios
   de las piezas puras: `IIDRConnection.quoteStringLiteral` (casos
   `"`, `'`, ambos → excepción), `TableSubscriptionRegistry` (CSV),
   `OutputFormatter` (JSON). No requieren AS.
4. **Capturar logs de las pruebas.** `log/` está vacío; no hay salidas
   JSON archivadas. Al validar, guardar la salida para trazabilidad.

## Hallazgos de comportamiento CHCCLP / AS (catálogo)

Descubiertos durante las pruebas manuales — importantes para no
tropezar de nuevo:

- `list datastores` devuelve `[]` para usuarios sin access connection al
  datastore. Para verificar existencia real se usa `list access
  datastores` (lo hacen `DatastoreCreate`, `DatastoreDelete`,
  `DatastoreGrant`, `CompositeSetup`).
- `list users` lanza `EmbeddedScriptException: null` en esta versión de
  CHCCLP. Por eso `datastore grant` no valida el usuario previamente.
- `add access connection` exige `dbUser` + `dbPassword` (rc -2004 si
  faltan) — credenciales del usuario técnico del motor.
- CHCCLP no escapa comillas internas: usa `"..."` por defecto y `'...'`
  si el valor contiene `"`. Un valor con ambas comillas no es
  representable → `quoteStringLiteral` lanza `IllegalArgumentException`.
- RCs 1312 / 1314 / 1322 son informativos (no fallos); los absorbe
  `IIDRConnection.executeForMessage`.
- **TLS: la librería IIDR NO usa `-Djavax.net.ssl.*`.** Lee su propio
  `tls.properties` desde el CWD (trustStorePath/Password/Type). `iidr.sh`
  lo genera en `PROJECT_ROOT` y hace `cd` allí antes de arrancar la JVM.
  Solo hace falta la sección de cliente (truststore); la de keystore
  privado es para el AS como servidor. Síntoma si falta: `rc=-2001
  SSLHandshakeException: PKIX path building failed`.
- **Cuentas del AS se bloquean por INACTIVIDAD**, no por password
  incorrecta. Una cuenta que lleva tiempo sin usarse muestra
  `Locked=false` en `dmlistusers`, pero al intentar conectar devuelve
  `[ERR2001] The new account has expired ... and has been locked`. Se
  reactiva desde la Management Console (SYSADMIN) reseteando/cambiando
  la pwd. Afecta a `APP-cdcauto`/`APP-cdcadmin`/`APP-cdcmon` tras periodos de
  inactividad — probable en DEV/PRE tras pausas largas. Detectado
  2026-07-15 en DEV con `APP-cdcauto`.
- **Si se reinstala el Access Server**, su certificado de servidor se
  regenera y el truststore del deploy (NFS) queda obsoleto → los
  clientes fallan con PKIX. Solución: copiar/reconstruir el truststore
  del NFS desde el AS reinstalado. Ocurrido en DEV (jun-jul 2026).
