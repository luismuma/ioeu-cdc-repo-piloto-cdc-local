# Gestión de Datastores y Suscripciones IBM IIDR/CDC mediante Ansible Automation Platform

## Descripción general de los playbooks de administración (iidr-admin)

**Versión**: 1.0
**Producto**: IBM InfoSphere Data Replication (IIDR) / CDC 11.4.0.5
**Plataforma de automatización**: Red Hat Ansible Automation Platform (AAP)
**Componente**: `iidr-admin` — CLI de administración post-instalación
**Fecha**: Julio 2026

---

## 1. Introducción

Este documento describe el conjunto de playbooks de Ansible desarrollados para la **administración de datastores y suscripciones** de IBM IIDR/CDC 11.4.0.5 en los entornos de Mapfre, desde Red Hat Ansible Automation Platform (AAP). A diferencia del proyecto hermano `iidr-install` —que cubre la instalación de los componentes de la plataforma (Access Server, motores DB2 y Kafka, Management Console)—, `iidr-admin` cubre las operaciones del **día a día** una vez la plataforma ya está instalada: consultar, crear, modificar y eliminar los datastores registrados en el Access Server y las suscripciones de replicación.

El objetivo es que un operador, sin conocimientos previos de Ansible ni de la línea de comandos interna de IIDR, pueda ejecutar estas operaciones desde la interfaz web de AAP, comprendiendo qué hace cada automatización, qué parámetros debe proporcionar y qué resultado puede esperar.

Estos playbooks se apoyan en una herramienta CLI Java desarrollada específicamente para este proyecto (`iidr.sh` y el artefacto `iidr-admin.jar`), que encapsula toda la interacción con el Access Server y devuelve resultados estructurados en formato JSON. Los playbooks actúan como capa de orquestación sobre esa herramienta: resuelven las credenciales del entorno, se conectan al servidor adecuado y ejecutan el comando correspondiente, interpretando el resultado y reportándolo al operador.

Este documento cubre los dos playbooks de gestión de entidades disponibles en la iteración actual:

- **`datastores.yml`** — gestión de datastores (registros de motores IIDR en el Access Server).
- **`subscriptions.yml`** — gestión de suscripciones de replicación.

Adicionalmente se describen dos playbooks auxiliares de diagnóstico (`iidr_ee_probe.yml` y `iidr_smoke_test.yml`) empleados para validar el entorno de ejecución.

---

## 2. Conceptos previos

### 2.1 ¿Qué es Ansible Automation Platform (AAP)?

AAP es la plataforma de automatización de Red Hat. En el contexto de este proyecto actúa como orquestador centralizado que:

- Gestiona de forma segura las credenciales de acceso a los servidores y a los secretos del entorno.
- Proporciona una interfaz web desde la que el operador lanza operaciones sin necesidad de acceder directamente a los servidores ni conocer la sintaxis interna de IIDR.
- Mantiene un historial completo de todas las ejecuciones con su salida, facilitando la auditoría y la resolución de incidencias.

Cada automatización se configura en AAP como un **Job Template**, la unidad básica de ejecución: asocia un playbook con un inventario, unas credenciales y unos parámetros de entrada. Cuando el operador lanza un Job Template, AAP crea un **Job** (una ejecución concreta) y muestra en tiempo real el progreso tarea a tarea.

### 2.2 ¿Qué es un playbook de Ansible?

Un playbook es un fichero en formato YAML que describe una serie de tareas a ejecutar. Cada tarea invoca un **módulo** de Ansible (copiar un fichero, ejecutar un comando, hacer una aserción, etc.) o llama a una herramienta externa desarrollada para el proyecto.

En `iidr-admin`, la lógica de negocio (conexión TLS al Access Server, autenticación, ejecución de comandos de administración y serialización de resultados) está encapsulada en la herramienta CLI `iidr.sh`, que reside en el NFS compartido (`/u01/repo/automation/iidr-ctl/bin/iidr.sh`). Los playbooks actúan como **director de orquesta**: resuelven las credenciales, se conectan al servidor adecuado, invocan `iidr.sh` con los argumentos correctos y comprueban el resultado.

### 2.3 El Execution Environment y la vía de ejecución

En AAP, los playbooks no se ejecutan directamente sobre el sistema operativo del controlador, sino dentro de un **Execution Environment (EE)**: un contenedor con Ansible y las dependencias necesarias. El EE de este proyecto es una imagen específica de Mapfre.

La herramienta `iidr.sh` requiere, para funcionar, un entorno Java con las librerías de IBM IIDR (que la certifican) y un almacén de confianza TLS (truststore). Estos elementos residen hoy en los servidores de la plataforma IIDR, no en el EE. Por ello, en la iteración actual los playbooks emplean una **vía de ejecución delegada**: el playbook se conecta por SSH desde el EE a un servidor que sí dispone de todo lo necesario (el propio Access Server del entorno) y ejecuta `iidr.sh` allí. Esta arquitectura se detalla en la sección 3.

Se trata de una solución **táctica**: cuando el Execution Environment incorpore el JDK y las librerías de IIDR, los playbooks podrán ejecutar `iidr.sh` directamente dentro del contenedor, eliminando el salto SSH. El diseño de los playbooks está preparado para ese cambio sin necesidad de reescribirlos (véase sección 3.3).

### 2.4 Gestión de secretos y contraseñas

Ninguna contraseña se escribe en la línea de comandos de ningún playbook (hacerlo la expondría en la salida de `ps` y en los logs). Todas las credenciales se recuperan de **AWS Secrets Manager** en tiempo de ejecución y se pasan a la herramienta como **variables de entorno**.

AAP dispone de un tipo de credencial personalizado (*Custom Credential Type* `cdc-secret-lookup`) que, para el entorno seleccionado, lee un secreto de AWS Secrets Manager y lo inyecta en el playbook como una variable con el contenido JSON del secreto. El playbook parsea ese JSON y extrae las claves que necesita. Cada entorno (DEV, PRE, PRO) tiene su propio secreto (`CDC-VAULT-DEV`, `CDC-VAULT-PRE`, ...), de modo que un mismo playbook sirve para todos los entornos sin cambios.

Las claves del secreto emplean el prefijo `IIDR_` en mayúsculas (nomenclatura acordada para el secreto compartido en AWS Secrets Manager con el resto de equipos). Las relevantes para `iidr-admin` son:

| Clave del secreto | Propósito |
|---|---|
| `IIDR_AS_KEYSTORE_PASSWORD` | Contraseña del truststore PKCS12 usado para el TLS con el Access Server |
| `IIDR_CDCADMIN_PASSWORD` | Contraseña del usuario `APP-cdcadmin` (operaciones de provisioning) |
| `IIDR_CDCAUTO_PASSWORD` | Contraseña del usuario `APP-cdcauto` (operaciones de día a día) |
| `IIDR_DB2_DS_USER_ENC` / `IIDR_DB2_DS_PASSWORD_ENC` | Credenciales cifradas del motor DB2 (para conceder acceso a datastores DB2) |
| `IIDR_KAFKA_DS_USER_ENC` / `IIDR_KAFKA_DS_PASSWORD_ENC` | Credenciales cifradas del motor Kafka |
| `KAFKA_CLIENT_ID` / `KAFKA_CLIENT_SECRET` / `KAFKA_POOL_ID` | Credenciales OAuth del Schema Registry (para `setup-kafka`) — sin prefijo `IIDR_` (compartidas con otros equipos) |

### 2.5 Usuarios de administración de IIDR

Las operaciones sobre el Access Server se autentican con un usuario IIDR. Por convención de Mapfre existen tres, con responsabilidades separadas:

| Usuario | Rol | Uso |
|---|---|---|
| `APP-cdcadmin` | Provisioning (ciclo de vida del datastore) | Crear, eliminar y conceder acceso a datastores |
| `APP-cdcauto` | Operación (ciclo de vida de suscripciones) | Listar, crear, arrancar, parar suscripciones y gestionar mapeos |
| `APP-cdcmon` | Monitorización (solo lectura) | Empleado por el proyecto de métricas, no por estos playbooks |

Los playbooks de `iidr-admin` operan por defecto como `APP-cdcauto` (el usuario del día a día). Para las operaciones de provisioning de datastores (crear/eliminar/conceder acceso) se emplea `APP-cdcadmin`, seleccionando el fichero de propiedades adecuado en tiempo de ejecución (véase sección 6).

### 2.6 Idempotencia

Todas las operaciones de escritura son **idempotentes**: si se ejecutan dos veces sobre el mismo estado, el resultado es el mismo que si se ejecutaran una sola vez. La herramienta comprueba si la operación ya está hecha y, en caso afirmativo, devuelve un estado `SKIPPED` en lugar de fallar o duplicar la acción. Esto permite relanzar un playbook tras un fallo parcial sin riesgo. El único comando no idempotente por naturaleza es `subscription reinit`, que fuerza siempre una recarga completa de datos.

---

## 3. Arquitectura de ejecución

### 3.1 Componentes implicados

```
   ┌────────────────────┐         SSH          ┌───────────────────────────┐
   │  AAP + Execution    │ ───────────────────► │  Access Server (entorno)  │
   │  Environment (EE)   │                      │  LESAWSEW131211 (DEV)     │
   │                     │  ejecuta iidr.sh     │  LESAWSEW141142 (PRE)     │
   │  - playbook         │                      │                           │
   │  - secreto AWS SM   │                      │  - JDK 17 (Semeru)        │
   └────────────────────┘                      │  - librerías IIDR         │
            │                                    │  - truststore (.p12)      │
            │ lee secreto                        │  - iidr.sh (en NFS)       │
            ▼                                    └────────────┬──────────────┘
   ┌────────────────────┐                                     │ TLS :10101
   │ AWS Secrets Manager │                                     ▼
   │ CDC-VAULT-<ENV>     │                          ┌────────────────────┐
   └────────────────────┘                          │  Access Server API  │
                                                    │  (puerto 10101)     │
                                                    └────────────────────┘
```

- **AAP / Execution Environment**: donde se ejecuta el playbook. Resuelve el secreto del entorno y orquesta.
- **AWS Secrets Manager**: almacén de credenciales por entorno. AAP lo consulta mediante el Custom Credential Type.
- **Access Server (servidor de ejecución)**: el servidor al que el EE se conecta por SSH y donde realmente se ejecuta `iidr.sh`. Dispone de Java, las librerías IIDR y el truststore. La herramienta se conecta desde ahí al Access Server por el puerto de administración (10101) mediante TLS.

### 3.2 Patrón de dos plays

Cada playbook de gestión se estructura en **dos plays** (dos fases):

1. **Fase de preparación (en el EE, `localhost`)**: valida los parámetros que el operador ha introducido, carga el secreto del entorno desde AWS Secrets Manager y lo parsea, y registra el servidor de ejecución en el inventario dinámico de Ansible.

2. **Fase de ejecución (en el servidor de ejecución, por SSH)**: Ansible abre una conexión SSH real al servidor registrado en la fase anterior, eleva privilegios al usuario adecuado y ejecuta `iidr.sh` con los argumentos correspondientes a la operación solicitada. Interpreta la salida JSON y decide si la operación fue correcta, no requirió cambios o falló.

Este patrón de dos plays es idéntico al empleado por los playbooks de instalación de `iidr-install`, y es el que garantiza que el comando se ejecute realmente en el servidor destino (y no en el contenedor del EE, que carece de las librerías necesarias).

### 3.3 Preparación para la ejecución nativa en el EE

Las variables que determinan **dónde** y **cómo** se ejecuta la herramienta están centralizadas en el fichero `group_vars/all/vars.yml` (el servidor de ejecución por entorno, la ruta de `iidr.sh`, el usuario de escalación). Cuando el Execution Environment incorpore el JDK y las librerías de IIDR, bastará con ajustar esas variables para que la ejecución sea local al EE, sin modificar la lógica de los playbooks.

### 3.4 Contrato de la herramienta `iidr.sh`

La herramienta devuelve siempre un objeto JSON por la salida estándar, con la siguiente estructura:

```json
{
  "status": "OK | SKIPPED | ERROR",
  "operation": "<entidad>.<operación>",
  "data": { ... },
  "message": "<texto descriptivo, sobre todo en SKIPPED y ERROR>"
}
```

- **`status`**: `OK` (operación ejecutada con éxito), `SKIPPED` (el estado deseado ya existía, no se hizo nada) o `ERROR` (fallo real).
- El código de salida del proceso es `0` para `OK` y `SKIPPED`, y `1` para `ERROR`.
- Los mensajes de log de la herramienta se emiten por la salida de error (stderr), separados del JSON de resultado.

Los playbooks consideran `OK` y `SKIPPED` como éxito, y `ERROR` como fallo (deteniendo el Job).

---

## 4. Estructura del repositorio

Toda la automatización de `iidr-admin` reside bajo `iidr-admin/ansible/`, de forma autocontenida (no depende de ficheros en la raíz del repositorio, para no interferir con los otros componentes del mismo repositorio git).

```
iidr-admin/
├── ansible/
│   ├── ansible.cfg                    # Configuración de Ansible (autocontenida)
│   ├── group_vars/
│   │   ├── all/
│   │   │   └── vars.yml               # Datos por entorno + claves del secreto
│   │   ├── pre/
│   │   │   └── vars_iidr_ha.yml       # IPs por AZ para el switch HA (PRE)
│   │   └── prod/
│   │       ├── vars_iidr_ha.yml       # IPs por AZ para el switch HA (PROD)
│   │       └── vars_iidr_dr.yml       # IPs de las 3 ubicaciones para el switch DR
│   ├── playbooks/
│   │   ├── datastores.yml             # Gestión de datastores
│   │   ├── subscriptions.yml          # Gestión de suscripciones
│   │   ├── iidr_ee_probe.yml          # Diagnóstico del Execution Environment
│   │   ├── iidr_smoke_test.yml        # Prueba de extremo a extremo de la cadena
│   │   ├── iidr_ha_switch_az.yml      # Conmutación de servicio entre AZs (HA)
│   │   └── iidr_dr_switch.yml         # Conmutación de servicio a DR / vuelta
│   └── README.md                      # Guía rápida de uso
├── bin/
│   └── iidr.sh                        # Herramienta CLI (se despliega al NFS)
├── conf/
│   └── cas/                           # CAs públicas para construir el truststore
└── docs/
    └── playbooks-iidr-admin-aap.md    # Este documento
```

Las variables comunes a todos los entornos están en `group_vars/all/vars.yml`. Los secretos no residen en el repositorio: se recuperan de AWS Secrets Manager en tiempo de ejecución.

### 4.1 Variables comunes (`group_vars/all/vars.yml`)

Este fichero centraliza los datos que dependen del entorno o de la infraestructura:

| Variable | Descripción |
|---|---|
| `iidr_exec_host_by_env` | Mapa entorno → servidor de ejecución (FQDN). DEV: `lesawsew131211.es.mapfre.net`; PRE: `LESAWSEW141142.es.mapfre.net`. |
| `iidr_sh_path` | Ruta de `iidr.sh` en el NFS (`/u01/repo/automation/iidr-ctl/bin/iidr.sh`). |
| `iidr_run_user` | Usuario del sistema bajo el que se ejecuta la herramienta tras el SSH. |
| `iidr_key_tls_password` | Nombre de la clave del secreto que contiene la contraseña del truststore. |
| `iidr_key_cdcadmin_password` | Nombre de la clave con la contraseña de `APP-cdcadmin`. |
| `iidr_key_db2_user_enc` / `iidr_key_db2_password_enc` | Claves de credenciales cifradas del motor DB2. |
| `iidr_key_kafka_user_enc` / `iidr_key_kafka_password_enc` | Claves de credenciales cifradas del motor Kafka. |
| `iidr_datastore_db2_by_env` / `iidr_datastore_kafka_by_env` | Nombres de los datastores conocidos por entorno (referencia). |

---

## 5. Playbook `datastores.yml` — Gestión de datastores

### 5.1 Descripción general

Un **datastore** es el registro, dentro del Access Server, de un motor IIDR (un agente DB2 z/OS de origen o un agente Kafka de destino). Define su nombre lógico, su endpoint (hostname y puerto) y su tipo. Este playbook permite consultar los datastores registrados y su detalle.

En la iteración actual, el playbook implementa las operaciones de **lectura** (`read`). Las operaciones de escritura (crear, eliminar, conceder acceso) están diseñadas y descritas más abajo, pendientes de su incorporación en la siguiente iteración, por requerir el usuario de provisioning `APP-cdcadmin`.

### 5.2 Parámetro de acción

El comportamiento del playbook se controla con el parámetro `action`:

| `action` | Operación | Comando IIDR subyacente |
|---|---|---|
| `read` (sin `name`) | Listar todos los datastores visibles | `datastore list` |
| `read` (con `name`) | Mostrar el detalle de un datastore concreto | `datastore show --name <name>` |

La distinción entre listar y mostrar se hace en función de si el operador proporciona o no el parámetro `name`: si lo proporciona, se muestra el detalle de ese datastore; si no, se listan todos.

### 5.3 Tareas en orden de ejecución

**Fase 1 — Preparación (en el EE)**

1. **Resolución del entorno y la acción**: se fija `iidr_env` (por defecto `dev`) y `action` (por defecto `read`) a partir de los parámetros del operador.
2. **Validación de parámetros**: se comprueba que `iidr_env` es uno de los valores válidos (`dev`, `pre`, `pro`) y que `action` es válida. Si algo es incorrecto, el playbook se detiene con un mensaje descriptivo.
3. **Carga del secreto del entorno**: se recupera el secreto de AWS Secrets Manager correspondiente al entorno y se parsea su contenido JSON.
4. **Verificación de la contraseña del truststore**: se comprueba que el secreto contiene la clave de la contraseña del truststore, sin mostrar su valor.
5. **Registro del servidor de ejecución**: se añade el servidor del entorno (el Access Server) al inventario dinámico, junto con la contraseña del truststore que necesitará la fase 2.

**Fase 2 — Ejecución (en el Access Server, por SSH)**

6. **Composición del comando**: se construye la línea de `iidr.sh` según la acción (`datastore list` o `datastore show --name ...`).
7. **Ejecución de la herramienta**: se ejecuta `iidr.sh` con la contraseña del truststore inyectada como variable de entorno. Se captura el resultado.
8. **Presentación del resultado**: se muestra la salida completa (código de retorno, JSON de resultado y logs).
9. **Interpretación y validación**: se parsea el JSON y se verifica que el estado es `OK` o `SKIPPED`; en caso de `ERROR`, el playbook falla.

### 5.4 Parámetros del playbook

| Parámetro | Obligatorio | Valores válidos | Descripción |
|---|---|---|---|
| `iidr_env` | No (por defecto `dev`) | `dev`, `pre`, `pro` | Entorno de ejecución. Determina servidor y secreto. |
| `action` | No (por defecto `read`) | `read` | Operación a realizar. |
| `name` | No | Nombre de datastore | Si se proporciona, muestra el detalle de ese datastore; si no, lista todos. |

### 5.5 Ejemplos de lanzamiento

Listar todos los datastores de DEV:

```
iidr_env: dev
action: read
```

Mostrar el detalle de un datastore concreto:

```
iidr_env: dev
action: read
name: IIDR_GR_DB2_E01_AWS_DEV
```

### 5.6 Salida esperada

Para `datastore list`, el resultado incluye la lista de datastores con su nombre, host, puerto, versión, base de datos, tipo (Source/Target) y estado. Para `datastore show`, el mismo detalle de un único datastore. El estado `Not connected` que puede aparecer en un datastore es normal: indica únicamente que en ese instante no hay una sesión activa abierta contra el motor, no que el datastore esté mal.

### 5.7 Operaciones de escritura (diseño, siguiente iteración)

Las siguientes operaciones están diseñadas y su mecánica validada, pendientes de incorporación al playbook. Todas requieren el usuario `APP-cdcadmin`, cuya contraseña se recupera del secreto y con la que se genera un fichero de propiedades temporal en el servidor de ejecución para autenticar la operación de provisioning:

| `action` | Operación | Comando IIDR subyacente | Requisitos adicionales |
|---|---|---|---|
| `create` | Registrar un datastore | `datastore create --name <n> --host <h> --port <p>` | Usuario `APP-cdcadmin` |
| `delete` | Eliminar un datastore | `datastore delete --name <n>` | Usuario `APP-cdcadmin` |
| `grant` | Conceder acceso a un usuario sobre un datastore | `datastore grant --name <n> --user <u>` | Usuario `APP-cdcadmin` + credenciales cifradas del motor (`DB_USER_ENC`, `DB_PASSWORD_ENC`) según el tipo de datastore |

---

## 6. Playbook `subscriptions.yml` — Gestión de suscripciones

### 6.1 Descripción general

Una **suscripción** define qué se replica desde un datastore origen hacia un datastore destino. Este playbook cubre el ciclo de vida completo de una suscripción: consulta, creación, eliminación y operaciones de estado (arranque, parada, bloqueo/desbloqueo y reinicialización).

Todas las operaciones se ejecutan como el usuario operacional `APP-cdcauto`, sin necesidad del usuario de provisioning, ya que la gestión de suscripciones es una tarea del día a día.

### 6.2 Parámetro de acción

| `action` | Operación | Comando IIDR subyacente |
|---|---|---|
| `read` (sin `name`) | Listar las suscripciones de un datastore | `subscription list --source <source>` |
| `read` (con `name`) | Mostrar el detalle de una suscripción | `subscription show --source <source> --name <name>` |
| `create` | Crear una suscripción | `subscription create --source <s> --target <t> --name <n> [--description <d>]` |
| `delete` | Eliminar una suscripción | `subscription delete --source <s> --target <t> --name <n>` |
| `start` | Arrancar la replicación | `subscription start --source <s> --name <n>` |
| `stop` | Parar la replicación | `subscription stop --source <s> --name <n> [--method <m>]` |
| `lock` | Bloquear la suscripción para edición exclusiva | `subscription lock --source <s> --target <t> --name <n>` |
| `unlock` | Desbloquear la suscripción | `subscription unlock --source <s> --target <t> --name <n>` |
| `reinit` | Reinicializar (recarga completa de datos) | `subscription reinit --source <s> --name <n>` |
| `setup-kafka` | Configurar una suscripción Kafka para que pueda arrancar (mapeos + propiedades Kafka + user exit KCOP + generación del fichero de propiedades) | `subscription setup-kafka --source <s> --target <t> --name <n> --tables ... --commit-topic ... --properties-path ...` (véase sección 7) |

### 6.3 Parámetros del playbook

| Parámetro | Obligatorio | Descripción |
|---|---|---|
| `iidr_env` | No (por defecto `dev`) | Entorno de ejecución (`dev`, `pre`, `pro`). |
| `action` | No (por defecto `read`) | Operación a realizar (véase tabla anterior). |
| `source` | **Sí** (todas las acciones) | Datastore origen de la suscripción. |
| `target` | Sí en `create`, `delete`, `lock`, `unlock`, `setup-kafka` | Datastore destino. |
| `name` | Sí, salvo en `read` tipo listado | Nombre de la suscripción. |
| `description` | No (solo `create`) | Descripción libre de la suscripción. |
| `method` | No (solo `stop`) | Método de parada: `normal` (por defecto), `immediate` o `abort`. |
| `tables` | No (solo `setup-kafka`; por defecto `REAL.TB030T001,REAL.TB030T002`) | Lista de tablas `SCHEMA.TABLA` separadas por coma a incluir en la suscripción. |
| `topic_prefix` | No (solo `setup-kafka`; por defecto `esp.int.cac.bronze.grcdcpiloto`) | Prefijo de los topics de Kafka. |
| `database` | No (solo `setup-kafka`; por defecto `DB2DSNT`) | Base de datos de origen, usada en el namespace del esquema Avro. |
| `audit_record_name` | No (solo `setup-kafka`; por defecto `DB2`) | Nombre del record de auditoría en el esquema Avro. |

### 6.4 Tareas en orden de ejecución

**Fase 1 — Preparación (en el EE)**

1. **Resolución del entorno y la acción** (por defecto `dev` y `read`).
2. **Validación de la acción**: se comprueba que `action` es una de las soportadas.
3. **Validación de `source`**: obligatorio en todas las acciones.
4. **Validación de `name`**: obligatorio salvo en el listado (`read` sin `name`).
5. **Validación de `target`**: obligatorio en `create`, `delete`, `lock` y `unlock`.
6. **Carga y verificación del secreto** del entorno (como en `datastores.yml`).
7. **Registro del servidor de ejecución** con los datos de la operación.

**Fase 2 — Ejecución (en el Access Server, por SSH)**

8. **Composición de los argumentos** de `iidr.sh` según la acción y los parámetros proporcionados.
9. **Ejecución de la herramienta** con la contraseña del truststore como variable de entorno.
10. **Presentación e interpretación** del resultado JSON, con validación de estado `OK`/`SKIPPED`.

### 6.5 Ejemplos de lanzamiento

Listar las suscripciones de un datastore:

```
iidr_env: dev
action: read
source: IIDR_GR_DB2_E01_AWS_DEV
```

Ver el detalle de una suscripción:

```
iidr_env: dev
action: read
source: IIDR_GR_DB2_E01_AWS_DEV
name: DESACDC2
```

Crear una suscripción:

```
iidr_env: dev
action: create
source: IIDR_GR_DB2_E01_AWS_DEV
target: IIDR_GR_KFK_R01_AWS_DEV
name: MI_SUSCRIPCION
description: Suscripción de ejemplo
```

Arrancar y parar una suscripción:

```
iidr_env: dev
action: start
source: IIDR_GR_DB2_E01_AWS_DEV
name: MI_SUSCRIPCION
```
```
iidr_env: dev
action: stop
source: IIDR_GR_DB2_E01_AWS_DEV
name: MI_SUSCRIPCION
method: immediate
```

Eliminar una suscripción:

```
iidr_env: dev
action: delete
source: IIDR_GR_DB2_E01_AWS_DEV
target: IIDR_GR_KFK_R01_AWS_DEV
name: MI_SUSCRIPCION
```

### 6.6 Comportamientos e idempotencia por operación

- **`create`**: idempotente. Si la suscripción ya existe, devuelve `SKIPPED` sin duplicarla.
- **`delete`**: idempotente (si no existe, `SKIPPED`). La herramienta bloquea la suscripción antes de eliminarla, tal y como exige el Access Server (que no permite borrar una suscripción sin bloquearla previamente).
- **`start`**: idempotente respecto al estado (si ya está en Mirror, `SKIPPED`). El comando devuelve en cuanto la suscripción inicia la transición a `Starting`; el estado `Starting` no garantiza que el arranque finalice con éxito. Una suscripción sin mapeos de tablas ni configuración de destino completa terminará en estado `Failed` (véase sección 7).
- **`stop`**: idempotente (si ya está parada, `SKIPPED`). Trata el estado `Failed` como "no en ejecución".
- **`lock` / `unlock`**: idempotentes (bloquear una ya bloqueada, o desbloquear una ya libre, devuelve `SKIPPED`).
- **`reinit`**: **no idempotente**. Fuerza siempre una recarga completa de los datos de la suscripción. Debe usarse con precaución sobre suscripciones con datos productivos.

---

## 7. Configuración de suscripciones con destino Kafka (KCOP)

### 7.1 Contexto

Una suscripción con destino Kafka no arranca únicamente con su creación básica. Para que replique correctamente necesita, además, una configuración específica de publicación en Kafka que incluye el procesador de operaciones personalizado (KCOP, *Kafka Custom Operation Processor*) y su fichero de propiedades asociado. Si se intenta arrancar una suscripción Kafka sin esta configuración, el motor de destino falla con el mensaje "No Records Found" (no encuentra tablas/topics que publicar) y la suscripción queda en estado `Failed`.

La acción `setup-kafka` del playbook `subscriptions.yml` **automatiza toda esta configuración**. Aplica a una suscripción ya creada los mapeos de tablas, las propiedades Kafka, el KCOP y genera el fichero de propiedades en el motor Kafka, dejando la suscripción lista para arrancar (`subscription start`).

### 7.2 Elementos necesarios

Para que una suscripción con destino Kafka arranque correctamente se requieren, en este orden:

1. **La suscripción creada** (`action=create`).
2. **Al menos un mapeo de tabla** (mapeo de una tabla de origen a Kafka). Este es un **prerrequisito**: el Access Server no permite configurar ni las propiedades Kafka ni el KCOP mientras la suscripción no tenga al menos una tabla mapeada.
3. **Las propiedades Kafka de la suscripción**, en particular el *topic* de commit (nombre del topic donde el motor registra los puntos de confirmación). Por convención, su nombre es `<topic_prefix>.<nombre_suscripción>-commitstream_cdc`.
4. **El KCOP (user exit)**: la clase Java que formatea los mensajes publicados, junto con la ruta a su fichero de propiedades.
5. **El fichero de propiedades del KCOP**, ubicado en el directorio de configuración del motor Kafka. Contiene la conexión al Schema Registry, el mapeo de cada tabla a su topic de destino y las reglas de transformación del esquema Avro.

### 7.3 Cómo lo automatiza `setup-kafka`

La acción `setup-kafka` se ejecuta en dos ámbitos coordinados:

- **En el motor Kafka**: genera el fichero `<nombre_suscripción>.properties` a partir de una plantilla y lo deposita en el directorio de configuración del motor. La plantilla sustituye los valores específicos de la suscripción (nombre, tablas, topics, base de datos) y recupera los secretos del Schema Registry desde AWS Secrets Manager.
- **En el Access Server** (mediante la herramienta `iidr.sh subscription setup-kafka`, un comando compuesto que garantiza el orden obligatorio): añade los mapeos de tablas, configura las propiedades Kafka (topic de commit) y configura el user exit (KCOP) apuntando al fichero de propiedades.

`setup-kafka` **no arranca** la suscripción; una vez completado, se arranca con `action=start`. Todos los pasos son idempotentes: un mapeo ya presente se omite, y las propiedades y el user exit se (re)establecen sin efecto duplicado.

### 7.4 El fichero de propiedades (cuatro partes)

El fichero `<suscripción>.properties` se genera a partir de una plantilla y está organizado en cuatro partes, cada una precedida por un comentario que la identifica:

1. **Schema Registry**: URL del Schema Registry y credenciales de autenticación OAuth. Los datos no sensibles (URL, endpoint del emisor, scope, cluster) son fijos por entorno; los sensibles (identificador de cliente, secreto de cliente e identificador de *pool*) se recuperan de AWS Secrets Manager y **no se almacenan** en el repositorio ni de forma fija en la plantilla.
2. **Mapeo de tablas a topics**: una línea por cada tabla, asociando la tabla de origen con su topic de destino en Kafka.
3. **Envelope del esquema Avro y campos de control**: la definición de los campos de auditoría (*journal control fields*) y del envoltorio del mensaje (imágenes *before*/*after*).
4. **Transformación del esquema Avro por tabla**: dos líneas por tabla que fijan el *namespace* y el nombre del record en el esquema Avro generado.

Las partes 2 y 4 se generan dinámicamente a partir de la lista de tablas proporcionada; las partes 1 y 3 son comunes.

### 7.5 Ejemplo de configuración completa de una suscripción Kafka

```
# 1. Crear la suscripción
iidr_env: dev
action: create
source: IIDR_GR_DB2_E01_AWS_DEV
target: IIDR_GR_KFK_R01_AWS_DEV
name: MI_SUSCRIPCION

# 2. Configurar Kafka (mapeos + propiedades + KCOP + fichero de propiedades)
iidr_env: dev
action: setup-kafka
source: IIDR_GR_DB2_E01_AWS_DEV
target: IIDR_GR_KFK_R01_AWS_DEV
name: MI_SUSCRIPCION
tables: REAL.TB030T001,REAL.TB030T002
topic_prefix: esp.int.cac.bronze.grcdcpiloto
database: DB2DSNT

# 3. Arrancar
iidr_env: dev
action: start
source: IIDR_GR_DB2_E01_AWS_DEV
name: MI_SUSCRIPCION
```

### 7.6 Requisito previo: creación del topic de commit

El arranque de la suscripción requiere que el **topic de commit** exista en el clúster de Kafka. Su creación depende de los permisos de gestión de topics en el clúster de destino, que deben estar concedidos al equipo. La creación automatizada del topic de commit como parte de `setup-kafka` está prevista como ampliación (requerirá, además de los datos del Schema Registry, los del *bootstrap server* del clúster de Kafka).

---

## 8. Playbooks auxiliares de diagnóstico

### 8.1 `iidr_ee_probe.yml` — Reconocimiento del Execution Environment

Playbook de solo lectura que, ejecutado en AAP con el Execution Environment que se desee diagnosticar, reporta qué contiene y a qué tiene acceso ese entorno: si dispone de Java, si tiene montado el NFS y las librerías de IIDR, si resuelve por DNS y alcanza el Access Server por el puerto 10101, el contenido del directorio de trabajo, los permisos de escritura y la versión de Ansible y sus colecciones.

Su finalidad es determinar qué necesita el Execution Environment para poder ejecutar `iidr.sh` de forma nativa (sin el salto SSH), y diagnosticar problemas de conectividad. No requiere ninguna credencial ni parámetro.

### 8.2 `iidr_smoke_test.yml` — Prueba de extremo a extremo

Playbook que valida la cadena completa de ejecución: desde el Execution Environment, se conecta por SSH al servidor de ejecución, recupera el secreto de AWS Secrets Manager y ejecuta un `datastore list` (operación de solo lectura), comprobando que el resultado es un JSON válido con estado `OK`. Es la prueba que confirma que toda la arquitectura (EE → SSH → herramienta → Access Server → resultado) funciona antes de emplear los playbooks de gestión. Su único parámetro opcional es `iidr_env` (por defecto `dev`).

---

## 9. Playbooks de conmutación de servicio (switch HA y switch a DR)

Estos dos playbooks proceden del proyecto hermano `iidr-install`, de donde se
movieron porque son operaciones del **día a día** (conmutar la ubicación activa
del servicio), no de instalación. A diferencia del resto de playbooks de
`iidr-admin` —que corren en `localhost` y saltan por SSH al Access Server con el
patrón de dos plays—, **estos actúan directamente sobre todos los nodos IIDR del
entorno mediante grupos de inventario**, por lo que el Job Template necesita un
**inventario real** (no vale «cualquiera»). Son autocontenidos: solo usan módulos
`ansible.builtin.*`, no requieren AWS Secrets Manager ni la herramienta `iidr.sh`.

Ambos hacen lo mismo en tres plays: (1) paran y deshabilitan los servicios systemd
(`cdc_access_server`, `cdc_db2_agent`, `cdc_kafka_agent`) en las ubicaciones que
dejan de ser activas; (2) actualizan `/etc/hosts` en todos los nodos para que los
**nombres canónicos** del servicio (`iidr-gr-db2-acs-a01-aws-<env>`, etc.) resuelvan
a las IPs de la ubicación que pasa a ser activa; (3) habilitan y arrancan los
servicios en la ubicación activa (comprobando antes que el unit file existe).

### 9.1 `iidr_ha_switch_az.yml` — Conmutación entre Availability Zones

Conmuta los servicios entre **AZ-A y AZ-B** de la misma región (Irlanda). Se usa
en mantenimiento planificado de una AZ o ante un fallo de infraestructura, y para
volver después.

| Parámetro | Obligatorio | Valores | Descripción |
|---|---|---|---|
| `iidr_env` | Sí | `dev`/`pre`/`prod` | Entorno. Determina qué `vars_iidr_ha.yml` se carga. |
| `iidr_ha_active_az` | Sí | `a`/`b` | La AZ que debe quedar activa tras la conmutación. |

Las IPs por AZ de cada nombre canónico están en
`group_vars/<env>/vars_iidr_ha.yml` (variable `iidr_ha_canonical_hosts`).
Inventario requerido: grupos `iidr_az_a_<env>` e `iidr_az_b_<env>`.

### 9.2 `iidr_dr_switch.yml` — Conmutación a DR (Frankfurt) y vuelta

Conmuta entre las **tres** ubicaciones del servicio: AZ-A Irlanda, AZ-B Irlanda y
**DR Frankfurt** (eu-central-1). Es simétrico: sirve tanto para ir a DR como para
volver a cualquiera de las dos AZ de Irlanda. Solo aplica a producción. El EFS de
Frankfurt es un volumen distinto, replicado asíncronamente desde Irlanda por AWS
(RPO ~10-15 min); el switch real se hace por DNS (~1h de propagación, margen de
sobra sobre el RPO).

| Parámetro | Obligatorio | Valores | Descripción |
|---|---|---|---|
| `iidr_env` | Sí | `prod` | DR solo existe para producción. |
| `iidr_dr_target_location` | Sí | `a`/`b`/`dr` | La ubicación que debe quedar activa tras la conmutación. |

Las IPs de las tres ubicaciones por nombre canónico están en
`group_vars/prod/vars_iidr_dr.yml` (variable `iidr_dr_canonical_hosts`).
Inventario requerido: grupos `iidr_az_a_prod`, `iidr_az_b_prod`, `iidr_dr_prod`.

**Nota operativa**: antes de conmutar, las suscripciones deben pararse limpiamente
(desde este mismo `iidr-admin`) para que los checkpoints finales queden escritos en
el EFS de origen y se propaguen al destino antes de arrancar allí.

### 9.3 Job Template en AAP (distinto al del resto de iidr-admin)

- **Inventario**: real, con los grupos citados (NO «cualquiera / localhost»).
- **Credencial de máquina**: ec2-user con sudo NOPASSWD (los playbooks escalan por
  tarea con `become: true`).
- **SIN** credencial de secretos (`cdc-secret-lookup`) — no usan AWS SM.
- **NO** activar «Enable privilege escalation» en el Job Template.

---

## 10. Requisitos de configuración en AAP

Para ejecutar estos playbooks, el Job Template correspondiente en AAP debe tener asociados:

1. **El proyecto** apuntando al repositorio git, con el directorio de playbooks en `iidr-admin/ansible/`.
2. **El Execution Environment** del proyecto.
3. **Una credencial de máquina** (tipo *Machine*) para el acceso SSH al servidor de ejecución. El usuario de esta credencial debe poder conectarse al Access Server del entorno y elevar privilegios.
4. **La credencial de secretos** (Custom Credential Type `cdc-secret-lookup`), que inyecta el secreto de AWS Secrets Manager del entorno.

Los parámetros de cada operación (`iidr_env`, `action`, `source`, `target`, `name`, etc.) se proporcionan como *extra variables* o mediante un *survey* (formulario) del Job Template.

> Los playbooks de conmutación de la sección 9 son la **excepción**: usan
> inventario real y NO usan la credencial de secretos (ver 9.3).

---

## 11. Glosario

| Término | Definición |
|---|---|
| **AAP** | Ansible Automation Platform. Plataforma de automatización de Red Hat. |
| **AS** | Access Server. Componente central de control de IBM IIDR, con el que se comunica `iidr.sh` por el puerto 10101. |
| **Execution Environment (EE)** | Imagen de contenedor donde AAP ejecuta los playbooks, con Ansible y sus dependencias. |
| **Job Template** | Definición de una automatización en AAP: playbook + inventario + credenciales + parámetros. |
| **Job** | Ejecución concreta de un Job Template. |
| **Datastore** | Registro de un motor IIDR (agente DB2 o Kafka) en el Access Server: nombre, endpoint y tipo. |
| **Subscription (suscripción)** | Definición de qué tablas se replican desde un datastore origen hacia uno destino. |
| **Table mapping (mapeo de tabla)** | Asociación de una tabla de origen a un destino dentro de una suscripción. |
| **KCOP** | Kafka Custom Operation Processor. User exit (clase Java) que formatea los mensajes que una suscripción publica en Kafka. |
| **Schema Registry** | Servicio que almacena los esquemas Avro de los mensajes publicados en Kafka. |
| **AWS Secrets Manager** | Servicio de AWS que almacena secretos. Cada entorno tiene su secreto (`CDC-VAULT-<ENV>`). |
| **Custom Credential Type** | Tipo de credencial personalizado de AAP. En este proyecto (`cdc-secret-lookup`) inyecta el secreto de AWS del entorno. |
| **iidr.sh** | Herramienta CLI del proyecto que ejecuta las operaciones de administración contra el Access Server y devuelve JSON. |
| **truststore** | Almacén de confianza TLS (formato PKCS12) con las CAs que validan las conexiones seguras del Access Server. |
| **Idempotencia** | Propiedad por la que ejecutar una operación dos veces produce el mismo resultado que ejecutarla una vez (devuelve `SKIPPED` si ya estaba hecha). |
| **APP-cdcadmin** | Usuario IIDR de provisioning (crear/eliminar/conceder acceso a datastores). |
| **APP-cdcauto** | Usuario IIDR de operación del día a día (gestión de suscripciones). |
| **APP-cdcmon** | Usuario IIDR de monitorización (solo lectura). |
| **Mirror Continuous** | Estado de una suscripción que replica cambios de forma continua y activa. |
