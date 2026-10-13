# iidr-admin — Playbooks para AAP

Capa de automatización de `iidr-admin` para **Ansible Automation Platform
(AAP)**. Todo self-contained bajo `iidr-admin/ansible/` (no depende de
ficheros en la raíz del repo, para no interferir con `ogg-ctl` ni
`cdc-controller`).

## Cómo se despliegan los cambios (IMPORTANTE)

Hay **dos destinos** para cada cambio, con dos caminos distintos. NO existe
un "playbook de deploy" (a diferencia del hermano iidr-install): nuestro
artefacto principal es un JAR Java que hay que **compilar**, y montar eso en
el flujo de AAP sería desproporcionado. En su lugar:

1. **El NFS `/u01/repo/automation/iidr-ctl/`** (donde `iidr.sh` lee el JAR,
   scripts, truststore, properties) → lo actualiza **el desarrollador**
   ejecutando `bin/deploy.sh --target dev|pre`. Ese script compila con
   Maven, empaqueta los JARs del AS, construye el truststore, renderiza
   `iidr.properties` y hace rsync al NFS. Requiere Maven+Java+AS local
   (hoy Maven está en la cuenta del desarrollador). Un deploy por entorno
   (DEV y PRE tienen NFS distintos en el mismo path).
2. **El repo GitHub Enterprise** (de donde AAP/EE clona los playbooks) → se
   actualiza con **git push + PR + merge**. Así el EE usa la última versión
   de los playbooks.

Regla práctica al tocar iidr-admin: (a) `bin/deploy.sh` a dev y pre para el
NFS; (b) push/merge a git para los playbooks. Son responsabilidades
separadas — `deploy.sh` para el runtime, git para AAP.

## Estado

Construcción por fases (ver `../../.claude/plans/` o el plan de trabajo):

- **Fase 0 — Sonda de reconocimiento del EE** ✅ disponible
  (`playbooks/iidr_ee_probe.yml`). Sirve para averiguar qué tiene y a qué
  accede el Execution Environment custom **antes** de construir los
  playbooks de negocio.
- **Fase 1 — Playbooks de negocio** (datastores, subscriptions):
  pendiente de la salida de la sonda.

## Cómo ejecutar la sonda en AAP

1. **Project** en AAP apuntando a este repo (GitHub Enterprise), con el
   *playbook directory* / *base path* en `iidr-admin/ansible/`.
2. **Job Template**:
   - Inventory: cualquiera (los playbooks corren en `localhost`).
   - Playbook: `playbooks/iidr_ee_probe.yml`.
   - Execution Environment: **el EE custom** que se quiere diagnosticar.
   - Credentials: **ninguna** (la sonda es solo lectura, sin secretos).
3. Lanzar y guardar el output completo del job.

La sonda reporta: Java, montaje de `/u01/repo` + JARs de IIDR +
truststore, conectividad TCP al Access Server (`:10101`), contenido del
checkout del proyecto, permisos de `/tmp` y SELinux, versión de Ansible y
colecciones. Con eso se decide cómo el EE accede a los JARs de IIDR que
necesita `iidr.sh` (montar NFS / hornear en la imagen / meter en el repo).

## Ejecución local (baseline de referencia)

En un host que ya funciona (p.ej. el AS de DEV `LESAWSEW131211`), la sonda
se puede correr en local para tener una foto de referencia:

```bash
cd iidr-admin/ansible
ansible-playbook playbooks/iidr_ee_probe.yml
```

## Playbooks de negocio (Fase 1 — solo lectura por ahora)

Patrón de 2 plays validado (ver `iidr_smoke_test.yml`): play 1 en
localhost resuelve el secreto AWS SM y registra el host destino; play 2
(`hosts: <la 11>`) ejecuta `iidr.sh` por SSH real (become cdcuser).

- **`datastores.yml`** — `action=read`:
  - sin `name` → `datastore list`
  - con `name` → `datastore show --name <name>`
- **`subscriptions.yml`** — `source` obligatorio; `action`:
  - `read` → `list --source S` (sin name) o `show --source S --name N`
  - `create` → `create --source S --target T --name N [--description D]`
  - `delete` → `delete --source S --target T --name N`
  - `start` / `stop` (`--method`) / `reinit` → `--source S --name N`
  - `lock` / `unlock` → `--source S --target T --name N`
  - `setup-kafka` → configura una sub Kafka para arrancar: mapeos + kafka
    properties + user exit (KCOP) + genera el `.properties` en el motor
    Kafka. Extra-vars: `tables` (default REAL.TB030T001,REAL.TB030T002),
    `topic_prefix` (default esp.int.cac.bronze.grcdcpiloto), `database`
    (default DB2DSNT), `audit_record_name` (default DB2). Requiere en el
    secreto AWS SM: `KAFKA_CLIENT_ID/SECRET/POOL_ID`. NO arranca
    la sub (usar `action=start` después).
  - (escritura como cdcauto; probar con la sub `DESACDC3` en DEV)

Job Template AAP: EE custom + Machine Credential (ec2-user) + Custom
Credential AWS SM (`cdc-custom-credential`). Extra-vars: `iidr_env`
(dev|pre|pro, default dev), `action=read`, y `source`/`name` según el caso.

Ejemplo de lanzamiento (survey / -e):
```
iidr_env=dev  action=read  source=IIDR_GR_DB2_E01_AWS_DEV
iidr_env=dev  action=read  source=IIDR_GR_DB2_E01_AWS_DEV  name=DESACDC2
```

(create/delete/grant y las operaciones de estado de subscription se
añadirán en la siguiente iteración; usan cdcadmin y más flags.)

## Playbooks de conmutación de servicio (switch HA / DR)

Movidos aquí desde `iidr-install` (son operación, no instalación). A diferencia
del resto de playbooks de iidr-admin (que corren en `localhost` + SSH a la 11),
**estos actúan directamente sobre grupos de inventario** de todos los nodos IIDR
del entorno — necesitan **inventario real** en el Job Template, no «cualquiera».
Son self-contained: solo `ansible.builtin.*`, sin secretos, sin `iidr.sh`.

- **`iidr_ha_switch_az.yml`** — conmuta los servicios IIDR entre **AZ-A y AZ-B**
  de la misma región (Irlanda). Tres plays: (1) para servicios en la AZ que deja
  de ser activa, (2) actualiza `/etc/hosts` de todos los nodos con las IPs de la
  nueva AZ, (3) arranca servicios en la AZ activa.
  - Extra-vars: `iidr_env` (`dev`|`pre`|`prod`), `iidr_ha_active_az` (`a`|`b`).
  - Vars: `group_vars/<env>/vars_iidr_ha.yml` (`iidr_ha_canonical_hosts`).
  - Inventario: grupos `iidr_az_a_<env>` e `iidr_az_b_<env>`.

- **`iidr_dr_switch.yml`** — conmuta entre las **tres** ubicaciones del servicio:
  AZ-A Irlanda, AZ-B Irlanda y **DR Frankfurt** (eu-central-1). Simétrico (sirve
  para ir a DR y para volver). Solo `prod`. Mismos tres plays que el HA pero sobre
  las tres ubicaciones.
  - Extra-vars: `iidr_env=prod`, `iidr_dr_target_location` (`a`|`b`|`dr`) = la
    ubicación que queda ACTIVA.
  - Vars: `group_vars/prod/vars_iidr_dr.yml` (`iidr_dr_canonical_hosts`, con las
    3 IPs por nombre canónico).
  - Inventario: grupos `iidr_az_a_prod`, `iidr_az_b_prod`, `iidr_dr_prod`.

Job Template AAP para ambos: Machine Credential (ec2-user con sudo NOPASSWD),
**inventario real** con los grupos citados, SIN credential AWS SM, y **sin**
«Enable privilege escalation» (escalan por tarea). Ejemplos:
```
# Conmutar PRE a AZ-B, y volver a AZ-A:
iidr_env=pre  iidr_ha_active_az=b        # y luego  iidr_ha_active_az=a
# Ir a DR y volver a AZ-B de Irlanda:
iidr_env=prod iidr_dr_target_location=dr # y luego  iidr_dr_target_location=b
```

## Estructura

```
ansible/
├── ansible.cfg                     # config self-contained
├── group_vars/
│   ├── all/vars.yml                # datos por entorno + claves del secreto
│   ├── pre/vars_iidr_ha.yml        # IPs por AZ (switch HA, PRE)
│   └── prod/
│       ├── vars_iidr_ha.yml        # IPs por AZ (switch HA, PROD)
│       └── vars_iidr_dr.yml        # IPs de las 3 ubicaciones (switch DR)
├── playbooks/
│   ├── iidr_ee_probe.yml           # Fase 0: sonda del EE
│   ├── iidr_smoke_test.yml         # piloto: valida EE->SSH->iidr.sh
│   ├── datastores.yml              # Fase 1: datastore read
│   ├── subscriptions.yml           # Fase 1: subscription read
│   ├── iidr_ha_switch_az.yml       # switch de AZ (HA) — inventario real
│   └── iidr_dr_switch.yml          # switch a DR / vuelta — inventario real
└── README.md                       # este fichero
```
