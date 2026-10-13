# Pliego a `iidr-install`: systemd template unit para multi-datastore Kafka

> **Audiencia**: la sesión del repo `iidr-install`
> (`../../ioeu-cdc-ogg-install-deploy/iidr-install/`).
>
> **Convención inversa**: este pliego sigue la nomenclatura espejo de
> los `requirements-to-iidr-admin-*.md` que `iidr-install` deja para
> nosotros (ver CLAUDE.md §11). Al revés: lo redacta `iidr-admin` y lo
> consume la sesión de `iidr-install`.
>
> **Resumen**: convertir la unit mono-instancia `cdc_kafka_agent.service`
> en una **systemd template unit** `cdc_kafka_agent@.service` y garantizar
> que `cdc_instance_mgmt.sh` sea genérico (instancia **solo** por `$1`),
> para que un mismo motor Kafka pueda alojar N datastores (R01, R02, R03…)
> arrancables/parables de forma independiente.

## Contexto / por qué

Estamos migrando el ciclo de vida de datastores IIDR a `iidr-admin`
(modelo "Golden Record"). Un datastore es una unidad de configuración
Kafka ligada a una aplicación funcional. **Un mismo motor Kafka puede
alojar N datastores**, cada uno con su propia instancia (carpeta en
`instance/<DATASTORE>/`) y su propia gestión de arranque/parada.

El modelo systemd actual es **mono-instancia**: hay una única unit con el
nombre del datastore cableado. Eso impide arrancar o parar cada datastore
por separado, que es el requisito que habilita el multi-datastore.

Decisión tomada el **2026-10-06** (verificada por SSH en PRE ese día):
pasar a una template unit parametrizada por `%i`.

## Estado actual (verificado)

### Script generador y unit mono-instancia

- La unit la genera `iidr-install` desde
  `templates/kafka-agent.service.template` vía
  `bin/iidr-setup-service.sh --component kafka-agent --instance-name <DS>`.
  El wrapper fija `SERVICE_NAME="cdc_kafka_agent"`
  (`bin/iidr-setup-service.sh:71`) e instala en
  `/etc/systemd/system/cdc_kafka_agent.service`
  (`bin/iidr-setup-service.sh:101`).
- El render sustituye `${INSTANCE_NAME}` por el valor de `--instance-name`
  con un `sed` (`bin/iidr-setup-service.sh` paso 1, `NEEDS_INSTANCE=true`).
  Resultado: el nombre del datastore queda **cableado en el fichero
  instalado**, no se pasa en tiempo de `systemctl`.
- En PRE, lo que corre hoy es equivalente a:
  ```ini
  ExecStart=/u01/IIDR/scripts/cdc_instance_mgmt.sh start IIDR_GR_DB2_KFK_R01_AWS_PRE
  ExecStop=/u01/IIDR/scripts/cdc_instance_mgmt.sh stop  IIDR_GR_DB2_KFK_R01_AWS_PRE
  ```
  con `Type=oneshot`, `RemainAfterExit=yes`, `User=cdcuser`.

> **Nota de divergencia** (a reconciliar por `iidr-install`, no bloquea
> este pliego): `templates/kafka-agent.service.template` en git ya usa
> `${INSTANCE_NAME}` y `Type=oneshot` / `RemainAfterExit=yes`, mientras
> que `docs/systemd-services.md:83-89` documenta la unit con
> `Type=forking` + `Restart=always` + `SuccessExitStatus=143` y un
> nombre distinto (`IIDR_GR_KFK_R01_AWS_DEV`). La referencia operativa es
> lo verificado en PRE el 2026-10-06. Conviene alinear la doc al cerrar
> este cambio.

### El script `cdc_instance_mgmt.sh` tiene un nombre fijo cableado

Verificado en
`templates/cdc_instance_mgmt.sh.template` (lo instala
`bin/iidr-setup-instance-mgmt.sh`, marcado "No editar a mano"):

- **`CDC_INSTANCE="@@INSTANCE_NAME@@"`** (línea 24): el nombre de la
  instancia se **cablea en tiempo de render** del wrapper, no se toma del
  argumento posicional.
- El script **sí** acepta un 2º argumento posicional
  (`INSTANCE_OVERRIDE=${2:-}`, línea 292), pero **solo como sanity-check**:
  si se pasa y no coincide con `CDC_INSTANCE`, aborta
  (líneas 306-311):
  ```bash
  if [ -n "$INSTANCE_OVERRIDE" ] && [ "$INSTANCE_OVERRIDE" != "$CDC_INSTANCE" ]; then
      log ERROR "Este script gestiona '$CDC_INSTANCE', no '$INSTANCE_OVERRIDE'"
      exit 1
  fi
  ```
- Toda la lógica interna (`start_instance`, `stop_instance`,
  `status_instance`, snapshot/restore de subscriptions, rutas de traces y
  logs) usa la variable **`$CDC_INSTANCE`**, no `$1`/`$2`.

Conclusión: **el template NO es genérico hoy**. Un mismo script servido
por el wrapper solo puede gestionar la instancia con la que se renderizó.
Esto es incompatible con una template unit, que pasa un `%i` distinto por
cada datastore al **mismo** script `/u01/IIDR/scripts/cdc_instance_mgmt.sh`.

### Diseño de referencia

Alinear terminología con
`iidr-install/docs/design-cdc-instance-mgmt.md` (REV.3): modo degradado
por defecto, integración opcional con `iidr-admin`, interfaz
`cdc_instance_mgmt.sh {start | stop [-a] | status} <INSTANCE>`. Este
pliego **no** cambia esa semántica; solo pide que `<INSTANCE>` venga
realmente del argumento y que la unit sea una template.

## Cambios solicitados

### Obligatorio 1 — Template unit `cdc_kafka_agent@.service`

Convertir la unit mono-instancia en una **systemd template unit**.

> **El bloque siguiente es ILUSTRATIVO**, no una transcripción de la unit
> real de PRE. Lo que pedimos es: **partir de la unit `cdc_kafka_agent.service`
> que `iidr-install` instala hoy y PRESERVAR sus directivas `[Unit]`/`[Service]`
> actuales**, cambiando únicamente lo imprescindible para convertirla en
> template (`%i` en `ExecStart`/`ExecStop`, `Description` con `%i`, y el
> nombre de fichero con `@`). Las directivas `After`/`Wants`/`Requires` de
> abajo son un ejemplo plausible; usad las reales de PRE, no estas. Lo único
> que afirmamos como verificado en PRE (2026-10-06) es `Type=oneshot`,
> `RemainAfterExit=yes`, `User=cdcuser` (mantenedlas).

```ini
[Unit]
Description=CDC Kafka instance %i
After=local-fs.target remote-fs.target network-online.target
Wants=local-fs.target remote-fs.target network-online.target
Requires=network-online.target remote-fs.target

[Service]
User=cdcuser
Type=oneshot
RemainAfterExit=yes
ExecStart=/u01/IIDR/scripts/cdc_instance_mgmt.sh start %i
ExecStop=/u01/IIDR/scripts/cdc_instance_mgmt.sh stop  %i

[Install]
WantedBy=default.target
```

Puntos clave:

- **Mantener** lo que hay hoy: `Type=oneshot`, `RemainAfterExit=yes`,
  `User=cdcuser`.
- `%i` = el nombre del datastore, que se pasa **al habilitar/arrancar**:
  `systemctl enable --now cdc_kafka_agent@<DATASTORE>`.
- El fichero de template se instala como
  `/etc/systemd/system/cdc_kafka_agent@.service` (nótese la `@` antes de
  `.service`; sin instancia en el nombre del fichero).
- `bin/iidr-setup-service.sh` para `--component kafka-agent` deja de
  cablear `--instance-name` en el `ExecStart`/`ExecStop`. El
  `--instance-name` sigue siendo útil para **habilitar el datastore 01**
  en la instalación inicial (`systemctl enable --now
  cdc_kafka_agent@<DS01>`), pero ya **no** se sustituye dentro del unit.
- Reglas sudoers: hoy son por nombre de servicio literal
  (`.../systemctl start cdc_kafka_agent`, etc.,
  `bin/iidr-setup-service.sh:120`). Con template unit, `cdcuser` debe
  poder operar instancias `cdc_kafka_agent@*`. Ajustar la línea sudoers
  para cubrir la forma instanciada, p.ej.:
  ```
  cdcuser ALL=(ALL) NOPASSWD: /usr/bin/systemctl status cdc_kafka_agent@*, /usr/bin/systemctl start cdc_kafka_agent@*, /usr/bin/systemctl stop cdc_kafka_agent@*
  ```
  (`iidr-admin` necesita que `cdcuser` pueda arrancar/parar cualquier
  `cdc_kafka_agent@<DS>` sin contraseña.)

### Obligatorio 2 — `cdc_instance_mgmt.sh` genérico (instancia solo por `$1`)

El script debe tomar la instancia **exclusivamente del argumento
posicional** (`$1` tras la acción), sin ninguna variable `CDC_INSTANCE`
fija ni cableada por el render del wrapper. En concreto:

- Eliminar la línea `CDC_INSTANCE="@@INSTANCE_NAME@@"`
  (`templates/cdc_instance_mgmt.sh.template:24`) y el placeholder
  `@@INSTANCE_NAME@@` asociado en `bin/iidr-setup-instance-mgmt.sh`.
- Derivar `CDC_INSTANCE` del argumento. Hoy la firma es
  `{start|stop|status} <instance>`, con la acción en `$1` y la instancia
  como 2º posicional opcional (`INSTANCE_OVERRIDE`). Debe pasar a ser
  **obligatoria** y ser la fuente única del nombre, por ejemplo:
  ```bash
  ACTION=$1
  CDC_INSTANCE=${2:-}
  [ -n "$CDC_INSTANCE" ] || { echo "falta <instance>"; show_help >&2; exit 1; }
  ```
  (y eliminar el bloque de sanity-check `INSTANCE_OVERRIDE` de las líneas
  306-311, que deja de tener sentido.)
- El resto del script ya usa `$CDC_INSTANCE` en todas partes
  (`start_instance`, `stop_instance`, `status_instance`,
  `ACTIVE_SUBSCRIPTION_FILE`, rutas de trace/log), así que el cambio es
  acotado al parsing y al template.
- Revisar que **ningún otro** valor quede cableado por datastore en el
  template o en su wrapper (el resto de placeholders —
  `@@CDC_INSTALL_DIR@@`, `@@SCRIPT_DIR@@`, `@@IIDR_ADMIN_SH@@`,
  `@@USER_FOLDER@@` — son por motor/host, no por datastore, y pueden
  seguir igual; `USER_FOLDER` es común a todas las instancias del mismo
  agente).

> Con estos dos cambios, el **mismo** `/u01/IIDR/scripts/cdc_instance_mgmt.sh`
> gestiona cualquier datastore según el `%i` que systemd le pase por
> argumento.

### Opcional (fase 2, NO urgente) — Mover los GRANT a `iidr-admin`

> **Esto NO es requisito de este pliego.** Se cita como mejora futura
> para evitar duplicidad de responsabilidades; no condiciona la
> aceptación de los Obligatorios 1 y 2.

A futuro, `iidr-install` podría **dejar de ejecutar los GRANT ×3** del
datastore durante la instalación, delegándolos en `iidr-admin` vía
`iidr.sh datastore grant`. Para la condición inicial (datastore 01) no es
necesario; se abordará en una fase 2 coordinada aparte.

## Reparto de responsabilidades resultante

- **`iidr-install`**:
  - Instala los binarios del motor Kafka.
  - Provee la **template unit** `cdc_kafka_agent@.service`.
  - Provee el **script genérico** `cdc_instance_mgmt.sh` (instancia por
    `$1`).
  - **Sigue creando el datastore 01 y arrancándolo** en la instalación
    inicial (condición inicial), ahora vía
    `systemctl enable --now cdc_kafka_agent@<DS01>`.
  - (Fase 2 opcional) dejaría de dar los GRANT del datastore.

- **`iidr-admin`**:
  - Al crear un datastore **nuevo** (R02, R03…): ejecuta
    `systemctl enable --now cdc_kafka_agent@<DS>` (vía el flujo
    `delegate_to` ec2-user + sudo a `cdcuser`), genera los
    `kafkaproducer.properties`/`kafkaconsumer.properties` del motor y
    (fase 2) los grants.
  - No modifica ni la template unit ni el script; los **consume**.

## Riesgos / transición (datastore 01)

**La condición inicial no debe romperse.** Hoy el datastore 01 está
arrancado con la unit vieja `cdc_kafka_agent.service` (mono-instancia).
Al migrar a `cdc_kafka_agent@.service` hay que contemplar la transición
**sin dejar el motor parado**. Puntos de atención a coordinar:

1. **Coexistencia de nombres**: `cdc_kafka_agent.service` (vieja) y
   `cdc_kafka_agent@.service` (nueva template) son units distintas para
   systemd. Instalar la template **no** sustituye ni deshabilita la vieja
   automáticamente.
2. **Orden de corte**. Secuencia propuesta (a validar en PRE):
   1. Instalar `cdc_kafka_agent@.service` y el `cdc_instance_mgmt.sh`
      genérico.
   2. `systemctl enable cdc_kafka_agent@<DS01>` (sin `--now` todavía).
   3. En ventana controlada: `systemctl stop cdc_kafka_agent` (vieja) y
      `systemctl start cdc_kafka_agent@<DS01>` (nueva). Dado que ambas
      llaman al mismo `dmts64 -I <DS01>` / `dmshutdown -I <DS01>`,
      **no deben solaparse** (evitar dos units "activas" apuntando al
      mismo motor a la vez).
   4. Verificar motor arriba con la nueva unit; luego
      `systemctl disable cdc_kafka_agent` y retirar el fichero
      `/etc/systemd/system/cdc_kafka_agent.service` + enlaces en
      `default.target.wants/`, y `daemon-reload`.
3. **Boot automático**: tras la migración, el `WantedBy=default.target`
   debe quedar solo en la instancia (`cdc_kafka_agent@<DS01>`), no en la
   unit vieja, para que el reboot levante el datastore por la vía nueva.
4. **Sudoers**: desplegar la regla `cdc_kafka_agent@*` **antes** de que
   `iidr-admin` intente operar instancias, o los `systemctl start/stop`
   delegados fallarán por falta de NOPASSWD.
5. **Entornos**: la transición aplica host a host (DEV/PRE/PRO/DR).
   Empezar por PRE (donde se verificó el estado el 2026-10-06).

## Criterios de aceptación

`iidr-admin` dará el cambio por hecho cuando, en un host con el motor
Kafka instalado:

1. Existe `/etc/systemd/system/cdc_kafka_agent@.service` (template unit)
   con `ExecStart=.../cdc_instance_mgmt.sh start %i` y
   `ExecStop=.../cdc_instance_mgmt.sh stop %i`, `Type=oneshot`,
   `RemainAfterExit=yes`, `User=cdcuser`.
2. `cat /u01/IIDR/scripts/cdc_instance_mgmt.sh` **no contiene** ningún
   nombre de datastore cableado (ni `CDC_INSTANCE="IIDR_..."`); la
   instancia se resuelve del argumento posicional.
3. Arranque/parada **independiente por datastore** funciona:
   ```bash
   systemctl enable --now cdc_kafka_agent@IIDR_GR_DB2_KFK_R01_AWS_PRE
   systemctl enable --now cdc_kafka_agent@IIDR_GR_DB2_KFK_R02_AWS_PRE
   systemctl status  cdc_kafka_agent@IIDR_GR_DB2_KFK_R01_AWS_PRE   # active
   systemctl stop    cdc_kafka_agent@IIDR_GR_DB2_KFK_R02_AWS_PRE   # para R02…
   systemctl status  cdc_kafka_agent@IIDR_GR_DB2_KFK_R01_AWS_PRE   # …R01 sigue active
   ```
   Parar R02 **no** afecta a R01 (y viceversa): cada
   `cdc_kafka_agent@<DS>` gestiona solo su `dmts64 -I <DS>` /
   `dmshutdown -I <DS>`.
4. `cdcuser` puede ejecutar `sudo systemctl {start|stop|status}
   cdc_kafka_agent@<DS>` sin contraseña (sudoers `cdc_kafka_agent@*`).
5. El datastore 01, migrado desde la unit vieja, **sigue arriba** y
   levanta correctamente en un reboot por la nueva template unit; la unit
   vieja `cdc_kafka_agent.service` queda retirada.

---

> Cuando `iidr-install` cierre este pliego, avísanos para actualizar el
> flujo de creación de datastore en `iidr-admin` (nota de memoria
> *Ciclo de vida datastore / Golden Record*) y marcar resuelta la
> dependencia systemd.
