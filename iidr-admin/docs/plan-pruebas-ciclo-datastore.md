# Plan de pruebas — ciclo de vida del datastore + suscripción (iidr-admin)

> Estado a cierre del **2026-10-07**. Todo el código está en
> `feature/iidr-admin-initial` (pusheado). Antes de probar vía AAP: **mergear a
> `main`** o apuntar el Job Template a la feature (Update Revision on Launch está
> activo, así que sincroniza solo).

## Leyenda de bloqueos

- 🟢 **Probable ya**: no depende de nada pendiente.
- 🟡 **Depende de un R02 real**: hay que definir un segundo datastore (nombre +
  puerto + instancia en el motor) para probar el alta "nueva" de verdad.
- 🔴 **Bloqueado por DB2**: necesita conexión al motor DB2 / started task
  (puertos de firewall), previsto **viernes 2026-10-10**.

---

## FASE 0 — Prerrequisitos (una vez)

1. **Mergear `feature/iidr-admin-initial` a `main`** (PR) para que el Job
   Template en `main` tenga todo: acciones `create/backup/enable/grant`, fixes de
   derivación y de mensajes de error, inventario ligero, grupos LDAP en
   group_vars.
2. **Recompilar + desplegar el JAR** para que surta efecto el fix de mensajes de
   error CHCCLP (commit `039adf3`, es Java):
   ```
   mvn -DskipTests package   # en iidr-admin/
   AS_RENDER_SKIP=1 TLS_TRUST_STORE_PASSWORD='<pwd>' bin/deploy.sh --target pre
   ```
   (Sin esto, los errores seguirán saliendo como `"message": null`.)
3. **Confirmar claves en el secreto `CDC-VAULT-PRE`** (las usa grant y
   setup-kafka): `IIDR_KAFKA_DS_USER_ENC`, `IIDR_KAFKA_DS_PASSWORD_ENC`,
   `IIDR_DB2_DS_USER_ENC`, `IIDR_DB2_DS_PASSWORD_ENC`, `KAFKA_CLIENT_ID`,
   `KAFKA_CLIENT_SECRET`, `KAFKA_POOL_ID`.

---

## FASE 1 — Pruebas inocuas sobre lo YA existente (🟢 hoy mismo)

Validan la lógica sin crear nada nuevo. Datastore de referencia:
`IIDR_GR_DB2_KFK_R01_AWS_PRE`, funcional `KAFKA_PILOTO_01_PRE`.

### 1.1 `read` (list y show)
```
iidr_env=pre action=read
iidr_env=pre action=read name=IIDR_GR_DB2_KFK_R01_AWS_PRE
```
Esperado: OK, lista de datastores / detalle del R01.

### 1.2 `validate` (3 patas)
```
iidr_env=pre action=validate name=IIDR_GR_DB2_KFK_R01_AWS_PRE funcional=KAFKA_PILOTO_01_PRE
```
Esperado: pata AS OK (existe) + pata motor OK (instancia + settings con bootstrap
que coincide con la funcional). Ya probado antes; re-confirma tras el merge.

### 1.3 `backup` (ya probado ✅, repetible)
```
iidr_env=pre action=backup name=IIDR_GR_DB2_KFK_R01_AWS_PRE
```
Esperado: respalda los *.properties del conf a
`<instance_root>/IIDR_GR_DB2_KFK_R01_AWS_PRE_conf_backup_<timestamp>/`.

### 1.4 `create` idempotente (SKIPPED)
```
iidr_env=pre action=create name=IIDR_GR_DB2_KFK_R01_AWS_PRE host=iidr-gr-db2-kfk-r01-aws-pre.es.mapfre.net port=<puerto R01>
```
Esperado: **SKIPPED** ("ya existe"). Valida que `create` compone bien y que
cdcauto tiene permiso, sin efectos. (El puerto del R01 se mira en la Management
Console: Datastore → propiedades.)

### 1.5 `grant` idempotente (SKIPPED)
```
iidr_env=pre action=grant name=IIDR_GR_DB2_KFK_R01_AWS_PRE funcional=KAFKA_PILOTO_01_PRE
```
Esperado: para cada grupo (sysadministrator, monitor) → **SKIPPED** ("ya tiene
access connection"). Valida la derivación del kind (kafka → credenciales kafka),
los nombres de grupo LDAP, y que cdcauto puede consultar access connections.
**No usa las credenciales del motor** en el camino SKIPPED (no las descifra).

### 1.6 `enable` idempotente (🟢 ya desbloqueado — sudoers cerrado)
```
iidr_env=pre action=enable name=IIDR_GR_DB2_KFK_R01_AWS_PRE
```
Esperado: `systemctl enable --now cdc_kafka_agent@IIDR_GR_DB2_KFK_R01_AWS_PRE`
→ ya está enabled+active, idempotente; la unit queda `active`. Valida que la
regla sudoers `enable` ampliada por iidr-install funciona NOPASSWD.
> **Nota**: operar la unit del R01 (en producción) es inocuo aquí porque ya está
> activa y enable es idempotente, pero si prefieres no tocar el R01, deja esta
> prueba para el R02 (1.6 se cubre en FASE 2).

---

## FASE 2 — Alta completa de un datastore NUEVO (🟡 requiere R02)

Esto valida el multi-datastore de verdad y las hipótesis de permisos de cdcauto
en operaciones que **sí cambian estado**. Prerrequisitos del R02:

- **Decidir el nombre**: p.ej. `IIDR_GR_DB2_KFK_R02_AWS_PRE`.
- **Crear su instancia en el motor** (carpeta `instance/<R02>/` con su puerto).
  ⚠️ **¿Quién crea la instancia?** Hoy la instancia inicial la crea iidr-install.
  Para un R02 hay que decidir si la crea iidr-install (pliego) o si iidr-admin
  debe añadir un paso de creación de instancia. **Punto de diseño abierto** — ver
  "Decisiones pendientes" al final. Sin instancia, `enable` y `ensure-settings`
  fallan (no existe el conf) y `create` registra un datastore que no conecta.
- **Añadir el R02 al inventario ligero** `iidr_datastores.yaml` (lo escribe un
  humano) bajo su funcional, con `kind: kafka` y el server canónico.

Orden una vez existe la instancia del R02:

### 2.1 `create` (registro en AS) — primer cambio de estado real
```
iidr_env=pre action=create name=IIDR_GR_DB2_KFK_R02_AWS_PRE host=iidr-gr-db2-kfk-r01-aws-pre.es.mapfre.net port=<puerto R02> description="Segundo datastore kafka PILOTO"
```
Esperado: OK (crea). **Confirma la hipótesis de que cdcauto puede crear.** Si
devuelve ERROR de permisos → pasar al patrón properties-temporal con APP-cdcadmin.

### 2.2 `enable` (habilitar+arrancar la unit del R02)
```
iidr_env=pre action=enable name=IIDR_GR_DB2_KFK_R02_AWS_PRE
```
Esperado: `cdc_kafka_agent@IIDR_GR_DB2_KFK_R02_AWS_PRE` queda enabled+active, y
**R01 sigue active** (multi-datastore independiente). Criterio de aceptación del
pliego systemd.

### 2.3 `ensure-settings` (properties del motor del R02)
```
iidr_env=pre action=ensure-settings name=IIDR_GR_DB2_KFK_R02_AWS_PRE funcional=<funcional del R02>
```
Esperado: rellena `kafkaproducer.properties` / `kafkaconsumer.properties` en el
conf del R02 con los settings de su funcional + client_secret de AWS SM.

### 2.4 `grant` (access connection a los grupos) — primer grant real
```
iidr_env=pre action=grant name=IIDR_GR_DB2_KFK_R02_AWS_PRE funcional=<funcional del R02>
```
Esperado: OK para sysadministrator y monitor (crea access connection; usa las
credenciales kafka del motor). **Confirma que cdcauto puede conceder** y que las
credenciales descifran bien.

### 2.5 `validate` (cierre del alta)
```
iidr_env=pre action=validate name=IIDR_GR_DB2_KFK_R02_AWS_PRE funcional=<funcional del R02>
```
Esperado: las 3 patas OK. El R02 queda dado de alta y listo para suscripciones.

### 2.6 `backup` + `delete` (ciclo de baja, al terminar la prueba)
```
iidr_env=pre action=backup name=IIDR_GR_DB2_KFK_R02_AWS_PRE
iidr_env=pre action=delete name=IIDR_GR_DB2_KFK_R02_AWS_PRE
```
Esperado: backup de los properties del R02, luego delete (OK; idempotente). Deja
el entorno limpio tras la prueba. ⚠️ El delete del AS **no** borra el conf del
motor ni deshabilita la unit — si se quiere limpieza total, `systemctl disable`
de la unit del R02 (hoy no hay acción `disable` en el playbook — ver pendientes).

---

## FASE 3 — Suscripción PREPCDC2 completa (🔴 bloqueado por DB2)

La suscripción PREPCDC2 ya está **creada** (create ✅). Falta configurarla y
arrancarla, y eso **necesita DB2** (leer el catálogo de las tablas origen).

### 3.1 `setup-kafka` (🔴 — falló con rc=-2404 por DB2 caído)
```
iidr_env=pre action=setup-kafka funcional=KAFKA_PILOTO_01_PRE name=PREPCDC2 tables=REAL.TB030T001,REAL.TB030T002 database=<SSID/BD DB2 real>
```
- Hoy falla en el paso de mapeo de tablas: `CHCCLP rc=-2404 "source table cannot
  be found"` porque el motor DB2 origen no arranca (puertos firewall → viernes).
- El `.properties` del KCOP **sí** se deposita (Play 1b corre antes del paso DB2);
  eso ya se verificó.
- ⚠️ **`database`**: va al namespace AVRO del `.properties`
  (`..._SET_NAMESPACE=<database>.SCHEMA.TABLA`), NO a un comando. La primera vez
  salió `DB2DSNT` (default). **Poner el SSID/BD DB2 real** al relanzar.
- **Reintentar el viernes**, cuando DB2 esté accesible.

### 3.2 `start` (🔴 — depende de 3.1)
```
iidr_env=pre action=start funcional=KAFKA_PILOTO_01_PRE name=PREPCDC2
```
Esperado (viernes, tras 3.1 OK): la suscripción arranca y empieza a replicar.

### 3.3 Verificaciones E2E (🔴 — viernes)
- **Commit topic**: confirmar si `setup-kafka` crea el commit topic en Kafka
  (`esp.int.cac.bronze.grcdcpiloto.PREPCDC2-commitstream`). Antes no había
  permisos; ahora con pool-lbGAr sí. Verificar en Confluent.
- **Topics de datos**: que aparecen `esp.int.cac.bronze.grcdcpiloto.tb030t001_cdc`
  y `...tb030t002_cdc` con mensajes al hacer cambios en DB2.
- **Schema Registry**: que se registran los esquemas AVRO con el namespace
  correcto (`<database>.REAL.TB030T00X`).

---

## Orden recomendado

1. **FASE 0** (merge + deploy JAR + claves secreto) — imprescindible antes de nada.
2. **FASE 1** (1.1→1.6) — rápido, inocuo, valida toda la lógica nueva hoy mismo.
3. **FASE 2** — cuando tengas el R02 definido (nombre, puerto, instancia,
   entrada en inventario). Resuelve antes la decisión de "quién crea la instancia".
4. **FASE 3** — el **viernes**, cuando haya DB2.

---

## Decisiones pendientes (para retomar)

- **Quién crea la instancia del motor de un datastore nuevo (R02)**: hoy la
  inicial la crea iidr-install. Para R02+, ¿pliego a iidr-install, o iidr-admin
  añade un paso de creación de instancia (`dminstance`)? Bloquea FASE 2.
- **Acción `disable`** en el playbook (para la baja total de un datastore:
  `systemctl disable` + retirar conf). Hoy `delete` solo quita el registro del AS.
  La sudoers de `disable` ya está (iidr-install la amplió junto con enable).
- **Semántica de `validate` en modo `new`** (si el datastore ya existe: ¿SKIPPED
  idempotente o error "pediste new pero existe"?).
- **Verificar permiso real de cdcauto** para `create` y `grant` (hipótesis a
  confirmar en FASE 2; si falla, patrón properties-temporal con APP-cdcadmin).

---

## Checklist rápido de extra-vars

| Acción | Extra-vars mínimas |
|---|---|
| read (list) | `iidr_env action=read` |
| read (show) | `iidr_env action=read name` |
| validate | `iidr_env action=validate name` (+`funcional` opcional) |
| backup | `iidr_env action=backup name` |
| create | `iidr_env action=create name host port` (+`description`) |
| enable | `iidr_env action=enable name` |
| ensure-settings | `iidr_env action=ensure-settings name funcional` |
| grant | `iidr_env action=grant name funcional` |
| delete | `iidr_env action=delete name` |
| sub setup-kafka 🔴 | `iidr_env action=setup-kafka funcional name tables database` |
| sub start 🔴 | `iidr_env action=start funcional name` |
