# Pliego a `iidr-install`: `enable`/`disable` en la regla sudoers de `cdc_kafka_agent@*`

> **Audiencia**: la sesión del repo `iidr-install`
> (`../../ioeu-cdc-ogg-install-deploy/iidr-install/`).
>
> **Convención inversa**: este pliego sigue la nomenclatura espejo de
> los `requirements-to-iidr-admin-*.md` que `iidr-install` deja para
> nosotros (ver CLAUDE.md §11). Al revés: lo redacta `iidr-admin` y lo
> consume la sesión de `iidr-install`.
>
> **Resumen**: ampliar la regla sudoers NOPASSWD de `cdcuser` sobre
> `cdc_kafka_agent@*` para incluir los verbos `enable` y `disable`
> (hoy solo cubre `status`, `start`, `stop`). Es un cambio pequeño:
> una línea sudoers.

## Contexto / por qué

El pliego anterior (`requirements-to-iidr-install-systemd-template-unit.md`)
quedó **cerrado**: la unit mono-instancia se convirtió en template unit
`cdc_kafka_agent@.service`, todos los motores Kafka corren con ella y el
script `cdc_instance_mgmt.sh` es genérico (instancia por argumento).
Verificado por SSH en PRE el **2026-10-07** (host motor Kafka
`LESAWSEW141141`): la template existe, la unit vieja está retirada, el
datastore 01 migró (`active`+`enabled`) y `cdc_instance_mgmt.sh` toma la
instancia de `$2` (`CDC_INSTANCE=$2`).

**Hallazgo nuevo (2026-10-07, mismo host)**: la regla sudoers de `cdcuser`
concede NOPASSWD **solo** para `status`, `start` y `stop` sobre
`cdc_kafka_agent@*`:

```
(ALL) NOPASSWD: /usr/bin/systemctl status cdc_kafka_agent@*, /usr/bin/systemctl start cdc_kafka_agent@*, /usr/bin/systemctl stop cdc_kafka_agent@*
```

(más las equivalentes sin `@` para la unit vieja, ya irrelevantes).

**No incluye `enable` ni `disable`.** Comprobado empíricamente como
`cdcuser`:

```bash
sudo -n systemctl status     cdc_kafka_agent@<DS>   # OK, NOPASSWD
sudo -n systemctl is-enabled cdc_kafka_agent@<DS>   # pide contraseña
```

### Por qué nos bloquea

`iidr-admin`, al dar de alta un datastore Kafka **nuevo** (R02, R03…),
necesita `systemctl enable --now cdc_kafka_agent@<DS>` para que el
datastore (a) arranque y (b) **persista a reboot**. El `start` solo **no
basta**: no sobrevive a un reinicio. Como `cdcuser` no tiene NOPASSWD para
`enable`/`disable`, el flujo `delegate_to` (ec2-user → `sudo` a `cdcuser`)
falla al pedir contraseña.

`enable --now` combina `enable` (persistencia) + `start`; dado que `start`
ya está permitido, basta con **añadir `enable`** (y `disable` por simetría,
para un futuro `delete` que quiera deshabilitar la unit del datastore
borrado).

## Cambio solicitado

Ampliar la regla sudoers de `cdcuser` en los hosts motor Kafka para
incluir NOPASSWD de `enable` y `disable` sobre `cdc_kafka_agent@*`. La
línea debe quedar equivalente a:

```
cdcuser ALL=(ALL) NOPASSWD: /usr/bin/systemctl status cdc_kafka_agent@*, /usr/bin/systemctl start cdc_kafka_agent@*, /usr/bin/systemctl stop cdc_kafka_agent@*, /usr/bin/systemctl enable cdc_kafka_agent@*, /usr/bin/systemctl disable cdc_kafka_agent@*
```

## Dónde está hoy (verificado)

La regla la genera `iidr-install` y se escribe en
`/etc/sudoers.d/cdc-sudoers` (validada con `visudo -cf` antes de aplicar).
El texto de la línea se construye en:

- `bin/iidr-setup-service.sh:145` — variable `SUDOERS_LINE`, que hoy solo
  concatena `status`/`start`/`stop`:
  ```bash
  SUDOERS_LINE="cdcuser ALL=(ALL) NOPASSWD: /usr/bin/systemctl status ${SUDOERS_TARGET}, /usr/bin/systemctl start ${SUDOERS_TARGET}, /usr/bin/systemctl stop ${SUDOERS_TARGET}"
  ```
- `bin/iidr-setup-service.sh:100` — `SUDOERS_TARGET="${SERVICE_NAME}@*"`
  (patrón instanciado para la template unit; `SERVICE_NAME="cdc_kafka_agent"`).

Sugerencia: añadir a `SUDOERS_LINE` los dos verbos restantes usando el
mismo `${SUDOERS_TARGET}`, p.ej.:
```bash
SUDOERS_LINE="cdcuser ALL=(ALL) NOPASSWD: /usr/bin/systemctl status ${SUDOERS_TARGET}, /usr/bin/systemctl start ${SUDOERS_TARGET}, /usr/bin/systemctl stop ${SUDOERS_TARGET}, /usr/bin/systemctl enable ${SUDOERS_TARGET}, /usr/bin/systemctl disable ${SUDOERS_TARGET}"
```

> **Nota**: el pliego anterior citaba `bin/iidr-setup-service.sh:120` como
> el sitio de la regla sudoers; a fecha de hoy el texto de la línea está en
> la **línea 145** (`SUDOERS_LINE`) y el patrón `@*` en la **línea 100**
> (`SUDOERS_TARGET`). La línea 120 es ahora el `sed` de render de la unit
> no-template. Referimos las líneas 145/100 como fuente verificada.

## Alcance por entornos

Aplica a **todos los hosts motor Kafka de todos los entornos**
(DEV/PRE/PRO/DR). **Empezar por PRE** (donde se verificó el estado el
2026-10-07). El cambio se despliega host a host al regenerar la regla
sudoers (o re-ejecutando el paso 3 de `iidr-setup-service.sh`).

## Criterio de aceptación

En un host motor Kafka, como `cdcuser`:

```bash
sudo -n systemctl enable  cdc_kafka_agent@<DS>   # funciona NOPASSWD
sudo -n systemctl disable cdc_kafka_agent@<DS>   # funciona NOPASSWD
```

Ambos sin pedir contraseña. Con esto, el flujo `delegate_to`
(ec2-user → `sudo` a `cdcuser`) de alta de datastore nuevo puede ejecutar
`systemctl enable --now cdc_kafka_agent@<DS>` y el datastore persiste a
reboot.

---

> Cuando `iidr-install` cierre este pliego, avísanos para habilitar el
> `enable --now` en el flujo de creación de datastore de `iidr-admin`
> (nota de memoria *Ciclo de vida datastore / Golden Record*) y marcar
> resuelta la dependencia sudoers.
