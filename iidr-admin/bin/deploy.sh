#!/usr/bin/env bash
# =============================================================================
# deploy.sh — Despliega iidr-admin en el repositorio NFS del entorno.
#
# Uso:
#   bin/deploy.sh                            # despliegue local (DEV)
#   bin/deploy.sh --target dev               # idem
#   bin/deploy.sh --target pre               # despliegue remoto vía SSH a PRE
#   bin/deploy.sh --target pro               # despliegue remoto vía SSH a PRO
#   bin/deploy.sh --target pre --dry-run     # muestra acciones sin ejecutar
#
# Entornos remotos (--target pre|pro): rsync por SSH al Access Server del
# entorno, ruta /u01/repo/automation/iidr-ctl/.
#
# Credenciales — render de conf/iidr.properties (OBLIGATORIO salvo skip):
#   AS_PASSWORD_ENC=<hex ya cifrada con IBM Encryptor>  bin/deploy.sh --target pre
#   AS_PASSWORD='<claro>'                               bin/deploy.sh --target pre
#     (en este segundo caso, deploy.sh cifra con bin/iidr-encrypt.sh).
#   AS_RENDER_SKIP=1                                    bin/deploy.sh --target pre
#     Salta el render — sólo refresca scripts/jars; el destino mantiene su
#     iidr.properties actual. Útil para humanos que sólo quieren parchear
#     binarios. NO usar desde AAP (la falta de password debe fallar ruidoso).
#
# Contrato AAP: el job template debe inyectar AS_PASSWORD_ENC y
# TLS_TRUST_STORE_PASSWORD desde el vault. Si no llegan, el deploy falla.
#
# Perfil motor (host sin AS): este deploy empaqueta server.jar/chcclp.jar/api.jar
# del Access Server donde se ejecuta bajo iidr-cli-libs/lib/, para que un host
# motor pueda exportar IIDR_HOME=<deploy>/iidr-cli-libs y usar iidr.sh por NFS.
#   --target pre|pro exige que /u01/IIDR/AccessServer/lib/ esté presente
#   localmente (deploy.sh debe ejecutarse desde un host con AS instalado).
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

TARGET="dev"
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target) TARGET="$2"; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    *) echo "Argumento desconocido: $1" >&2; exit 1 ;;
  esac
done

# Por target: host remoto + datos para renderizar conf/iidr.properties.
# Datos verificados 2026-05-25 (PRE). El asPassword se rellena más abajo
# cifrando AS_PASSWORD o usando AS_PASSWORD_ENC tal cual.
case "${TARGET}" in
  dev)
    REMOTE_HOST=""
    DEPLOY_ROOT="${DEPLOY_ROOT:-/u01/repo/automation/iidr-ctl}"
    AS_HOSTNAME="${AS_HOSTNAME:-lesawsew131211.es.mapfre.net}"
    AS_PORT="${AS_PORT:-10101}"
    # DEV ya está integrado con LDAP (2026-09) → usuario con prefijo APP- (el
    # rol lo da el grupo LDAP). Override con AS_USERNAME si se necesita otro.
    AS_USERNAME="${AS_USERNAME:-APP-cdcauto}"
    AS_CERT_DIR="${AS_CERT_DIR:-/u01/repo/certificates/LESAWSEW131211}"
    ;;
  pre)
    if [[ -z "${REMOTE_USER:-}" ]]; then
      echo "ERROR: para --target pre define REMOTE_USER con tu uid de Linux." >&2
      echo "  Ejemplo: REMOTE_USER=tuuid bin/deploy.sh --target pre" >&2
      exit 2
    fi
    REMOTE_HOST="${REMOTE_USER}@LESAWSEW141142"
    DEPLOY_ROOT="/u01/repo/automation/iidr-ctl"
    AS_HOSTNAME="${AS_HOSTNAME:-LESAWSEW141142.es.mapfre.net}"
    AS_PORT="${AS_PORT:-10101}"
    # PRE ya está integrado con LDAP → usuario con prefijo APP- (el rol lo da
    # el grupo LDAP). Override con AS_USERNAME si se necesita otro.
    AS_USERNAME="${AS_USERNAME:-APP-cdcauto}"
    # AS_CERT_DIR solo se usa si hay que CONSTRUIR el truststore, y ese build es
    # LOCAL (en la máquina que ejecuta deploy.sh). Al desplegar PRE desde la 11,
    # esta ruta apunta al NFS de la 11, que NO tiene los certs de PRE. Por eso el
    # truststore de PRE/PRO lo crea iidr-install (propietario de los keystores):
    # una vez existe el .p12 en destino, deploy.sh no lo reconstruye y no mira
    # aquí. Este valor solo aplicaría si se ejecutara deploy.sh EN el host de PRE.
    AS_CERT_DIR="${AS_CERT_DIR:-/u01/repo/certificates/LESAWSEW141142}"
    ;;
  pro)
    if [[ -z "${REMOTE_USER:-}" ]]; then
      echo "ERROR: para --target pro define REMOTE_USER con tu uid de Linux." >&2
      echo "  Ejemplo: REMOTE_USER=tuuid bin/deploy.sh --target pro" >&2
      exit 2
    fi
    # AS de PRO AZ-A (Irlanda). Si el servicio está en AZ-B o DR, pasar
    # AS_HOSTNAME/AS_CERT_DIR por env para apuntar a la ubicación activa.
    REMOTE_HOST="${REMOTE_USER}@LESAWSEW161143"
    DEPLOY_ROOT="/u01/repo/automation/iidr-ctl"
    AS_HOSTNAME="${AS_HOSTNAME:-LESAWSEW161143.es.mapfre.net}"
    AS_PORT="${AS_PORT:-10101}"
    # PRO ya está integrado con LDAP → usuario con prefijo APP- (el rol lo da
    # el grupo LDAP). Override con AS_USERNAME si se necesita otro.
    AS_USERNAME="${AS_USERNAME:-APP-cdcauto}"
    # Ver nota en PRE: AS_CERT_DIR solo aplica al construir el truststore en
    # LOCAL. El truststore de PRO lo crea iidr-install en destino. Si algún día
    # se construyera EN el host de PRO, los certs allí están bajo el nombre
    # CANÓNICO del servicio: /u01/repo/certificates/iidr-gr-db2-acs-a01-aws-pro
    # (pasarlo por env AS_CERT_DIR en ese caso).
    AS_CERT_DIR="${AS_CERT_DIR:-/u01/repo/certificates/LESAWSEW161143}"
    ;;
  *)
    echo "Target desconocido: ${TARGET} (válidos: dev, pre, pro)" >&2; exit 1 ;;
esac

OWNER="cdcuser:cdcadmin"

log()  { echo "[deploy:${TARGET}] $*"; }
warn() { echo "[deploy:${TARGET}] WARN: $*" >&2; }
run()  { if $DRY_RUN; then echo "  [DRY] $*"; else "$@"; fi; }

# --- 1. Compilar -------------------------------------------------------------
log "Compilando con Maven..."
if $DRY_RUN; then
  log "[DRY] mvn -DskipTests package"
else
  (cd "${PROJECT_ROOT}" && mvn -q -DskipTests package)
  log "Compilación OK"
fi

JAR_SRC="${PROJECT_ROOT}/target/iidr-admin-1.0-SNAPSHOT.jar"
if [[ ! -f "${JAR_SRC}" && ! $DRY_RUN ]]; then
  echo "ERROR: JAR no encontrado tras compilación: ${JAR_SRC}" >&2
  exit 1
fi

# --- 2. Staging local --------------------------------------------------------
# Se construye en /tmp y luego se vuelca al destino (local o remoto via rsync).
STAGING="$(mktemp -d -t iidr-admin-deploy-XXXXXX)"
trap 'rm -rf "${STAGING}"' EXIT

log "Preparando staging en ${STAGING}..."
for dir in bin lib lib/deps conf log iidr-cli-libs/lib; do
  mkdir -p "${STAGING}/${dir}"
done

# JAR principal
cp -f "${JAR_SRC}" "${STAGING}/lib/iidr-admin.jar"

# Dependencias Maven
CP_FILE="${PROJECT_ROOT}/target/classpath.txt"
if [[ ! -f "${CP_FILE}" && ! $DRY_RUN ]]; then
  log "Regenerando classpath.txt..."
  (cd "${PROJECT_ROOT}" && mvn -q -DskipTests dependency:build-classpath \
    -Dmdep.outputFile="${CP_FILE}")
fi

if [[ -f "${CP_FILE}" ]]; then
  copied=0
  IFS=':' read -ra CP_ENTRIES <<< "$(tr -d '\n' < "${CP_FILE}")"
  for entry in "${CP_ENTRIES[@]}"; do
    [[ "${entry}" == *"server.jar"   ]] && continue
    [[ "${entry}" == *"chcclp.jar"   ]] && continue
    [[ "${entry}" == *"/junit/"*     ]] && continue
    [[ "${entry}" == *"/hamcrest/"*  ]] && continue
    if [[ -f "${entry}" ]]; then
      cp -f "${entry}" "${STAGING}/lib/deps/"
      ((copied++)) || true
    else
      warn "JAR no encontrado, omitiendo: ${entry}"
    fi
  done
  log "Dependencias copiadas a staging: ${copied} JARs"
fi

# Scripts
cp -f "${SCRIPT_DIR}/iidr-ctl.py"             "${STAGING}/bin/iidr-ctl.py"
cp -f "${SCRIPT_DIR}/iidr.sh"                 "${STAGING}/bin/iidr.sh"
cp -f "${SCRIPT_DIR}/iidr-encrypt.sh"         "${STAGING}/bin/iidr-encrypt.sh"
cp -f "${SCRIPT_DIR}/iidr-build-truststore.sh" "${STAGING}/bin/iidr-build-truststore.sh"
chmod 755 "${STAGING}/bin/iidr-ctl.py" \
          "${STAGING}/bin/iidr.sh" \
          "${STAGING}/bin/iidr-encrypt.sh" \
          "${STAGING}/bin/iidr-build-truststore.sh"

# Fuente de IIDRCrypt.java (la usa iidr-encrypt.sh — la compila on-the-fly)
mkdir -p "${STAGING}/lib"
cp -f "${PROJECT_ROOT}/lib/IIDRCrypt.java" "${STAGING}/lib/IIDRCrypt.java"

# Conf (sin credenciales)
cp -f "${PROJECT_ROOT}/conf/logback.xml" "${STAGING}/conf/logback.xml"

# Truststore: el .p12 no está en git (decisión 2026-05). Solo se construye
# si NO existe en el destino — así protegemos truststores ya creados por
# iidr-install (que es el propietario natural de los keystores en cada host).
# Necesita TLS_TRUST_STORE_PASSWORD y acceso al p7b en /u01/repo/certificates/.
ts_exists_in_destination() {
  local ts_rel="conf/cdc_truststore_AccessServer.p12"
  if [[ -z "${REMOTE_HOST}" ]]; then
    sudo test -f "${DEPLOY_ROOT}/${ts_rel}"
  else
    ssh "${REMOTE_HOST}" "sudo test -f ${DEPLOY_ROOT}/${ts_rel}"
  fi
}

build_truststore_if_needed() {
  local ts_out="${STAGING}/conf/cdc_truststore_AccessServer.p12"

  if $DRY_RUN; then
    log "[DRY] comprobando si el truststore existe en destino..."
    log "[DRY] (si no existiera, se construiría con iidr-build-truststore.sh)"
    return 0
  fi

  if ts_exists_in_destination; then
    log "Truststore ya existe en destino — NO se reconstruye"
    return 0
  fi

  log "Truststore no existe en destino — construyendo"
  if [[ -z "${TLS_TRUST_STORE_PASSWORD:-}" ]]; then
    echo "ERROR: el truststore no existe en ${DEPLOY_ROOT}/conf/ y" >&2
    echo "  TLS_TRUST_STORE_PASSWORD no está definida — no se puede construir." >&2
    echo "  Expórtala antes de relanzar el deploy." >&2
    exit 7
  fi

  local p7b="${AS_CERT_DIR}/certnew.p7b"
  if ! sudo test -r "${p7b}"; then
    echo "ERROR: no se puede leer ${p7b}." >&2
    echo "  Necesario para construir el truststore. Comprueba que existen" >&2
    echo "  los certs del AS de este entorno en ${AS_CERT_DIR}/." >&2
    exit 8
  fi

  # El p7b está bajo /u01/repo/certificates/ con permisos restringidos:
  # lo copiamos a /tmp con sudo y luego construimos como usuario normal.
  local p7b_tmp; p7b_tmp="$(mktemp -t certnew-XXXXXX.p7b)"
  sudo cat "${p7b}" > "${p7b_tmp}"
  # Encadenamos al trap del staging para no pisarlo (rm -rf "${STAGING}" sigue).
  trap 'rm -f "'"${p7b_tmp}"'"; rm -rf "'"${STAGING}"'"' EXIT

  TLS_TRUST_STORE_PASSWORD="${TLS_TRUST_STORE_PASSWORD}" \
    "${SCRIPT_DIR}/iidr-build-truststore.sh" \
    --ca-chain "${p7b_tmp}" \
    --out      "${ts_out}"
}
build_truststore_if_needed

# Plantilla iidr.properties (siempre se publica la plantilla)
cp -f "${PROJECT_ROOT}/conf/iidr.properties.template" \
      "${STAGING}/conf/iidr.properties.template"

# --- 2b. Render de conf/iidr.properties --------------------------------------
# Si AS_PASSWORD_ENC viene del entorno, se usa tal cual (caso AAP+vault).
# Si no, se exige AS_PASSWORD en claro y se cifra con bin/iidr-encrypt.sh.
# En dry-run o si no hay password disponible se omite (solo la plantilla sube).
render_iidr_properties() {
  local enc=""
  if [[ -n "${AS_PASSWORD_ENC:-}" ]]; then
    enc="${AS_PASSWORD_ENC}"
    log "Render iidr.properties: usando AS_PASSWORD_ENC (ya cifrada)"
  elif [[ -n "${AS_PASSWORD:-}" ]]; then
    log "Render iidr.properties: cifrando AS_PASSWORD con bin/iidr-encrypt.sh"
    if $DRY_RUN; then
      enc="<DRY_RUN_ENCRYPTED>"
    else
      enc="$("${SCRIPT_DIR}/iidr-encrypt.sh" encrypt "${AS_PASSWORD}")"
    fi
  elif [[ "${AS_RENDER_SKIP:-0}" == "1" ]]; then
    log "AS_RENDER_SKIP=1 — no se renderiza iidr.properties (sólo se publica la plantilla)"
    return 0
  else
    echo "ERROR: para target=${TARGET} hay que renderizar conf/iidr.properties." >&2
    echo "  Define una de estas variables antes de relanzar:" >&2
    echo "    AS_PASSWORD_ENC=<cadena hex cifrada>     (caso AAP+vault, recomendado)" >&2
    echo "    AS_PASSWORD='<pwd APP-cdcauto en claro>' (deploy.sh la cifra)" >&2
    echo "  O bien AS_RENDER_SKIP=1 si sólo quieres refrescar scripts/jars" >&2
    echo "  sin tocar el iidr.properties del destino." >&2
    exit 9
  fi

  local rendered="${STAGING}/conf/iidr.properties"
  # Sustituciones literales (sed) sobre la plantilla.
  sed \
    -e "s|^asHostName=.*|asHostName=${AS_HOSTNAME}|" \
    -e "s|^asPort=.*|asPort=${AS_PORT}|" \
    -e "s|^asUserName=.*|asUserName=${AS_USERNAME}|" \
    -e "s|<<AS_PASSWORD_ENC>>|${enc}|" \
    "${PROJECT_ROOT}/conf/iidr.properties.template" > "${rendered}"
  chmod 640 "${rendered}"
  log "iidr.properties renderizado en staging para target=${TARGET}"
}
render_iidr_properties

# --- 2c. Empaquetar JARs del AS bajo iidr-cli-libs/ (perfil motor) -----------
# Permite que un host motor (sin AS instalado) ejecute iidr.sh exportando
# IIDR_HOME=${DEPLOY_ROOT}/iidr-cli-libs. Los JARs se leen del AS local.
#
# Se copia TODO ${AS_LIB_SRC}/*.jar (~42 MB, 24 ficheros) en lugar de un
# subset. Razón: chcclp arrastra cadenas transitivas del framework Eclipse
# RCP + messaging de IIDR + drivers DB2 que el JVM resuelve dinámicamente.
# Filtrar mal causa NoClassDefFoundError en runtime (visto 2026-05-25 con
# subset {api,chcclp,server}.jar). El tamaño es irrelevante en NFS.
AS_LIB_SRC="/u01/IIDR/AccessServer/lib"
mkdir -p "${STAGING}/iidr-cli-libs/lib"
log "Empaquetando JARs del AS (${AS_LIB_SRC}/*.jar) en iidr-cli-libs/lib/..."
if [[ ! -d "${AS_LIB_SRC}" ]]; then
  if $DRY_RUN; then
    log "  [DRY] copiaría todos los .jar de ${AS_LIB_SRC}"
  else
    echo "ERROR: ${AS_LIB_SRC} no existe en este host." >&2
    echo "  deploy.sh para --target ${TARGET} debe ejecutarse desde un host con" >&2
    echo "  Access Server instalado en /u01/IIDR/AccessServer/." >&2
    exit 6
  fi
else
  shopt -s nullglob
  as_jars=( "${AS_LIB_SRC}"/*.jar )
  shopt -u nullglob
  if (( ${#as_jars[@]} == 0 )); then
    echo "ERROR: ${AS_LIB_SRC} no contiene ningún .jar" >&2
    exit 6
  fi
  cp -p "${as_jars[@]}" "${STAGING}/iidr-cli-libs/lib/"
  log "  ${#as_jars[@]} JARs del AS copiados a iidr-cli-libs/lib/"
fi

# --- 3. Volcado al destino ---------------------------------------------------
# Ficheros que NO se sobreescriben en destino:
#   conf/table_subscription_registry.csv (estado de producción)
# El iidr.properties SÍ se sobreescribe si lo hemos renderizado en este deploy;
# si no se ha renderizado, el destino conserva el suyo (lo excluimos en ese caso).

RSYNC_EXCLUDES=(
  --exclude='conf/table_subscription_registry.csv'
)
if [[ ! -f "${STAGING}/conf/iidr.properties" ]]; then
  RSYNC_EXCLUDES+=( --exclude='conf/iidr.properties' )
fi
if [[ ! -f "${STAGING}/conf/cdc_truststore_AccessServer.p12" ]]; then
  # Truststore ya estaba en destino y no se reconstruyó — no lo pisamos.
  RSYNC_EXCLUDES+=( --exclude='conf/cdc_truststore_AccessServer.p12' )
fi

if [[ -z "${REMOTE_HOST}" ]]; then
  log "Volcado local → ${DEPLOY_ROOT}"
  run sudo mkdir -p "${DEPLOY_ROOT}"
  run sudo rsync -a "${RSYNC_EXCLUDES[@]}" "${STAGING}/" "${DEPLOY_ROOT}/"

  # Registro CSV: copiar solo si no existe
  REGISTRY_DEST="${DEPLOY_ROOT}/conf/table_subscription_registry.csv"
  if [[ ! -f "${REGISTRY_DEST}" ]]; then
    log "Creando registro CSV (no existía en destino)"
    run sudo cp -f "${PROJECT_ROOT}/conf/table_subscription_registry.csv" "${REGISTRY_DEST}"
  else
    log "Registro CSV ya existe en destino — NO sobreescribiendo"
  fi

  run sudo chown -R "${OWNER}" "${DEPLOY_ROOT}"
  run sudo chmod -R g+rX "${DEPLOY_ROOT}"
  run sudo chmod 770 "${DEPLOY_ROOT}/log"
  run sudo chmod g+w "${DEPLOY_ROOT}/conf/table_subscription_registry.csv"
else
  log "Volcado remoto → ${REMOTE_HOST}:${DEPLOY_ROOT}"
  run ssh "${REMOTE_HOST}" "sudo mkdir -p ${DEPLOY_ROOT}"
  run rsync -a --rsync-path="sudo rsync" "${RSYNC_EXCLUDES[@]}" \
    "${STAGING}/" "${REMOTE_HOST}:${DEPLOY_ROOT}/"

  # Registro CSV: solo si no existe en destino remoto
  if run ssh "${REMOTE_HOST}" "[ ! -f ${DEPLOY_ROOT}/conf/table_subscription_registry.csv ]"; then
    log "Creando registro CSV remoto (no existía en destino)"
    run scp "${PROJECT_ROOT}/conf/table_subscription_registry.csv" \
      "${REMOTE_HOST}:/tmp/table_subscription_registry.csv"
    run ssh "${REMOTE_HOST}" "sudo mv /tmp/table_subscription_registry.csv ${DEPLOY_ROOT}/conf/"
  else
    log "Registro CSV ya existe en destino remoto — NO sobreescribiendo"
  fi

  run ssh "${REMOTE_HOST}" "sudo chown -R ${OWNER} ${DEPLOY_ROOT} && \
    sudo chmod -R g+rX ${DEPLOY_ROOT} && \
    sudo chmod 770 ${DEPLOY_ROOT}/log && \
    sudo chmod g+w ${DEPLOY_ROOT}/conf/table_subscription_registry.csv"
fi

# --- 4. Resumen --------------------------------------------------------------
log "---------------------------------------------------"
log "Despliegue completado: ${TARGET} → ${DEPLOY_ROOT}"
if ! $DRY_RUN; then
  if [[ -z "${REMOTE_HOST}" ]]; then
    find "${DEPLOY_ROOT}" -not -path "*/log/*" | sort | sed "s|${DEPLOY_ROOT}||"
  else
    ssh "${REMOTE_HOST}" "find ${DEPLOY_ROOT} -not -path '*/log/*' | sort | sed 's|${DEPLOY_ROOT}||'"
  fi
fi
log ""
log "Pasos siguientes:"
log "  1. Si no se han pasado credenciales en esta ejecución, configurar"
log "     ${DEPLOY_ROOT}/conf/iidr.properties (asPassword cifrada con"
log "     bin/iidr-encrypt.sh encrypt '<pwd>')."
log "  2. Verificar (host AS):    ${DEPLOY_ROOT}/bin/iidr-ctl.py datastore list"
log "  3. Verificar (host motor): IIDR_HOME=${DEPLOY_ROOT}/iidr-cli-libs \\"
log "                             JAVA_HOME=/opt/ibm-semeru-jdk-17 \\"
log "                             TLS_TRUST_STORE_PASSWORD=... \\"
log "                             ${DEPLOY_ROOT}/bin/iidr.sh datastore list"
