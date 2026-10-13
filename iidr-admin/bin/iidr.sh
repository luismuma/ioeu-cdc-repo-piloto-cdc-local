#!/usr/bin/env bash
set -euo pipefail

# iidr-admin CLI entry point.
# Usage:  bin/iidr.sh <entity> <operation> [options]
# Examples:
#   bin/iidr.sh datastore list
#   bin/iidr.sh subscription create --name MYSUB --source DB2DS --target KAFKADS
#   bin/iidr.sh mapping find-by-table --table SCHEMA.CLIENTES
#   bin/iidr.sh composite setup --config conf/setup.json

# Ayuda rápida sin levantar JVM ni exigir env vars (TLS, properties, etc.).
# Útil para descubrir entidades/operaciones disponibles tras un primer aterrizaje.
if [[ $# -eq 0 || "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  cat <<'EOF'
Uso:  iidr.sh <entidad> <operación> [opciones]

Entidades y operaciones (ver detalles con: iidr.sh <entidad> <operación> --help):

  datastore list
  datastore show       --name <nombre>
  datastore create     --name <n> --host <h> --port <p> [--description <d>]
  datastore delete     --name <nombre>
  datastore grant      --name <n> --user <u>
                       (env: DB_USER_ENC, DB_PASSWORD_ENC)

  subscription list    --source <datastore>
  subscription show    --name <n> --source <ds>
  subscription create  --name <n> --source <ds> --target <dt> [--description <d>]
  subscription delete  --name <n> --source <ds>
  subscription start   --name <n> --source <ds>
  subscription stop    --name <n> --source <ds> [--method normal|immediate|abort]
  subscription lock    --name <n> --source <ds> --target <dt>
  subscription unlock  --name <n> --source <ds> --target <dt>
  subscription reinit  --name <n> --source <ds>

  mapping add          --name <sub> --source <ds> --table <SCHEMA.TABLA>
  mapping remove       --name <sub> --source <ds> --table <SCHEMA.TABLA>
  mapping list         --name <sub> --source <ds>
  mapping find-by-table --table <SCHEMA.TABLA>
  mapping refresh      --name <sub> --source <ds>

  composite setup        --config <fichero.json>
  composite teardown     --config <fichero.json>
  composite restart      --config <fichero.json>
  composite health-check

Variables de entorno:

  TLS_TRUST_STORE_PASSWORD   (obligatoria) password del truststore PKCS12.
  IIDR_HOME                  raíz del Access Server o de iidr-cli-libs/ (perfil motor).
                             Default: /u01/IIDR/AccessServer
  IIDR_JAVA                  override del binario java.
  JAVA_HOME                  fallback de IIDR_JAVA (perfil motor).
  IIDR_PROPERTIES_FILE       override de conf/iidr.properties (provisioning con APP-cdcadmin).
  DB_USER_ENC                (solo datastore grant) usuario dbUser cifrado con IBM Encryptor.
  DB_PASSWORD_ENC            (solo datastore grant) pwd dbPassword cifrada.

Más documentación: docs/DOCUMENTACION.md
EOF
  exit 0
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

IIDR_HOME="${IIDR_HOME:-/u01/IIDR/AccessServer}"
IIDR_LIB="${IIDR_HOME}/lib"

# Resolución de Java en este orden:
#   1. ${IIDR_JAVA} exportado y ejecutable  → override explícito (AAP, tests).
#   2. ${IIDR_HOME}/jre64/jre/bin/java      → perfil "host AS" (JRE del AS).
#   3. ${JAVA_HOME}/bin/java                → perfil "host motor" (JDK de iidr-install).
if [[ -n "${IIDR_JAVA:-}" && -x "${IIDR_JAVA}" ]]; then
  :  # ya resuelto via env
elif [[ -x "${IIDR_HOME}/jre64/jre/bin/java" ]]; then
  IIDR_JAVA="${IIDR_HOME}/jre64/jre/bin/java"
elif [[ -n "${JAVA_HOME:-}" && -x "${JAVA_HOME}/bin/java" ]]; then
  IIDR_JAVA="${JAVA_HOME}/bin/java"
else
  echo "ERROR: no se pudo resolver un Java ejecutable. Opciones:" >&2
  echo "  - exportar IIDR_JAVA con la ruta absoluta al binario java" >&2
  echo "  - tener \${IIDR_HOME}/jre64/jre/bin/java (perfil host AS)" >&2
  echo "  - exportar JAVA_HOME con un JDK válido (perfil host motor)" >&2
  exit 5
fi

CONF_DIR="${PROJECT_ROOT}/conf"
LOG_DIR="${PROJECT_ROOT}/log"

mkdir -p "${LOG_DIR}"

JAR_SERVER="${IIDR_LIB}/server.jar"
JAR_CHCCLP="${IIDR_LIB}/chcclp.jar"

# Localizar JAR principal: NFS (lib/iidr-admin.jar) o desarrollo (target/)
if [[ -f "${PROJECT_ROOT}/lib/iidr-admin.jar" ]]; then
  JAR_APP="${PROJECT_ROOT}/lib/iidr-admin.jar"
elif [[ -f "${PROJECT_ROOT}/target/iidr-admin-1.0-SNAPSHOT.jar" ]]; then
  JAR_APP="${PROJECT_ROOT}/target/iidr-admin-1.0-SNAPSHOT.jar"
else
  echo "ERROR: Build artifact not found under target/ or lib/." >&2
  echo "Build with: mvn -DskipTests package" >&2
  exit 2
fi

if [[ ! -f "${JAR_SERVER}" || ! -f "${JAR_CHCCLP}" ]]; then
  echo "ERROR: IIDR jars not found under: ${IIDR_LIB}" >&2
  echo "Opciones para IIDR_HOME:" >&2
  echo "  - host AS:    /u01/IIDR/AccessServer  (por defecto)" >&2
  echo "  - host motor: /u01/repo/automation/iidr-ctl/iidr-cli-libs  (NFS, sin AS)" >&2
  exit 3
fi

CP="${JAR_APP}:${JAR_SERVER}:${JAR_CHCCLP}"

# Classpath de dependencias: NFS (lib/deps/) o desarrollo (target/classpath.txt)
DEPS_DIR="${PROJECT_ROOT}/lib/deps"
MAVEN_CP_FILE="${PROJECT_ROOT}/target/classpath.txt"

if [[ -d "${DEPS_DIR}" ]]; then
  for dep in "${DEPS_DIR}"/*.jar; do
    [[ -f "${dep}" ]] && CP="${CP}:${dep}"
  done
else
  if [[ ! -f "${MAVEN_CP_FILE}" ]]; then
    (cd "${PROJECT_ROOT}" >/dev/null && mvn -q -DskipTests dependency:build-classpath -Dmdep.outputFile="${MAVEN_CP_FILE}")
  fi
  if [[ -f "${MAVEN_CP_FILE}" ]]; then
    CP="${CP}:$(tr -d '\n' < "${MAVEN_CP_FILE}")"
  fi
fi

# TLS truststore — la contraseña debe llegar por env var (no fallback)
TLS_TRUST_STORE="${TLS_TRUST_STORE:-${CONF_DIR}/cdc_truststore_AccessServer.p12}"
TLS_TRUST_STORE_TYPE="${TLS_TRUST_STORE_TYPE:-PKCS12}"
if [[ -z "${TLS_TRUST_STORE_PASSWORD:-}" ]]; then
  echo "ERROR: TLS_TRUST_STORE_PASSWORD no está definida. Expórtala antes de invocar este script." >&2
  exit 4
fi

# La librería IIDR NO usa las propiedades estándar javax.net.ssl.*; lee su
# propio fichero tls.properties desde el directorio de trabajo (CWD).
#
# Generamos un tls.properties de CLIENTE (solo sección truststore; la de
# keystore privado es para el AS como servidor) en un directorio TEMPORAL
# PROPIO DEL USUARIO, y más abajo hacemos `cd` a ese temporal para que la
# librería lo encuentre. NO se escribe en PROJECT_ROOT porque el deploy en
# NFS es de solo-lectura para usuarios que no sean su dueño (cdcuser); si
# escribiéramos ahí, iidr.sh fallaría para jpfrut2, el dashboard, AAP, etc.
# El resto de rutas (classpath, conf, truststore) son absolutas, así que
# cambiar el CWD a un temporal no afecta a nada más.
#
# Ruta absoluta al .p12 y pwd de la env var. Permisos 600 (pwd en claro).
# El temporal se borra al salir. Funciona igual en host AS y en host motor.
TLS_RUN_DIR="$(mktemp -d -t iidr-run-XXXXXX)"
trap 'rm -rf "${TLS_RUN_DIR}"' EXIT
umask 077
cat > "${TLS_RUN_DIR}/tls.properties" <<EOF
trustStorePath=${TLS_TRUST_STORE}
trustStorePassword=${TLS_TRUST_STORE_PASSWORD}
trustStoreType=${TLS_TRUST_STORE_TYPE}
EOF

# Properties file: por defecto conf/iidr.properties (usuario APP-cdcauto, día a día).
# Override con IIDR_PROPERTIES_FILE para ejecuciones puntuales con otro usuario —
# típicamente APP-cdcadmin para provisioning (datastore create / grant) desde
# iidr-install. El consumidor genera un .properties temporal con APP-cdcadmin y lo
# pasa por env var; iidr.sh lo propaga como -Diidr.properties al JVM, donde
# IIDRAdmin.main lo lee con la misma key.
# Ruta ABSOLUTA siempre: como abajo hacemos `cd` a un temporal, el default
# relativo "conf/iidr.properties" de IIDRAdmin ya no resolvería. Si el usuario
# no da IIDR_PROPERTIES_FILE, usamos el del deploy (${CONF_DIR}) por ruta abs.
if [[ -n "${IIDR_PROPERTIES_FILE:-}" ]]; then
  if [[ ! -r "${IIDR_PROPERTIES_FILE}" ]]; then
    echo "ERROR: IIDR_PROPERTIES_FILE='${IIDR_PROPERTIES_FILE}' no existe o no es legible." >&2
    exit 10
  fi
  IIDR_PROPERTIES_ARG=( "-Diidr.properties=${IIDR_PROPERTIES_FILE}" )
else
  IIDR_PROPERTIES_ARG=( "-Diidr.properties=${CONF_DIR}/iidr.properties" )
fi

# cd al directorio temporal: imprescindible para que la librería IIDR
# encuentre el tls.properties que acabamos de generar (lo busca en el CWD).
# El resto de rutas son absolutas, así que esto no afecta a nada más.
# No usamos `exec` para que el `trap EXIT` limpie el temporal al terminar.
cd "${TLS_RUN_DIR}"
"${IIDR_JAVA}" \
  -Dlogback.configurationFile="${CONF_DIR}/logback.xml" \
  -Dlogback.statusListenerClass=ch.qos.logback.core.status.NopStatusListener \
  "${IIDR_PROPERTIES_ARG[@]}" \
  -cp "${CP}" com.mapfre.cdc.admin.IIDRAdmin "$@"
exit $?
