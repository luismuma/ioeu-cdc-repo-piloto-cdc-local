#!/usr/bin/env bash
# =============================================================================
# iidr-encrypt.sh
#
# Encripta/desencripta passwords con el IBM Encryptor de IIDR (api.jar).
# Lo usa bin/deploy.sh al renderizar conf/iidr.properties desde la plantilla
# (cifra AS_PASSWORD si no se pasa AS_PASSWORD_ENC). También invocable a mano.
#
# Origen: copia de ioeu-cdc-ogg-install-deploy/iidr-install/bin/iidr-encrypt.sh
# (decisión 2026-05-25: iidr-admin autocontenido, sin dep cruzada al repo
# hermano). Mantener sincronizado si el original cambia.
#
# Uso:
#   bin/iidr-encrypt.sh encrypt <PASSWORD_EN_CLARO>
#   bin/iidr-encrypt.sh decrypt <PASSWORD_OFUSCADA_HEX>
#
# Necesita /u01/IIDR/AccessServer/lib/api.jar (o equivalente en otra ruta
# pasada con --api-jar).
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

# El JAR puede estar tanto en AccessServer como en cualquier agente
API_JAR="${API_JAR:-/u01/IIDR/AccessServer/lib/api.jar}"
JAVA_HOME_DEFAULT="/opt/ibm-semeru-jdk-17"
JAVA="${JAVA_HOME:-${JAVA_HOME_DEFAULT}}/bin/java"
JAVAC="${JAVA_HOME:-${JAVA_HOME_DEFAULT}}/bin/javac"

usage() {
    sed -n '2,/^# ===/p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

[[ $# -ge 1 ]] || usage
case "$1" in
    -h|--help) usage ;;
    --api-jar) API_JAR="$2"; shift 2 ;;
esac

OP="${1:-}"
VALUE="${2:-}"

[[ -n "${OP}" && -n "${VALUE}" ]] || { usage; }
[[ "${OP}" == "encrypt" || "${OP}" == "decrypt" ]] \
    || { echo "Operación inválida: ${OP}. Usa 'encrypt' o 'decrypt'." >&2; exit 2; }
[[ -f "${API_JAR}" ]] || { echo "No se encontró ${API_JAR}." >&2; exit 1; }
[[ -x "${JAVA}"   ]] || { echo "No se encontró java en ${JAVA}." >&2; exit 1; }

# Localizar/Compilar la clase IIDRCrypt.
# El .class lo cacheamos en /tmp del usuario actual (no en el NFS compartido)
# porque cada servidor puede tener un api.jar distinto.
SRC="${PROJECT_ROOT}/lib/IIDRCrypt.java"
CLS_DIR="/tmp/iidr-encrypt-${USER:-$(whoami)}"
CLS="${CLS_DIR}/IIDRCrypt.class"
mkdir -p "${CLS_DIR}"

if [[ ! -f "${CLS}" || "${SRC}" -nt "${CLS}" ]]; then
    [[ -x "${JAVAC}" ]] || { echo "Necesito javac en ${JAVAC} para compilar IIDRCrypt." >&2; exit 1; }
    "${JAVAC}" -cp "${API_JAR}" -d "${CLS_DIR}" "${SRC}" >&2
fi

exec "${JAVA}" -cp "${API_JAR}:${CLS_DIR}" IIDRCrypt "${OP}" "${VALUE}"
