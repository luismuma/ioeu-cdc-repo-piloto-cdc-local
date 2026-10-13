#!/usr/bin/env bash
# =============================================================================
# iidr-build-truststore.sh
#
# Construye el truststore PKCS12 que usa iidr-admin como cliente TLS del
# Access Server. Contiene únicamente las CAs corporativas de Mapfre,
# extraídas del p7b que entrega la PKI con los certs del host AS.
#
# Es una versión adelgazada de iidr-install/bin/iidr-build-keystores.sh
# (en el repo hermano ioeu-cdc-ogg-install-deploy): solo la rama del
# truststore, sin la del keystore (iidr-admin es cliente, no servidor
# TLS, así que no necesita cert propio). Se duplica intencionadamente
# para que iidr-admin sea autocontenido — misma decisión que con
# iidr-encrypt.sh (2026-05-25).
#
# Uso:
#   TLS_TRUST_STORE_PASSWORD='...' \
#   bin/iidr-build-truststore.sh \
#     --ca-chain /u01/repo/certificates/LESAWSEW141142/certnew.p7b \
#     --out      /tmp/staging/conf/cdc_truststore_AccessServer.p12
#
# Lo invoca bin/deploy.sh cuando el destino aún no tiene truststore;
# también es invocable a mano para reconstruir.
# =============================================================================
set -euo pipefail

JAVA_HOME_DEFAULT="/opt/ibm-semeru-jdk-17"
KEYTOOL_BIN="${JAVA_HOME:-${JAVA_HOME_DEFAULT}}/bin/keytool"

log() { echo "[build-truststore] $*" >&2; }
err() { echo "[build-truststore] ERROR: $*" >&2; exit 1; }

CA_CHAIN=""
OUT=""
PASSWORD="${TLS_TRUST_STORE_PASSWORD:-}"
FORCE=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ca-chain) CA_CHAIN="$2"; shift 2 ;;
    --out)      OUT="$2"; shift 2 ;;
    --password) PASSWORD="$2"; shift 2 ;;
    --force)    FORCE=true; shift ;;
    -h|--help)
      sed -n '2,/^# ===/p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) err "Argumento desconocido: $1" ;;
  esac
done

[[ -n "${CA_CHAIN}" ]] || err "--ca-chain es obligatorio"
[[ -n "${OUT}"      ]] || err "--out es obligatorio"
[[ -n "${PASSWORD}" ]] || err "Falta password (--password o env TLS_TRUST_STORE_PASSWORD)"
[[ -r "${CA_CHAIN}" ]] || err "No se puede leer --ca-chain: ${CA_CHAIN} (¿permisos de /u01/repo/certificates/?)"

if [[ -f "${OUT}" ]] && ! ${FORCE}; then
  log "${OUT} ya existe — usar --force para sobreescribir."
  exit 0
fi

TMP_DIR="$(mktemp -d -t iidr-truststore-XXXXXX)"
trap 'rm -rf "${TMP_DIR}"' EXIT

CA_PEM="${TMP_DIR}/ca-chain.pem"

# Extraer las CAs del p7b y limpiar metadatos de openssl pkcs7 -print_certs.
log "Extrayendo CAs del p7b: ${CA_CHAIN}"
openssl pkcs7 -in "${CA_CHAIN}" -print_certs -out "${CA_PEM}"
awk '
  /-----BEGIN CERTIFICATE-----/ { keep=1 }
  keep { print }
  /-----END CERTIFICATE-----/   { keep=0; print "" }
' "${CA_PEM}" > "${CA_PEM}.clean"
mv "${CA_PEM}.clean" "${CA_PEM}"

mkdir -p "$(dirname -- "${OUT}")"

# Estrategia idéntica al script original: keytool (con aliases por CA, si está
# disponible) o fallback a openssl pkcs12 -export -nokeys (bundle sin aliases).
if [[ -x "${KEYTOOL_BIN}" ]]; then
  log "Usando ${KEYTOOL_BIN} (aliases por CA)"
  CA_SPLIT_DIR="${TMP_DIR}/cas"
  mkdir -p "${CA_SPLIT_DIR}"
  csplit -s -z -f "${CA_SPLIT_DIR}/ca_" -b "%02d.pem" \
    "${CA_PEM}" '/-----BEGIN CERTIFICATE-----/' '{*}' || true

  rm -f "${OUT}"
  for ca_file in "${CA_SPLIT_DIR}"/ca_*.pem; do
    [[ -s "${ca_file}" ]] || continue
    cn="$(openssl x509 -in "${ca_file}" -noout -subject \
          | sed -E 's/.*CN[[:space:]]*=[[:space:]]*([^,]+).*/\1/' \
          | tr '[:upper:] ' '[:lower:]_')"
    ts_alias="${cn//mapfre.net_subca/mapfre_subca}"
    ts_alias="${ts_alias//mapfre_ca_raiz/mapfre_ca}"
    log "  Añadiendo ${ca_file} con alias '${ts_alias}'"
    "${KEYTOOL_BIN}" -importcert -noprompt \
      -trustcacerts \
      -alias "${ts_alias}" \
      -file "${ca_file}" \
      -keystore "${OUT}" \
      -storetype PKCS12 \
      -storepass "${PASSWORD}" >/dev/null
  done
else
  log "${KEYTOOL_BIN} no disponible — fallback a openssl (bundle sin aliases por CA)"
  openssl pkcs12 -export -nokeys \
    -in "${CA_PEM}" \
    -out "${OUT}" \
    -passout pass:"${PASSWORD}" \
    -name mapfre_cas
fi

chmod 640 "${OUT}"

log "Verificando truststore..."
openssl pkcs12 -info -in "${OUT}" -nodes \
  -passin pass:"${PASSWORD}" >/dev/null 2>&1 \
  || err "El truststore generado no se abre con la password proporcionada"

log "OK: ${OUT}"
