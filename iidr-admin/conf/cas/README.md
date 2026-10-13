# CAs para el truststore de iidr-admin

Certificados de **Autoridades de Certificación (CA) públicos** que deben ir
en el truststore PKCS12 (`cdc_truststore_AccessServer.p12`) que usa la
librería IIDR / `iidr.sh` para validar TLS.

Son **solo certificados de CA (públicos)** — NO hay claves privadas aquí,
por eso es seguro versionarlos. `bin/iidr-build-truststore.sh` los mete en
el `.p12` al construirlo.

Origen: extraídos del truststore que ya funciona en producción de pruebas
(`cdc_truststore_KAFKA.p12` de la máquina 10, creado por el equipo IBM tras
identificar a mano qué CAs valida cada endpoint). Réplica versionada de ese
superset.

| Fichero | CA | Para qué |
|---|---|---|
| `mapfre-ca-raiz.pem` | MAPFRE CA RAIZ | TLS del **Access Server** (raíz Mapfre) |
| `mapfre-subca.pem` | MAPFRE.NET SUBCA | TLS del Access Server (intermedia Mapfre) |
| `digicert-assured-id-root-g2.pem` | DigiCert Assured ID Root G2 | **OAuth contra EntraID** (autenticación Kafka). NO es DigiCert Global Root G2 (esa es la del login.microsoftonline.com público); el endpoint EntraID de Mapfre cuelga de Assured ID Root G2. |
| `letsencrypt-isrg-root-x1.pem` | ISRG Root X1 | Endpoints con Let's Encrypt (prob. Schema Registry / broker Kafka) |
| `letsencrypt-r12.pem` | Let's Encrypt R12 | ídem (intermedia Let's Encrypt) |
| `microsoft-tls-rsa-root-g2.pem` | Microsoft TLS RSA Root G2 | Incluida en el superset original (posible residuo de pruebas) |
| `microsoft-tls-g2-rsa-ca-ocsp-10.pem` | Microsoft TLS G2 RSA CA OCSP 10 | ídem |

## Notas

- El JDK antiguo que usa la librería IIDR no trae algunas de estas raíces en
  su `cacerts`, y la librería IIDR usa su **propio** truststore (`.p12` vía
  `tls.properties`), no el del SO ni el del JDK. Por eso hay que incluirlas
  aquí explícitamente.
- Si un endpoint **rota** su certificado a otra cadena, habrá que actualizar
  la CA correspondiente aquí y reconstruir el `.p12`.
- Para descubrir qué CA valida un endpoint concreto:
  `openssl s_client -connect <host>:<port> -showcerts` y mirar la cadena.
