# execution_env/certs

Esta carpeta contiene los certificados de CA corporativos que se importarán en el Execution Environment durante su construcción, permitiendo que los procesos dentro del contenedor confíen en servicios internos con TLS.

## Contenido esperado

| Archivos | Descripción |
|---|---|
| `*.crt` / `*.pem` | Certificados de autoridades de certificación (CA) en formato PEM que deben ser de confianza dentro del contenedor. |

## Uso en el workflow `Create_EE.yaml`

Esta carpeta se activa cuando el parámetro **`import_certs`** se establece a `true` en el disparo manual del workflow.

Cuando está habilitado, el workflow añade al `execution-environment.yml` generado las siguientes instrucciones que `ansible-builder` ejecuta durante la fase `prepend_base`:

```dockerfile
COPY certs/* /etc/pki/ca-trust/source/anchors/
RUN update-ca-trust
```

Esto copia todos los archivos de esta carpeta al almacén de confianza del sistema operativo base del contenedor y actualiza el índice de CAs, de modo que herramientas como `curl`, `pip` o los módulos de Ansible puedan conectarse a endpoints HTTPS internos sin errores de certificado.

## Cuándo añadir certificados aquí

- El runner o el contenedor resultante necesita conectarse a registros privados, APIs internas o servicios con certificados emitidos por una CA corporativa no incluida en la imagen base.
- Se producen errores de tipo `SSL: CERTIFICATE_VERIFY_FAILED` al ejecutar playbooks contra infraestructura interna.
