# execution_env/extra

Esta carpeta contiene archivos adicionales (*extra files*) que se copiarán directamente al sistema de archivos del Execution Environment durante su construcción.

## Contenido esperado

Cualquier archivo o directorio que deba estar disponible dentro del contenedor en tiempo de ejecución: scripts auxiliares, configuraciones, ficheros de datos, binarios externos, etc.

## Uso en el workflow `Create_EE.yaml`

Esta carpeta se activa cuando el parámetro **`enable_extra_files`** se establece a `true` en el disparo manual del workflow.

Adicionalmente, el parámetro **`extra_files_dest`** controla la ruta destino dentro del contenedor donde se colocarán los archivos (por defecto `/` — la raíz del sistema de archivos). Si la ruta indicada no termina en `/`, el workflow la normaliza automáticamente.

Cuando está habilitado, se añade la siguiente entrada al `execution-environment.yml` generado bajo la clave `additional_build_files`:

```yaml
additional_build_files:
  - src: execution_env/extra
    dest: <extra_files_dest>/
```

`ansible-builder` se encarga de copiar el contenido de esta carpeta a la ruta destino indicada durante la construcción de la imagen.

## Ejemplo de uso

Si se necesita incluir un script de configuración personalizado en `/usr/local/bin/` dentro del contenedor:

1. Colocar el script en `execution_env/extra/mi-script.sh`.
2. Disparar el workflow con `enable_extra_files: true` y `extra_files_dest: /usr/local/bin`.

> Los archivos se copian tal cual; si se requieren permisos especiales (p. ej. `chmod +x`), añadir un paso adicional en `append_final` del `execution-environment.yml` o gestionar los permisos directamente en el origen.
