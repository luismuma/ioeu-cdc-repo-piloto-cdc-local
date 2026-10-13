# execution_env/requirements

Esta carpeta contiene las dependencias Python que se incluirán en el Execution Environment durante su construcción.

## Contenido esperado

| Archivo | Descripción |
|---|---|
| `requirements.txt` | Lista de paquetes Python en formato PEP 508 que se instalarán en la imagen final. |

## Uso en el workflow `Create_EE.yaml`

Esta carpeta se activa cuando el parámetro **`enable_requirements`** se establece a `true` en el disparo manual del workflow.

Cuando está habilitado, el workflow realiza dos acciones:

1. **Valida la sintaxis** de `requirements.txt` línea por línea usando `packaging.requirements.Requirement`. Las líneas vacías y comentarios (`#`) se ignoran.
2. **Incluye el archivo** en el `execution-environment.yml` generado bajo la clave `dependencies.python_v3`, de modo que `ansible-builder` instale los paquetes durante la construcción de la imagen.

## Formato del archivo `requirements.txt`

```
# Ejemplo de contenido válido
boto3>=1.26.0
requests~=2.31
cryptography
```

> Si el checkbox `enable_requirements` está activo pero el archivo `requirements.txt` no existe, el workflow fallará con un error explícito.
