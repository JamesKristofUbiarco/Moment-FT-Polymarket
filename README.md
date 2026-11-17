# Polymarket → MOMENT → LoRA

Este proyecto transforma el historial de órdenes de Polymarket en series de precios y actividad, y utiliza esas series para ajustar un modelo de inteligencia artificial llamado MOMENT. Busca señalar periodos de comportamiento inusual para que una persona pueda revisarlos.

**La primera ejecución integral terminó.** Se procesaron los 164 archivos originales, se entrenó el modelo y se compararon sus resultados con dos referencias. Una señal inusual requiere investigación y no demuestra fraude.

## Resultado de la ejecución

| Qué se completó | Resultado |
|---|---:|
| Archivos originales procesados y verificados | 164 de 164 |
| Tamaño original | 1.177.799.623.341 bytes, aproximadamente 1,18 TB |
| Eventos revisados | 217.647.146.849 |
| Activos identificados | 3.071.967 |
| Condiciones identificadas | 1.535.992 |
| Fragmentos de tiempo aptos para el modelo | 51.142.460 |
| Fragmentos utilizados para entrenar | 33.243.262 |
| Fragmentos evaluados en validación | 9.044.944 |
| Fragmentos evaluados en prueba final | 8.854.254 |
| Actualizaciones de los parámetros durante el entrenamiento | 129.857 |

Un **evento** es un registro del historial, como un cambio de precio. El **libro de órdenes** reúne las ofertas de compra y venta. Un **activo** identifica un token negociado. Una **condición** identifica la pregunta o resultado al que pertenecen tokens como YES y NO.

El trabajo acumuló **66 h 54 min dentro de sus intentos de ejecución** y **89 h 07 min de calendario**. Hubo interrupciones, recuperación y un apagado solicitado. El tiempo dentro de intentos puede incluir suspensión del equipo y no mide exclusivamente trabajo del procesador. El recorrido inicial de descarga y procesamiento tardó **38 h 56 min** y el entrenamiento **6 h 19 min**.

Los objetivos, las pruebas, los tiempos, los fallos y las propuestas de mejora están en el [informe de la ejecución integral](INFORME_PRUEBA_INTEGRAL_MVP.md).

## Qué aprendió el modelo

En esta ejecución el modelo trabajó con el **spread**, la diferencia entre el mejor precio de venta y el mejor precio de compra. Un spread amplio puede reflejar que resulta más costoso comprar y vender rápidamente.

MOMENT ya tenía aprendizaje previo sobre **series temporales**, números ordenados por tiempo. Se ajustó con nuestros datos mediante **LoRA**, una técnica que añade pequeños grupos de parámetros ajustables y conserva los pesos originales. Los parámetros son números internos que determinan cómo calcula sus respuestas.

Se ajustaron **118.792 parámetros**, incluidos los de la salida del modelo. Las predicciones siguen utilizando todos los pesos preentrenados de MOMENT Small.

La prueba final usó fechas posteriores al entrenamiento y a la validación. La **validación** permite comparar decisiones del modelo antes de hacer la **prueba final**, que mide su desempeño en otro periodo. Los tres métodos recibieron los mismos objetivos.

| Método | Error absoluto medio, MAE | Raíz del error cuadrático medio, RMSE |
|---|---:|---:|
| Repetir el último valor observado | **0,00878** | 0,04338 |
| MOMENT original | 0,01836 | 0,05844 |
| MOMENT ajustado con LoRA | 0,01157 | **0,04239** |

**MAE** mide cuánto se equivoca una predicción en promedio, sin importar si queda por arriba o por abajo. **RMSE** da más peso a los errores grandes. En ambas columnas, menos es mejor.

LoRA mejoró ambos errores frente a MOMENT original. Repetir el último valor consiguió el menor MAE y LoRA el menor RMSE. Se guardaron **19.055 episodios candidatos** en la prueba final, con identificadores, periodos y puntuaciones. Su utilidad práctica requiere revisión.

## Cómo recorre los datos

El **pipeline** es la secuencia de pasos que transforma los archivos originales hasta obtener el modelo y sus resultados.

1. **Comprobar el inventario.** Verifica lista, tamaños y versión exacta de los 164 archivos.
2. **Descargar y procesar varios archivos a la vez.** Cada archivo se descarga completo y después se lee por partes pequeñas, para controlar memoria y espacio temporal.
3. **Crear series por activo y segundo.** Calcula spread, precios medios, extremos, actualizaciones y calidad. A la vez guarda las relaciones entre activos y condiciones.
4. **Unir y verificar resultados parciales.** Combina registros de un mismo segundo y comprueba conteos antes de liberar temporales.
5. **Construir el catálogo.** Reúne las relaciones activo–condición ya extraídas, después de producir las series.
6. **Hacer el censo de ventanas.** Una ventana es un fragmento de 512 segundos. El censo registra cuáles tienen información suficiente, cuáles se excluyen y dónde están en disco.
7. **Seleccionar y preparar el entrenamiento.** Busca representar fechas, condiciones, fuentes y actividad. En esta ejecución cupieron todas las ventanas aptas del periodo de entrenamiento.
8. **Entrenar y guardar el modelo.** Ajusta LoRA y guarda puntos de recuperación, con lo necesario para continuar el entrenamiento.
9. **Evaluar y guardar episodios candidatos.** Compara los tres métodos y comprueba que las salidas estén completas.

Una ventana puede contener huecos. Una **máscara** marca qué posiciones tienen información utilizable y cuáles están ocultas. El objetivo fue un grupo de ocho observaciones completas dentro de la ventana. Los valores que debe predecir el modelo se ocultan en su entrada.

Se procesó el 100 % de los eventos. Aproximadamente el 3,04 % de los bloques de tiempo observados reunió los requisitos del modelo. El censo conserva el motivo de cada exclusión.

## Dónde empezar

La interfaz principal es el [notebook del MVP](mvp_polymarket_moment_lora.ipynb). Un **notebook** es un documento con explicaciones y bloques de código ejecutables. Permite configurar el entorno, revisar recursos, lanzar trabajos y consultar resultados.

Los trabajos largos se ejecutan separados del notebook. En Linux se utilizó **tmux**, una herramienta que mantiene una sesión de terminal abierta aunque se cierre su ventana. Después de apagar la computadora, la recuperación depende de los resultados guardados en disco.

El coordinador del recorrido completo está en [full.py](src/polymarket_mvp/full.py). Su función execute comprueba las condiciones de inicio y su función run_full encadena las etapas.

## Preparar el entorno local

Se necesita **uv**, el programa que instala las bibliotecas y prepara el entorno de Python del proyecto. Una biblioteca es código preparado por otros que utilizamos para tareas como leer archivos o entrenar modelos.

Estas órdenes parten de uv ya instalado. Se ejecutan en una terminal abierta en la carpeta del repositorio.

    uv sync --frozen --python 3.14
    uv run python -m unittest discover -s tests/mvp -v
    uv run jupyter lab mvp_polymarket_moment_lora.ipynb

La primera instala las versiones guardadas en uv.lock. La segunda ejecuta las pruebas automatizadas. La tercera abre JupyterLab para usar el notebook.

La corrida se completó en Linux con Ryzen 7 5700X, 32 GB de RAM y RTX 5070 Ti de 16 GB. La versión exacta del proyecto aún requiere una prueba completa en Colab. El uso de la tarjeta gráfica depende también del controlador NVIDIA instalado, que permite a Python comunicarse con ella.

| Herramienta observada | Para qué se utiliza |
|---|---|
| Python 3.14.9 y uv 0.11.25 | Ejecutar el proyecto y administrar sus bibliotecas |
| PyArrow 26.0.1 | Leer y escribir datos por partes |
| Polars 2.1.0 | Calcular y agrupar muchas filas |
| DuckDB 2.0.0 | Consultar, ordenar y reunir datos guardados en disco |
| PyTorch 2.15.0 y CUDA 13.2 | Calcular el modelo y entrenarlo en la tarjeta gráfica |
| momentfm 0.1.5 | Implementación de MOMENT |
| PEFT 0.21.2 y Transformers 5.22.2 | Incorporar LoRA y cargar el modelo |
| huggingface-hub 2.4.0 y httpx | Consultar la fuente y descargar archivos |
| psutil 7.2.2 | Consultar memoria, procesos y actividad del equipo |

pyproject.toml declara las bibliotecas necesarias. uv.lock registra sus versiones exactas, incluidas las que necesitan otras bibliotecas. No se utiliza pip directamente.

## Archivos de src/polymarket_mvp

Una **dependencia** significa que una pieza utiliza otra para realizar su trabajo.

| Archivo | Responsabilidad |
|---|---|
| [__init__.py](src/polymarket_mvp/__init__.py) | Guarda la fuente y versiones exactas del dataset y del modelo |
| [full.py](src/polymarket_mvp/full.py) | Coordina el recorrido completo, tiempos, presupuesto y recuperación |
| [parallel.py](src/polymarket_mvp/parallel.py) | Decide cuántos trabajos pueden ejecutarse a la vez según memoria y disco |
| [worker.py](src/polymarket_mvp/worker.py) | Ejecuta la lectura o unión de resultados de un archivo en un proceso separado |
| [download.py](src/polymarket_mvp/download.py) | Verifica el inventario, descarga y guarda reportes pequeños de forma segura |
| [engine.py](src/polymarket_mvp/engine.py) | Lee originales, aplica cálculos, confirma avances y reúne las series |
| [features.py](src/polymarket_mvp/features.py) | Define cálculos de precios, spread, actividad y calidad |
| [storage.py](src/polymarket_mvp/storage.py) | Registra resultados confirmados y comprueba su integridad |
| [columnar.py](src/polymarket_mvp/columnar.py) | Configura cómo guardar y comprimir tablas sin perder valores |
| [catalog.py](src/polymarket_mvp/catalog.py) | Reúne activos y condiciones y busca contradicciones |
| [windows.py](src/polymarket_mvp/windows.py) | Hace el censo, selecciona ejemplos y prepara entradas del modelo |
| [model.py](src/polymarket_mvp/model.py) | Carga MOMENT, incorpora LoRA, entrena, guarda, recarga y evalúa |
| [reporting.py](src/polymarket_mvp/reporting.py) | Consulta progreso y estima tiempo y espacio a partir de pruebas medidas |
| [runner.py](src/polymarket_mvp/runner.py) | Permite probar el procesamiento completo de un solo archivo diario |
| [io_audit.py](src/polymarket_mvp/io_audit.py) | Resume registros de lectura y transferencia de experimentos de desarrollo |
| [gates.py](src/polymarket_mvp/gates.py) | Conserva un control anterior de 48 horas; el FULL actual usa reporting.py y full.py |

windows.py también conserva una implementación anterior del censo para comparaciones. La función activa es la última definición de build_census. Esa duplicación y el control antiguo de gates.py quedan como limpieza pendiente del código.

## Dónde están los datos y resultados

**Parquet** es el formato de las tablas grandes. Guarda por columnas y permite leer partes concretas. **JSON** es el formato de reportes pequeños, legible como texto. **SQLite** es una base de datos guardada en un archivo, usada aquí para registrar avances confirmados.

| Ruta | Contenido |
|---|---|
| runs/mvp/inventory.json | Lista de originales y versión fijada |
| runs/mvp/inventory_enriched.json | Conteos y detalles internos de los originales |
| runs/mvp/staging/ | Descargas temporales |
| runs/mvp/full/ | Series y comprobaciones de cada original |
| runs/mvp/coverage.json | Cobertura y conteos reconciliados |
| runs/mvp/catalog/ | Relaciones entre activos y condiciones |
| runs/mvp/full_census/ | Censo, exclusiones y selección |
| runs/mvp/full_packs/train/ | Entradas numéricas preparadas para entrenamiento |
| runs/mvp/full_model/ | Modelo ajustado, historial y puntos de recuperación |
| runs/mvp/full_evaluation/ | Métricas, errores y episodios de validación y prueba |
| runs/mvp/mission.json | Historial de intentos, etapas y pausas |
| runs/mvp/full_status.json | Último estado registrado |
| runs/mvp/full_result.json | Resultado integral final |

Los datos grandes y el modelo final permanecen en la computadora local, dentro de runs/, excluido de Git. GitHub contiene código, informe y evidencias pequeñas. El adaptador en evidence/pilot corresponde al ensayo previo de un día. El adaptador integral está en runs/mvp/full_model/adapter/.

## Consultar el resultado existente

Esta orden lee el resumen final sin lanzar de nuevo la ejecución.

    uv run python -c "import json; d=json.load(open('runs/mvp/full_result.json')); print('Completo:', d['complete']); print(d['test'])"

El informe explica las interrupciones y correcciones. Hubo un límite de archivos abiertos y dos fallos por límites de memoria de DuckDB. Se resolvieron leyendo relaciones en grupos pequeños y ajustando memoria y cantidad de tareas simultáneas en las consultas afectadas.

## Reproducir o reanudar

El FULL requiere el inventario detallado, pruebas de archivos completos y un ensayo que incluya entrenamiento y evaluación. Esos preparativos permiten estimar recursos antes de empezar. Descargar el repositorio por sí solo no crea los datos locales ni esos reportes.

Para repetir el ensayo de un día con los preparativos disponibles:

    uv run python herramientas/preflight_full.py runs/mvp

Para calcular una proyección con los reportes disponibles:

    uv run python -c "from polymarket_mvp.reporting import projection; print(projection('runs/mvp', budget_hours=120))"

El comando utilizado para lanzar o reanudar la corrida completa fue:

    uv run python -u -m polymarket_mvp.full --root runs/mvp --projection runs/mvp/full_launch_projection.json --budget-hours 120

--root indica la carpeta de resultados. --projection señala el reporte del presupuesto calculado. --budget-hours fija las horas autorizadas. Para reanudar se conserva la carpeta y configuración. Una ampliación autorizada puede registrarse con --extra-hours H, donde H es la cantidad de horas adicionales.

Si ya existe el resultado integral final, el coordinador devuelve ese resultado. Cambiar características, fechas o modelo requiere una salida independiente y preparación compatible. No se deben mezclar resultados de distintas configuraciones.

## Versiones de datos y modelo

Una **revisión** identifica una versión exacta, para que una actualización de la fuente no cambie silenciosamente la prueba.

- Dataset Joseph3222/polymarket-orderbook, configuración orderbook. Revisión efe472f1f00a62f2cab1fed2851da439d9c1fe11.
- Modelo AutonLab/MOMENT-1-small. Revisión 411e288267f82cce86296dbe4d6c8bc533cc162f.
- Código momentfm fijado al commit 38f7310ad594100747ca2a8357e9c7ca7d323e0e. Un commit es una versión guardada en Git.

El entrenamiento usa datos anteriores al 21 de junio de 2026. La validación va del 21 de junio al 16 de julio y la prueba del 16 de julio al 11 de agosto. Los finales son exclusivos y las fechas usan UTC. Las ventanas que cruzan fronteras se excluyen.

## Limitaciones conocidas

- El modelo se evaluó en ventanas con suficientes observaciones, una fracción de los bloques observados.
- Repetir el último valor sigue siendo mejor en MAE. El informe propone comparaciones adicionales para entender cuándo aporta LoRA.
- La revisión humana de episodios, nombres legibles de mercado y utilidad empresarial siguen pendientes.
- La prueba exacta en Colab y la medición integral de consumo eléctrico no se realizaron.
- El último full_benchmark.json describe una reanudación corta. El tiempo real de procesamiento debe consultarse en mission.json, como explica el informe.
- Algunas celdas del notebook y herramientas conservan referencias a ensayos previos. Las métricas finales están en full_result.json y full_evaluation/.
