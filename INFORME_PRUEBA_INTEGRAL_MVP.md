# Informe de la prueba integral Polymarket → MOMENT → LoRA

Fecha del informe: 14 de noviembre de 2026. Las fechas de ejecución usan America/Mexico_City, UTC−06. Los cortes del dataset usan UTC.

## 1. Resultado general

La corrida publicó el resultado integral con estado complete. Procesó y verificó los **164 archivos originales**, con **1.177.799.623.341 bytes** y **217.647.146.849 eventos**. Completó la generación de series, catálogo, censo, entrenamiento LoRA y evaluación cronológica contra dos referencias.

El fine-tuning recorrió **33.243.262 ventanas** y realizó **129.857 actualizaciones del optimizador**. La evaluación usó todas las ventanas elegibles de validación y prueba. Los adaptadores se exportaron y recargaron, con diferencia máxima de parámetros igual a cero.

MOMENT + LoRA mejoró tanto MAE como RMSE respecto a MOMENT original. Frente a persistencia, obtuvo menor RMSE y mayor MAE de prueba. La utilidad empresarial de los episodios candidatos requiere revisión histórica adicional.

Fuentes: [resultado integral](runs/mvp/full_result.json), [cobertura](runs/mvp/coverage.json), [misión e intentos](runs/mvp/mission.json).

## 2. Objetivos

1. Procesar el 100 % de un dataset original superior a 1 TB.
2. Eliminar el mapeo global previo y extraer relaciones y características durante la misma lectura.
3. Mantener memoria, staging y disco controlados, con recuperación y confirmaciones verificables.
4. Usar el modelo preentrenado obligatorio MOMENT-1-small y realizar fine-tuning mediante LoRA.
5. Mantener separación temporal estricta y registrar ventanas elegibles, entrenadas y excluidas.
6. Comparar contra persistencia y MOMENT original.
7. Priorizar episodios históricos de anomalías de liquidez y microestructura.
8. Entregar notebook, módulos, dependencias con uv, manifiestos, adaptadores y resultados.

El objetivo inicial de 48 horas se flexibilizó con autorización. Se adoptaron 72 horas orientativas para procesamiento y 120 horas como presupuesto operativo integral. La cobertura original nunca se redujo por el presupuesto.

El modelo final de esta prueba utiliza spread como canal y objetivo. Los derivados conservan más características para experimentos posteriores. Una anomalía estadística no acredita manipulación ni fraude.

## 3. Datos y cobertura

| Concepto | Resultado |
|---|---:|
| Dataset | Joseph3222/polymarket-orderbook, configuración orderbook |
| Revisión fijada | efe472f1f00a62f2cab1fed2851da439d9c1fe11 |
| Originales confirmados | 164 / 164 |
| Bytes originales | 1.177.799.623.341 |
| Eventos declarados y reconciliados | 217.647.146.849 |
| Grupos Parquet contabilizados | 2.171.252 |
| Unidades de confirmación | 135.777 |
| Filas sin clave registradas en cobertura | 0 |
| Filas de series consolidadas | 61.115.946.937 |
| Bytes de series consolidadas | 765.435.141.118 |
| Activos del catálogo | 3.071.967 |
| Condiciones del catálogo | 1.535.992 |
| Contradicciones de condición registradas | 0 |

La revisión y los bytes se verificaron contra el inventario fijado. El contador histórico de 168 elementos no se adoptó como inventario. No existe evidencia suficiente para atribuir con certeza el origen de esos cuatro elementos adicionales del pipeline anterior.

### Calidad y tipos de eventos

Los conteos siguientes se recalcularon desde los metadatos de confirmación de los 164 manifiestos SQLite. Cada tabla suma el total de eventos.

| Estado de calidad | Eventos |
|---|---:|
| bilateral | 204.876.462.558 |
| boundary | 12.526.343.548 |
| empty_ask | 19.443.663 |
| empty_bid | 19.430.899 |
| empty_both | 3.229.487 |
| unknown | 201.582.012 |
| invalid_quote | 654.682 |

Los límites de precio, lados vacíos, desconocidos e inválidos tienen estados diferenciados. Cero filas sin clave no implica cero cotizaciones inválidas ni cotización bilateral ejecutable en todos los eventos.

| Tipo de evento | Eventos |
|---|---:|
| book | 637.377.485 |
| price_change | 216.808.643.197 |
| last_trade_price | 199.345.532 |
| tick_size_change | 1.780.635 |

Fuentes: [inventario enriquecido](runs/mvp/inventory_enriched.json), [catálogo](runs/mvp/catalog/catalog.json) y manifiestos de cada partición en runs/mvp/full.

## 4. Recorrido ejecutado

### Descarga, extracción y consolidación

Cada original se descargó completo a staging acotado y se verificó con SHA256. La lectura por lotes produjo simultáneamente relaciones activo–condición y características temporales. Se conservaron spread inicial/final, extremos, mid-price, actividad, estados de calidad y procedencia. El tamaño agregado de operaciones representa tamaño bruto reportado.

Los parciales se fusionaron por activo, condición y segundo. El orden reproducible utiliza timestamp, ordinal de archivo y posición original. Los desempates no acreditan causalidad cuando los timestamps son ambiguos. La reconstrucción perfecta de L2 no fue una dependencia del recorrido.

Las series se almacenaron en Parquet. SQLite se utilizó para manifiestos transaccionales. Los objetos se publican y sincronizan antes de confirmar la unidad. La compactación verifica sustitutos antes de retirar parciales. El catálogo global se construyó después desde relaciones derivadas.

Se ejecutaron hasta cuatro trabajos de archivo y dos consolidadores, con reservas globales de RAM y disco. Los archivos temporales originales se liberaron después de confirmar sus derivados. Las reanudaciones reutilizaron unidades confirmadas y verificaron sus hashes.

### Censo y ventanas

El censo recorrió las series locales y enumeró bloques alineados de 512 segundos por activo–condición. Los bloques sin eventos no se enumeran. Se exigieron al menos dos patches completos de ocho observaciones. El objetivo fue el último patch completo del bloque.

Una ventana elegible puede ser dispersa. La longitud nominal de 512 segundos no exige 512 observaciones válidas. El horizonte del objetivo dentro del bloque varía según dónde esté el último patch completo. La elegibilidad favorece periodos de actividad suficiente.

### Entrenamiento

La entrada de MOMENT tuvo dimensiones [B, 1, 512] y el objetivo ocho valores de spread. Los valores del objetivo y posteriores se ocultaron a la entrada visible. Las máscaras distinguen observaciones y posiciones ocultas. La normalización utiliza el contexto compatible con MOMENT.

LoRA se aplicó a q y v con rango 8, alpha 16 y dropout 0, más cabeza entrenable. Se usaron BF16, AdamW, learning rate 0,0001, weight decay 0,01, clipping de gradiente 1 y semilla 2026. El resto de pesos quedó congelado.

Checkpoint: AutonLab/MOMENT-1-small, revisión 411e288267f82cce86296dbe4d6c8bc533cc162f. El código momentfm está fijado al commit 38f7310ad594100747ca2a8357e9c7ca7d323e0e.

## 5. Hardware y entorno

- Ryzen 7 5700X, 8 núcleos y 16 hilos, con 32 GB de RAM.
- RTX 5070 Ti de 16 GB, usada para entrenamiento y evaluación.
- Python 3.14.9 y entorno administrado con uv.
- PyTorch 2.15.0, CUDA 13.2, PyArrow 26.0.1, Polars 2.1.0, DuckDB 2.0.0, PEFT 0.21.2 y Transformers 5.22.2 según el reporte local.
- Dependencias fijadas mediante pyproject.toml y uv.lock.

La corrida se completó localmente. La implementación exacta conserva pendiente su validación en Colab. El consumo eléctrico y las CPU-horas/GPU-horas integrales no tienen una medición global validada.

Fuente: [reporte del entorno](runs/mvp/environment.json).

## 6. Tiempos y presupuesto

Inicio FULL: 10 de noviembre de 2026, 17:44:23 UTC−06. Finalización: 14 de noviembre de 2026, 10:51:19 UTC−06.

| Medición | Duración |
|---|---:|
| Primer recorrido FULL de descarga, ingestión y consolidación | 38 h 56 min 27 s |
| Censo completo, contador interno | 16 h 57 min 19 s |
| Primera selección completa | 9 min 19 s |
| Empaquetado de entrenamiento completado | 32 min 37 s |
| Entrenamiento, contador interno | 6 h 18 min 56 s |
| Etapa de entrenamiento, incluida carga y verificación | 6 h 20 min 22 s |
| Empaquetado validación | 10 min 03 s |
| Evaluación validación, contador interno | 1 h 01 min 28 s |
| Empaquetado prueba | 9 min 09 s |
| Evaluación prueba, contador interno | 58 min 33 s |
| Verificación de cobertura acumulada en reanudaciones | 1 h 09 min 36 s |
| Intentos de catálogo acumulados | 11 min 23 s |
| Total acumulado dentro de intentos | 66 h 53 min 42 s |
| Intervalos entre intentos | 22 h 13 min 14 s |
| Total de calendario | 89 h 06 min 57 s |

Las filas incluyen mediciones contenidas en otras filas y no deben sumarse directamente. Los intentos incluyen fallos y revalidaciones. Los intervalos entre intentos incluyen espera, recuperación y apagado solicitado. No se midió por separado toda suspensión del equipo dentro de un intento. Por ello, el tiempo dentro de intentos tampoco equivale a horas efectivas de CPU.

La proyección congelada al lanzamiento fue de unas 103,5 horas. La corrida se mantuvo dentro del presupuesto autorizado de 120 horas. El censo necesitó casi 17 horas frente a unas 12,1 horas proyectadas. El trabajo anterior al arranque corresponde a desarrollo y benchmarks, cuyas evidencias individuales se conservan. No hay una contabilidad global fiable de todas las horas de desarrollo.

### Red, fases concurrentes y limitación de instrumentación

Los recibos por original suman 1.177.799.623.341 bytes de payload, unas 5,99 horas-trabajador de red y 6,68 horas-trabajador de descarga/verificación. Incluyen recibos de originales ya descargados antes de FULL. Las transferencias fallidas y descargas de desarrollo tienen sus propios logs. Esas sumas no acreditan el tráfico total de la misión ni incluyen cabeceras/TLS.

Los reportes por original suman 52,61 horas-trabajador de ingestión y 59,66 de consolidación. Las fases se solaparon y algunos recibos se reutilizaron. El tiempo real de pared del primer recorrido proviene de mission.json.

**Defecto de instrumentación pendiente:** full_benchmark.json se sobrescribió al revalidar las 164 particiones. Su último wall_seconds, aproximadamente 240 s, mide esa reanudación. El observed_overlap_factor cercano a 1682 mezcla tiempos históricos con una reanudación corta y no tiene interpretación de rendimiento. Para el tiempo integral se utiliza el primer intento de mission.json. Una siguiente versión debe conservar reportes inmutables por intento y separar trabajo nuevo de reutilizado.

Fuente: [misión](runs/mvp/mission.json), [reporte sobrescrito de reanudación](runs/mvp/full_benchmark.json), recibos de cada partición.

## 7. Benchmarks y pruebas previas

### Cuatro originales completos

El benchmark concurrente procesó 5.054.034.321 eventos y 27.448.623.212 bytes en **1 h 32 min 19 s**, con originales ya descargados. La suma de fases fue de 10.909,54 s y el solapamiento observado de 1,97. No constituye una aceleración medida contra un único trabajador.

La tabla detallada por archivo, con decodificación, transformación, relaciones, publicación, reparticionado, agregación, RSS y escrituras, se encuentra en la [auditoría previa](evidence/benchmark/full_launch_audit.md). Los datos originales están en [benchmark_audit.json](runs/mvp/benchmark_audit.json).

La consolidación fue la fase más larga en tres de los cuatro archivos. Las relaciones están incluidas en transformación y la agregación incluye escritura de salida. La verificación histórica no tuvo un cronómetro aislado. La amplificación de escritura SO fue de aproximadamente 5,70×, 4,13×, 4,79× y 4,13× para febrero 26, abril 26, junio 6 y junio 9 respectivamente. Es volumen escrito dividido por original, no pico de ocupación temporal.

| Fecha | Eventos | Original GB | Ingesta s | Lectura/decodificación s | Transformación s | Relaciones s | Publicación s | Reparticionado s | Agregación/escritura s | RSS máx. proceso GB | Final GB | Escrituras SO GB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-02-26 | 550.440.717 | 3.152 | 678.65 | 56.04 | 386.75 | 36.95 | 231.30 | 279.26 | 557.83 | 3.057 | 2.575 | 17.959 |
| 2026-04-26 | 1.939.840.389 | 9.639 | 1784.12 | 176.80 | 1031.79 | 114.92 | 562.88 | 850.47 | 980.95 | 3.221 | 5.992 | 39.833 |
| 2026-06-06 | 93.595.651 | 0.511 | 124.27 | 10.75 | 76.26 | 6.65 | 36.50 | 33.45 | 88.87 | 2.264 | 0.441 | 2.446 |
| 2026-06-09 | 2.470.157.564 | 14.147 | 2795.68 | 231.50 | 1734.42 | 143.24 | 813.90 | 1478.91 | 1249.41 | 5.002 | 8.887 | 58.389 |

### Cambios medidos y comprobaciones

- La lectura remota selectiva del 6 de junio transfirió 493.232.473 bytes frente a 510.963.243 del original completo. Ahorró aproximadamente 3,5 % y tardó 634,42 s hasta consolidación. Se eligió descarga completa y procesamiento local.
- El kernel con lectura por diccionarios tardó 51,29 s frente a 65,00 s, aproximadamente 21 % menos, sobre los 93.595.651 eventos. Se alternó el orden y se compararon campos y contadores.
- El particionado Arrow sustituyó variantes COPY que generaban excesivos temporales. La consolidación optimizada agrupa claves duplicadas y deja pasar las únicas.
- El ensayo end-to-end nuevo del 6 de junio registró 67,05 s de ingestión y 53,42 s de consolidación. El benchmark concurrente registró 124,27 y 122,54 s. La concurrencia y carga distintas impiden atribuir todo el cambio a una optimización aislada.
- La comparación semántica posterior recorrió **28.175.552 filas y todas las columnas**, sin diferencias. La diferencia máxima del tamaño bruto reportado también fue cero.
- Al reabrir el ensayo confirmado hubo 934 grupos decodificados antes y después. No se añadieron lecturas de originales.
- Se comprobaron máscaras y objetivos de 9.002 ventanas del piloto, gradientes, actualizaciones de LoRA y cabeza, pesos congelados, checkpoints y recarga.
- Las pruebas de publicación simularon interrupción entre Parquet y confirmación SQLite, reintento idempotente y corrupción. Las pruebas de ventanas verificaron cortes temporales y mínimo de selección.
- El ensayo end-to-end se ejecutó en un entorno nuevo creado mediante uv. El registro automatizado del MVP confirmó cinco pruebas aprobadas.

Fuentes: [ensayo](runs/mvp/preflight_full/result.json), [equivalencia](runs/mvp/preflight_full/partition/equivalence.json), [reinicio y máscaras](runs/mvp/preflight_full/recovery_and_mask.json), [pruebas](runs/mvp/release_tests.log).

## 8. Censo, exclusiones y selección

| Concepto | Ventanas |
|---|---:|
| Universo de bloques observados | 1.682.526.815 |
| Elegibles | 51.142.460 |
| Excluidas por patches completos insuficientes | 1.631.233.533 |
| Excluidas por frontera temporal o fuera de rango | 150.822 |
| Entrenamiento elegible y entrenado | 33.243.262 |
| Validación elegible y evaluada | 9.044.944 |
| Prueba elegible y evaluada | 8.854.254 |
| Mínimo de cobertura por activos y estratos | 2.656.195 |

Solo **3,04 %** de los bloques observados fue elegible. La exclusión responde principalmente a sparsidad y al requisito de patches completos. Todos los eventos se procesaron, pero muchos bloques no cumplen las condiciones del objetivo.

Los intervalos UTC usan inicio inclusivo y final exclusivo.

| Conjunto | Inicio | Final exclusivo |
|---|---|---|
| Entrenamiento | 2026-02-22 | 2026-06-21 |
| Validación | 2026-06-21 | 2026-07-16 |
| Prueba | 2026-07-16 | 2026-08-11 |

La selección reproducible utiliza fecha, condición, fuente, cuartil de actividad y cobertura por activo. Los bordes de actividad fueron 523, 900 y 1.691 eventos por ventana. El presupuesto real permitió incluir todas las ventanas elegibles de entrenamiento, por lo que no hubo exclusiones finales por presupuesto.

Se completó una pasada por la selección congelada que, en esta corrida, coincide con el conjunto elegible de entrenamiento. Eso no equivale a entrenar cada evento original ni los bloques excluidos.

| Fuente | Cuartil | Ventanas entrenadas |
|---|---:|---:|
| v1 | 0 | 1.224.908 |
| v1 | 1 | 1.214.538 |
| v1 | 2 | 1.193.442 |
| v1 | 3 | 1.121.603 |
| v2 | 0 | 7.090.109 |
| v2 | 1 | 7.096.902 |
| v2 | 2 | 7.115.682 |
| v2 | 3 | 7.186.078 |

La distribución por fecha y condición se conserva en selection_distribution.parquet. Los activos pueden aparecer en distintos conjuntos en fechas distintas. Esta evaluación no mide exclusivamente mercados nuevos.

Fuentes: [censo](runs/mvp/full_census/census.json), [presupuesto mínimo](runs/mvp/full_census/selection_budget.json), [selección](runs/mvp/full_census/selection.json).

## 9. Resultados del modelo

| Parámetro o resultado | Valor |
|---|---:|
| Parámetros entrenables | 118.792 |
| Microbatch / batch efectivo | 256 / 256 |
| Ventanas entrenadas | 33.243.262 |
| Actualizaciones | 129.857 |
| Throughput medido de pasos | 1.466,67 ventanas/s |
| Pico CUDA asignado por PyTorch | 1,155 GB |
| Gradientes verificados | Sí |
| Adaptadores y cabeza actualizados | Sí |
| Tensores comprobados en recarga | 34 |
| Diferencia máxima en recarga | 0 |

El throughput corresponde al contador de pasos tras el calentamiento implementado. El pico de memoria es lo asignado por PyTorch, no toda la reserva CUDA o el consumo mostrado por nvidia-smi. La MSE media de los primeros 1.000 pasos fue aproximadamente 0,001665 y la de los últimos 1.000, 0,001349. La evaluación temporal determina la generalización.

### Errores globales

Los errores se calculan sobre los ocho valores objetivo y se promedian por ventana. Están expresados en unidades de spread.

| Conjunto | Modelo | MAE | RMSE |
|---|---|---:|---:|
| Validación | Persistencia | 0,00829984 | 0,04659414 |
| Validación | MOMENT original | 0,01639049 | 0,05647582 |
| Validación | MOMENT + LoRA | 0,01115216 | 0,04302751 |
| Prueba | Persistencia | 0,00878095 | 0,04337830 |
| Prueba | MOMENT original | 0,01835810 | 0,05843636 |
| Prueba | MOMENT + LoRA | 0,01156939 | 0,04238525 |

En prueba, LoRA reduce aproximadamente **36,98 % el MAE** y **27,47 % el RMSE** respecto a MOMENT original. Frente a persistencia, presenta aproximadamente **31,76 % más MAE** y **2,29 % menos RMSE**. El resultado depende de la métrica y no acredita superioridad general sobre persistencia.

### Promedio macro por condición

Este promedio asigna el mismo peso a cada condición evaluada, independientemente de sus ventanas.

| Conjunto | MAE persistencia | MAE MOMENT | MAE LoRA |
|---|---:|---:|---:|
| Validación | 0,01201234 | 0,02276489 | 0,01551851 |
| Prueba | 0,01744514 | 0,03132412 | 0,02170516 |

Persistencia conserva el menor MAE macro. Falta calcular intervalos de confianza por condición/día y estabilidad por subperiodos. Los tokens complementarios y ventanas de una condición no son observaciones independientes.

Fuentes: [entrenamiento](runs/mvp/full_model/training.json), [recarga](runs/mvp/full_model/adapter_verification.json), [validación](runs/mvp/full_evaluation/validation/evaluation.json), [prueba](runs/mvp/full_evaluation/test/evaluation.json).

### Episodios de anomalía

Se calibró en validación el percentil 99,5 del MAE de LoRA, con umbral **0,2769572139**, y se aplicó sin reajustarlo en prueba. El score representa un residual de predicción.

Se guardaron **23.127 episodios de validación y 19.055 de prueba**, agrupados por condición y bloque temporal. Se conservaron errores por activo y ventana para inspeccionar objetivos, contexto y actividad.

El máximo score de prueba fue aproximadamente 1,061323, con dos activos agrupados. La reconstrucción no está acotada al rango del spread y por eso el residual puede superar uno. Ese comportamiento merece análisis antes de usar el ranking operativamente.

Los resultados incluyen condition_id y asset_id. La resolución a títulos legibles de mercado y la revisión independiente de los episodios quedan pendientes. La pérdida predictiva no constituye una medida validada de precisión de detección de fraude.

Fuentes: [episodios de prueba](runs/mvp/full_evaluation/test/episodes.parquet), [evidencia por ventana](runs/mvp/full_evaluation/test/errors.parquet).

## 10. Interrupciones y resolución

Los logs acreditan **tres interrupciones técnicas del FULL**. Dos fueron agotamientos del límite interno de memoria de DuckDB. La primera fue un límite de descriptores de archivo. No hay evidencia de tres agotamientos de memoria distintos en esta corrida.

| Incidente | Etapa y causa | Corrección | Resultado |
|---|---|---|---|
| Too many open files | Catálogo, al consultar simultáneamente numerosos archivos de relaciones | Importar lotes de 128 a una tabla DuckDB persistente | Se incorporaron los 135.777 archivos de relaciones |
| OutOfMemoryException, 1,8 GiB/1,8 GiB | Agrupación del catálogo con límite de 2 GB | Límite de 8 GB, dos hilos, preserve_insertion_order=false y reutilización de la tabla importada | Catálogo completado, 3.071.967 activos, cero contradicciones registradas |
| OutOfMemoryException, 1,8 GiB/1,8 GiB | Ordenamiento de locators de 33 millones de ventanas para crear el lector Arrow del pack | Límite de 8 GB, dos hilos y orden de inserción desactivado | Empaquetado y entrenamiento completados |

Los mensajes de 1,8 GiB describen el límite de memoria del motor en esas consultas. No acreditan agotamiento de los 32 GB físicos ni de la VRAM. Los límites de 2 GB resultaron insuficientes para la cardinalidad global.

El ensayo de un día no reprodujo la presión de agrupar todas las relaciones ni ordenar 33 millones de locators. Esta fue una limitación de la validación previa al FULL. Aumentar memoria y reducir hilos resolvió las consultas medidas. Para crecer más se debe particionar la agregación y el ordenamiento, con pruebas de cardinalidad y spill.

Las correcciones se aplicaron en catalog.py para descriptores y agrupación, y en windows.py para el empaquetado.

Fuentes: [primer fallo](runs/mvp/full.log), [memoria del catálogo](runs/mvp/full_resume.log), [memoria del empaquetado](runs/mvp/full_recovery.log).

### Otros problemas y decisiones

- Las primeras alternativas COPY de consolidación generaron demasiadas escrituras temporales. Se sustituyeron por particionado incremental Arrow y consolidación por buckets, conservando evidencias de desarrollo.
- Se corrigió una dependencia implícita de fecha en reporting.py y se formalizaron cuatro benchmarks representativos.
- El deadline dejó de derivarse de la fecha del inventario. La misión registra intentos, presupuesto, extensiones y pausas.
- Se eliminó la dependencia obligatoria de una carpeta experimental mediante un recibo explícito de preparación.
- Se incorporaron reservas globales de RAM y disco y comprobación de espacio antes del empaquetado.
- Validación y prueba se empaquetaron por separado y sus packs se liberaron después de verificar las evaluaciones.
- Reiniciar cosmic-greeter no detuvo el proceso observado. La suspensión conservó el proceso, cuya lectura y escritura se comprobaron después.
- Por petición del usuario se pausó y cerró la ejecución para apagar y usar Windows. Se sincronizó el disco y se reanudó al regresar a Linux, reutilizando censo y selección.
- Las reanudaciones repitieron comprobaciones de integridad de los derivados. Ese coste no requiere releer todos los eventos originales.

## 11. Almacenamiento y recursos

Tamaños medidos al preparar este informe. GB decimales. Las carpetas corresponden a artefactos persistentes y no reconstruyen el pico temporal histórico.

| Artefacto | GB |
|---|---:|
| Series consolidadas | 765,44 |
| Censo, índices y selección | 84,52 |
| Pack de entrenamiento | 86,82 |
| Modelo, checkpoints y adaptadores | 1,28 |
| Evaluaciones finales | 3,29 |
| Catálogo y tabla de trabajo persistente | 26,17 |

Los packs de validación y prueba ya se liberaron tras confirmar sus evaluaciones. El adaptador aislado ocupa alrededor de 0,48 MB. El directorio del modelo incluye checkpoints acumulados y ocupa más.

Al lanzamiento se presupuestaron unos 949,1 GB frente a 959,8 GB libres y se reservaban 34,36 GB para contingencias. El usuario liberó espacio adicional durante la corrida. No se puede atribuir toda variación de espacio a una operación del pipeline ni reconstruir un pico global exacto a partir de esos estados.

El mayor pico RSS de proceso en los reportes por archivo fue aproximadamente 5,61 GB. Ese valor no representa RAM total simultánea ni necesariamente todas las etapas posteriores. El censo presentó observaciones de aproximadamente 4,5–4,8 GB RSS. Falta una contabilidad global validada de CPU-horas, RAM agregada, energía y tiempo exacto de suspensión.

## 12. Mejoras propuestas para tiempos y robustez

Estas propuestas no representan nuevos resultados ejecutados.

### 12.1 Instrumentación por intento y revalidación

Guardar reportes inmutables por intento, separando bytes nuevos, trabajo reutilizado, verificación, red y espera. Corregir el factor de solapamiento del reporte sobrescrito. Mantener controles de integridad al publicar y cerrar, y evaluar comprobaciones selectivas en reanudaciones intermedias sobre artefactos ya verificados e inmutables.

Las verificaciones de cobertura acumularon casi 70 minutos. Reducir lecturas repetidas exige conservar detección de corrupción y de cambios de identidad. El recibo de reanudación debe conservar el costo de trabajo histórico sin atribuirlo a ese intento.

### 12.2 Recuperación por día y bucket

El censo tardó casi 17 horas y publica su recibo global al terminar. Incorporar confirmaciones por día/bucket y un índice de fragmentos permitiría recuperarlo sin repetir todo si falla tarde. Aplicar confirmaciones transaccionales también a lotes del catálogo y shards de empaquetado.

Reducir pequeños archivos normalizados, aumentar buffers de forma medida y ajustar row groups puede reducir aperturas y metadatos. La comparación debe comprobar valores, claves, conteos y exclusiones.

### 12.3 Locators y tensores por shards

El ordenamiento global de locators fue uno de los fallos. Publicar índices ordenados por archivo, row group y fila desde el censo permite fusión externa acotada. Un Dataset con lectura incremental y caché por row group puede reducir materialización de tensores, pero requiere medir el nuevo throughput de GPU.

Se puede barajar mediante un buffer reproducible por shards y comparar su efecto con la permutación actual. La solución debe manejar de forma consistente interrupciones y publicaciones parciales.

### 12.4 Concurrencia basada en contención real

La consolidación acumuló 59,66 horas-trabajador y la ingestión 52,61. El censo añadió casi 17 horas de pared. Son los principales objetivos de optimización integral.

Comparar tres trabajos/un consolidator con cuatro/dos sobre archivos completos equivalentes permitirá detectar contención de CPU/disco. El factor 1,97 no demuestra que aumentar trabajadores siempre ayude. Una GPU mayor no acelera directamente el censo o la consolidación.

### 12.5 Throughput de modelo y evaluación

El entrenamiento alcanzó unas 1.467 ventanas/s con batch efectivo y microbatch 256. Un microbatch mayor por sí solo no cambia la carga si el batch efectivo permanece en 256. Probar batches efectivos mayores o compilación exige medir convergencia, calidad y tiempo total.

La evaluación usa batch 64 y crea registros por ventana en Python. Vectorizar la construcción de resultados Arrow y medir batches mayores puede reducir aproximadamente una hora por split. El ahorro real debe medirse y conservar exactamente las métricas y la calibración.

## 13. Mejoras propuestas para el modelo

### 13.1 Determinar dónde aporta frente a persistencia

Persistencia conserva el mejor MAE global y macro. Desglosar resultados por actividad, volatilidad, spread, fuente, activo y condición ayudará a identificar el origen de la mejora en RMSE. Medir intervalos de confianza por bloques condición/día y desempeño en colas.

Para el negocio, revisar los primeros K episodios, estabilidad de ranking y evidencia histórica. La ausencia de etiquetas fiables impide convertir estos errores en precisión de detección de fraude.

### 13.2 Pérdida, referencia y horizonte

El entrenamiento usa MSE, mientras que MAE es otra métrica principal. Comparar Huber, MAE y combinaciones con MSE puede modificar ese compromiso. Predicción residual respecto al último spread visible incorpora explícitamente la referencia de persistencia.

Comparar horizonte fijo y mínimos de contexto más estrictos ayudaría a homogeneizar la tarea frente al último patch completo actual. Debe reportarse su impacto en cobertura.

### 13.3 Calidad y rango de salida

Las reconstrucciones pueden salir del rango del spread y producir residuales superiores a uno. Evaluar una transformación o cabeza compatible con el dominio, y errores separados por estados de calidad. Comparar calibración robusta según actividad y condición para evitar que el ranking dependa solo de la escala del residual.

La elegibilidad del 3,04 % evidencia sparsidad importante. Revisar patches, huecos y contexto podría ampliar cobertura, con validación explícita y sin inventar cotizaciones ejecutables mediante relleno.

### 13.4 LoRA y canales

Comparar rangos 4, 8, 16 y 32, más proyecciones adaptadas y más de una pasada con early stopping en validación. Los 118.792 entrenables corresponden a la configuración elegida. Aumentarlos no garantiza mejora y exige benchmarks.

Los derivados permiten experimentar con mid-price, extremos y actividad sin releer el terabyte. El encoder investigado procesa canales con pesos compartidos e independencia de canal. Añadir variables requiere diseñar y verificar su combinación, sin asumir aprendizaje automático de relaciones multivariables.

Las futuras variantes deben seleccionarse con entrenamiento/validación. Usar reiteradamente esta prueba para decidir cambios reduce su valor como prueba independiente. Se necesita otro periodo reservado o evaluación temporal sucesiva.

## 14. Evidencia, reproducción y límites

La interfaz principal es el [notebook MVP](mvp_polymarket_moment_lora.ipynb), apoyado por [módulos](src/polymarket_mvp/), [pyproject.toml](pyproject.toml) y [uv.lock](uv.lock).

Comprobaciones desde la raíz, sin relanzar FULL:

    uv sync --frozen --python 3.14
    uv run python -m unittest discover -s tests/mvp -v
    uv run python -c "import json; print(json.load(open('runs/mvp/full_result.json'))['complete'])"

| Evidencia | Ubicación |
|---|---|
| Resultado completo | runs/mvp/full_result.json |
| Tiempos, intentos y pausas | runs/mvp/mission.json |
| Cobertura original | runs/mvp/coverage.json |
| Catálogo | runs/mvp/catalog/catalog.json |
| Censo y exclusiones | runs/mvp/full_census/census.json |
| Selección y mínimo | runs/mvp/full_census/selection.json y selection_budget.json |
| Historial | runs/mvp/full_model/history.jsonl |
| Checkpoint recuperable | runs/mvp/full_model/checkpoint.json y archivo referenciado |
| Adaptador final | runs/mvp/full_model/adapter/ |
| Verificación de recarga | runs/mvp/full_model/adapter_verification.json |
| Métricas y hashes | runs/mvp/full_evaluation/validation y test, evaluation.json y verified.json |
| Episodios y evidencia | episodes.parquet y errors.parquet en cada evaluación |
| Finalización | runs/mvp/full_resumed_after_shutdown.log |

Antes de confirmar el resultado integral se verificaron nuevamente hashes de las cinco salidas de cada evaluación, identidad del adaptador y coincidencia de conteos entre censo, entrenamiento y evaluación.

Los archivos voluminosos están locales en runs/, ignorado por Git. La publicación del código en GitHub no incluye todos los derivados y checkpoints. Este documento no cambia ese estado.

La ejecución de datos, fine-tuning y evaluación quedó completada. Quedan como límites la validación de negocio de los episodios, nombres legibles de mercados, intervalos de confianza, portabilidad exacta a Colab e instrumentación global de recursos. Los problemas de memoria y descriptores demostraron que la prueba de un día no cubrió toda la cardinalidad de las etapas globales. Las correcciones permitieron completar la corrida, y las mejoras futuras deben preservar cobertura, integridad y separación temporal.
