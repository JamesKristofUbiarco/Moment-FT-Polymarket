# Polymarket → MOMENT LoRA

La interfaz principal es `mvp_polymarket_moment_lora.ipynb`. El MVP exige procesar los 164 Parquet originales de la revisión `efe472f1f00a62f2cab1fed2851da439d9c1fe11` y completar entrenamiento y evaluación. **Los experimentos del día completo no equivalen al resultado integral.**

## Entorno reproducible

```sh
uv sync --frozen --python 3.14
uv run python -m unittest discover -s tests/mvp -v
uv run jupyter lab mvp_polymarket_moment_lora.ipynb
```

`uv.lock` fija dependencias transitivas. MOMENT se instala desde el commit oficial `38f7310ad594100747ca2a8357e9c7ca7d323e0e`; el checkpoint se fija a `411e288267f82cce86296dbe4d6c8bc533cc162f`. No se usa pip.

| Componente observado | Función y justificación |
|---|---|
| Python 3.14.9 / uv 0.11.25 | Entorno local previamente existente, resolución y ejecución aislada |
| PyArrow 26.0.1 | Lectura incremental, metadatos y persistencia Parquet |
| Polars 2.1.0 | Validación y agregación vectorizadas; sin bucle Python por evento ordinario |
| DuckDB 2.0.0 | Fusión por buckets, consultas del censo y evaluación; límite de memoria explícito |
| huggingface-hub 2.4.0 / httpx bloqueado | Inventario fijado, rangos remotos y descarga recuperable con SHA256 |
| PyTorch 2.15.0, CUDA 13.2 | Dataset sobre memmap y entrenamiento BF16 en RTX 5070 Ti; controlador local 595.104.02 |
| momentfm 0.1.5, PEFT 0.21.2, Transformers 5.22.2 | Checkpoint preentrenado y adaptadores q/v, con cabeza entrenable |
| psutil bloqueado | RSS, CPU, E/S observada y comprobación de procesos |

El notebook puede coordinar un entorno uv aislado en Colab. La versión actual se ejecutó localmente en un kernel Jupyter limpio; **esta implementación exacta todavía no tiene validación de ejecución en Colab**. Se necesita un controlador compatible con la rueda CUDA resuelta. Copiar paquetes de ventanas y checkpoints permite trasladar la etapa GPU sin volver a leer los originales. El almacenamiento temporal de Colab no es durable.

## Datos y trazabilidad

La ingesta obtiene cotizaciones, actividad, tamaños reportados de operaciones y relaciones activo–condición en la misma lectura. No hay mapeo global previo. Los parciales conservan claves repetidas hasta la consolidación determinista. Las relaciones contradictorias quedan registradas; el censo excluye ventanas con identidad ambigua.

Se distinguen cotización bilateral, lados explícitamente vacíos, valores límite, nulos, estados desconocidos e inválidos. Una ausencia de evento no se convierte en liquidez cero. El tamaño agregado de operaciones es tamaño bruto reportado, sin afirmar volumen negociado deduplicado. La reconstrucción L2 no es una dependencia.

Cada original tiene `source.json`, metadatos Parquet, `original_reads.jsonl`, manifiesto SQLite, progreso y reportes. El manifiesto confirma una unidad solo después de publicar y sincronizar los objetos inmutables. Los objetos huérfanos no se consultan. Antes de confiar en unidades existentes se verifican los hashes. La compactación confirma y verifica reemplazos antes de retirar parciales; conserva las relaciones originales.

Las lecturas HTTP contabilizan payload obtenido por el cliente, no cabeceras/TLS ni operaciones físicas internas del servidor. El log distingue decodificaciones de grupos, rangos remotos y reintentos; las métricas del SO muestran tráfico físico local observado. Las pasadas de consolidación, validación y hashes se reportan aparte. Los benchmarks de desarrollo repiten originales deliberadamente; la cobertura final elige una sola representación por archivo.

## Ejecución y recuperación

```sh
uv run python -m polymarket_mvp.runner --date 2026-06-06 --tag experimento
uv run python -c 'from polymarket_mvp.reporting import progress; print(progress("runs/mvp"))'
uv run python -c 'from polymarket_mvp.reporting import projection; print(projection("runs/mvp"))'
uv run python -m polymarket_mvp.full --root runs/mvp --projection runs/mvp/projection.json
```

La última operación falla antes de iniciar FULL si faltan tres benchmarks completos representativos o si las puertas de tiempo/disco no pasan. No es una autorización para una ejecución de varios días. El notebook permite lanzar procesos independientes mediante tmux y verificar su sesión. Consultar también PID, estado y logs; la existencia de un PID antiguo no prueba actividad.

Para reanudar, repetir los mismos argumentos sobre la misma salida. Una identidad de fuentes o reglas de características diferente exige una salida nueva. Los lotes no son la identidad del resultado; los límites de unidades de confirmación sí se fijan. Los checkpoints de entrenamiento incluyen optimizador, RNG y cursor en límites del optimizador. `train(..., resume=True)` valida identidad y SHA256.

La evaluación final usa cortes UTC: entrenamiento antes del 21/06; validación hasta el 16/07; prueba hasta el 11/08. Las ventanas que cruzan fronteras se excluyen. El experimento intradiario del 06/06 usa otros cortes, documentados como exploratorios; no es prueba final independiente.

## Evidencia medida hasta ahora

- Día 2026-06-06 completo: 93.595.651 eventos, 934 grupos, 70.363 activos y 35.636 condiciones. Descarga de 510.963.243 bytes verificada con SHA256.
- Ingesta inicial: 193,3 s. Fusión de bordes vectorizada: 104,7 s. Eliminar una agrupación parcial redundante se mide en los siguientes días completos.
- Consolidación COPY global: ensayo censurado; E/S excesiva. Particionado COPY: aproximadamente 49 GB escritos en un ensayo completo. Escritores Arrow incrementales: 1,7 GB escritos y 127,1 s de consolidación sobre el mismo día; los costos de los intentos anteriores se conservan por separado.
- Censo de ese día: 488.621 bloques. El experimento cronológico intradiario dejó 21.247 elegibles: 9.002 train, 5.987 validación, 6.258 prueba. La cobertura mínima de entrenamiento fue 5.387 ventanas. Las exclusiones y el sesgo hacia actividad suficiente se conservan en el censo.
- LoRA: 9.002 ventanas, 36 actualizaciones, 118.792 parámetros entrenables. Gradientes y cambios reales verificados. Adaptadores y checkpoints guardados.
- Microbatch 256: aproximadamente 1.907 ventanas/s; la selección final se calculará con el censo integral y presupuesto restante, no con esta cifra aislada.
- Recuperación real en procesos separados: diferencia máxima de parámetros 0 respecto a una ejecución continua.
- Prueba exploratoria: MAE persistencia 0,01043; MOMENT inicial 0,02087; LoRA 0,01967. LoRA no supera persistencia.

Los reportes exactos, límites de medición y estados actuales están en `runs/mvp/`. No se afirma cobertura 164/164 mientras `coverage.json` no la verifique, ni entrenamiento integral mientras no exista un `full_result.json` válido.

## Evidencias incluidas en Git

- [Benchmark concurrente](evidence/benchmark/four_partitions.json). Cuatro particiones completas, 27,45 GB y 5.054.034.321 eventos procesados en 1 h 32 min 19 s con los originales en disco. Extrapolar linealmente por bytes da unas 66 horas de procesamiento local. Esa cifra no incluye descarga, censo, entrenamiento ni evaluación.
- [Variante optimizada del 6 de junio](evidence/benchmark/optimized_june_06.json). Procesó 93.595.651 eventos en 159,7 s con el original en disco.
- [Piloto LoRA](evidence/pilot/). Incluye el adaptador exploratorio del 6 de junio, los resultados de validación y prueba, y la verificación de recuperación del entrenamiento.

El procesamiento integral y el fine-tuning definitivo están pendientes. Los originales, derivados voluminosos, entornos y estados de ejecución permanecen en el espacio de trabajo local.
