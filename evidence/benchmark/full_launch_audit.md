# Auditoría de rendimiento y lanzamiento FULL

Los cuatro archivos del benchmark ya estaban en disco. El tiempo transcurrido del grupo fue 5.539,16 s. La suma de ingestión y consolidación fue 10.909,54 s. Su cociente, 1,97, mide solapamiento y no es una comparación experimental contra un solo trabajador.

| Fecha UTC | Eventos | Original GB | Ingesta s | Lectura y decodificación s | Transformación s | Relaciones s | Publicación s | Reparticionado s | Agregación y escritura s | Verificación aislada | Eventos/s de ingesta | RSS máx. por proceso GB | Parciales GB | Intermedios GB | Final GB | Escrituras SO / original |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|
| 2026-02-26 | 550,440,717 | 3.15 | 678.6 | 56.0 | 386.8 | 36.9 | 231.3 | 279.3 | 557.8 | Sin separar | 811,078 | 3.06 | 7.83 | 7.48 | 2.57 | 5.70× |
| 2026-04-26 | 1,939,840,389 | 9.64 | 1784.1 | 176.8 | 1031.8 | 114.9 | 562.9 | 850.5 | 981.0 | Sin separar | 1,087,272 | 3.22 | 17.19 | 16.36 | 5.99 | 4.13× |
| 2026-06-06 | 93,595,651 | 0.51 | 124.3 | 10.8 | 76.3 | 6.7 | 36.5 | 33.4 | 88.9 | Sin separar | 753,152 | 2.26 | 0.99 | 1.02 | 0.44 | 4.79× |
| 2026-06-09 | 2,470,157,564 | 14.15 | 2795.7 | 231.5 | 1734.4 | 143.2 | 813.9 | 1478.9 | 1249.4 | Sin separar | 883,559 | 5.00 | 24.96 | 23.91 | 8.89 | 4.13× |

Relaciones está incluido en transformación. La agregación incluye la escritura Parquet. La validación de conteos y los hashes forman parte del tiempo total de consolidación. Los registros muestran una decodificación por grupo original en las cuatro ejecuciones. La amplificación es el volumen de escritura observado por el SO, incluidas las salidas finales, dividido entre los bytes originales.

El ensayo completo del 6 de junio en un entorno nuevo de uv tardó 67,1 s en ingestión y 53,4 s en consolidación. En el benchmark concurrente fueron 124,3 s y 122,5 s respectivamente. La carga de fondo y la concurrencia fueron distintas. La comparación posterior verificó todas las columnas de las 28.175.552 filas y obtuvo una diferencia de 0 para el tamaño bruto reportado.

El censo exploratorio contó 488.621 bloques, de los cuales 21.247 eran elegibles. El entrenamiento completó 9.002 ventanas y 36 actualizaciones con una recarga intermedia del checkpoint. El pico de memoria CUDA asignada fue 1,31 GB. MAE de prueba: persistencia 0,01043, MOMENT original 0,02087, LoRA 0,01961.

La proyección congelada al lanzamiento estimó 103.5 h frente a 120 h de presupuesto. Se midieron 959.8 GB libres y se proyectaron 949.1 GB. La proyección incluye 769.3 GB de series, 11.8 GB para manifiestos y relaciones, 91.4 GB de censo y 34.4 GB de reserva. El almacenamiento de ingestión y el de tensores corresponden a fases distintas. La reserva transitoria de la partición más grande es de 120.2 GB. El censo y la selección real pueden diferir sustancialmente de las estimaciones de un día.

El expediente de ejecución está en `runs/mvp/mission.json`. Los tiempos de trabajadores se acumulan aparte del tiempo de pared. Consultar `full_progress.json`, `full_status.json` y `full.log`. La ejecución solo está completa cuando existe `full_result.json` con cobertura 164/164, entrenamiento y ambas evaluaciones.
