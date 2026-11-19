# Taller local: Data Engineer Junior Advanced

ETL local con PySpark y Spark SQL (`local[*]`) sobre las ventas sintéticas de una tienda latinoamericana:
lectura, perfilamiento, limpieza con cuarentena, importe neto, agregación mensual, incremental idempotente,
salida en Parquet y controles de calidad.

## Cómo ejecutarlo

Requisitos: Python 3.12, Java 17/21 y PySpark (`pip install -r requirements.txt`).

1. Dejar los datos en `datos/` (`customers.csv`, `products.csv`, `orders.csv`, `order_items.csv` y `datos/incremental/*.jsonl`).
2. Ejecutar:
   ```bash
   python taller.py
   ```
3. Los resultados quedan en `output/` y el script imprime el perfilamiento y los controles de calidad.

## Estructura

```
datos/                       entrada (CSV + JSONL incremental)
taller.py                    ETL completo
output/
├── ventas_detalle/          una fila por línea válida, con importe_neto
├── resumen_mensual/         ventas netas por mes, país, canal y categoría
├── cuarentena/orders/       pedidos apartados, con su motivo
├── cuarentena/order_items/  líneas apartadas, con su motivo
└── incremental/             mismo resultado tras aplicar el lote incremental
```

## Anomalías y decisiones

| Anomalía | Decisión |
|---|---|
| Cliente `C0020` y pedido `O00012` duplicados | Corregir: deduplicar por clave (el pedido duplicado multiplicaría sus líneas en el join) |
| Status `deliverd` (`O00030`) | Corregir a `delivered` |
| Fecha imposible `2026-99-31` (`O00025`) | Cuarentena |
| Pedido sin cliente (`O00011`) o con cliente inexistente (`O00007` → `C9999`) | Cuarentena |
| Precio negativo (`P088`) o nulo (`P089`) | Cuarentena de sus líneas |
| Cantidad 0 o negativa, descuento 1.2 | Cuarentena |
| Líneas huérfanas (`O99999`, `P999`) | Cuarentena |
| Email nulo o mal formado | Corregir a `null` y marcar `email_valido` |
| `signup_date` en el futuro (919 clientes, hasta 2059) | Marcar `signup_futuro` (no afecta a las ventas) |

Decisiones de negocio: los pedidos `cancelled` no suman a las ventas netas; las ventas de productos
inactivos sí cuentan; las líneas de un pedido en cuarentena también van a cuarentena.

Importe neto por línea: `quantity * unit_price * (1 - discount_pct)`, en `decimal` y redondeado a 2 decimales.

## Resultados

| | Histórico | Con incremental |
|---|---|---|
| Líneas de entrada | 40.026 | 40.226 |
| Líneas válidas | 39.190 | 39.386 |
| Líneas en cuarentena | 836 | 840 |
| Pedidos en cuarentena | 3 | 3 |
| Ventas netas (sin cancelados) | 71.680.923,28 | 72.087.222,04 |

## Controles de calidad (todos OK)

1. **Reconciliación**: la suma de `ventas_netas` del resumen es igual a la suma de `importe_neto` del detalle válido.
2. **Unicidad**: el detalle tiene una fila por `line_id` y las mismas filas que antes de los joins (no se duplican importes).
3. **Balance**: líneas de entrada = válidas + cuarentena (ninguna fila se descarta en silencio).

## Incremental idempotente

Se une el histórico con el lote y se conserva una fila por `order_id`, la de `updated_at` más reciente
(`row_number` sobre una ventana), y una por `line_id`. Aplicar el mismo lote dos veces da el mismo resultado
(20.100 pedidos y 40.226 líneas), y la actualización tardía de `O00005` (`updated_at` 2026-03-05) prevalece.
La salida se escribe con `mode("overwrite")`.
