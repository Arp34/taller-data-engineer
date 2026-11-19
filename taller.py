"""
Taller local: Data Engineer Junior Advanced

ETL local con PySpark y Spark SQL (local[*]): lectura, perfilamiento, limpieza con
cuarentena, importe neto, agregación mensual, salida en Parquet, controles de calidad
e incremental idempotente.

Uso:  python taller.py      (los datos deben estar en datos/, junto a este archivo)
"""
import os
from pathlib import Path

# Rutas relativas a la carpeta del script (datos/ y output/), se ejecute desde donde se ejecute
os.chdir(Path(__file__).resolve().parent)


# ========================================================================================
# 0. Sesión de Spark
# ========================================================================================
import sys
from pyspark.sql import functions as F
from pyspark.sql import Window

LAB_SCRIPTS = Path(r"C:\Users\Arp\apache-spark-lab\scripts")

if LAB_SCRIPTS.exists():
    # Solo en mi PC: Windows sin winutils necesita el arreglo del laboratorio
    # (Java 17 portable + sistema de archivos local en Java puro). Por dentro es
    # el mismo SparkSession.builder.master("local[*]") del enunciado.
    sys.path.insert(0, str(LAB_SCRIPTS))
    from spark_utils import crear_spark
    spark = crear_spark("taller-data-engineer")
else:
    # En cualquier otra máquina: la sesión tal como pide el enunciado.
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("taller-data-engineer")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())

print("Spark", spark.version, "| master:", spark.sparkContext.master)


# ========================================================================================
# 1. Lectura local y modelo de datos
#
# Leemos todo como texto (`inferSchema=False`). Si Spark adivinara los tipos, un valor mal formado
# (por ejemplo la fecha `2026-99-31`) se convertiría en `null` sin avisar o dejaría la columna entera
# como texto. Así, los tipos los convertimos nosotros y podemos contar lo que no se pudo convertir.
#
# Granularidad y claves
#
# | Archivo | Una fila es... | Clave | Relación |
# |---|---|---|---|
# | `customers.csv` | un cliente | `customer_id` | |
# | `products.csv` | un producto | `product_id` | |
# | `orders.csv` | un pedido (cabecera) | `order_id` | `customer_id → customers` |
# | `order_items.csv` | una línea de un pedido | `line_id` | `order_id → orders`, `product_id → products` |
#
# El precio no está en las líneas: `unit_price` viene de `products`.
# ========================================================================================
def leer_csv(nombre):
    return (spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(f"datos/{nombre}.csv"))

customers_raw   = leer_csv("customers")
products_raw    = leer_csv("products")
orders_raw      = leer_csv("orders")
order_items_raw = leer_csv("order_items")

RAW = {"customers": (customers_raw, "customer_id"),
       "products": (products_raw, "product_id"),
       "orders": (orders_raw, "order_id"),
       "order_items": (order_items_raw, "line_id")}

for nombre, (df, clave) in RAW.items():
    print(f"{nombre:12} {df.count():>6} filas | columnas: {', '.join(df.columns)}")

orders_raw.printSchema()
orders_raw.show(5, truncate=False)


# ========================================================================================
# 2. Perfilamiento y diagnóstico de calidad
#
# 2.1 Claves y nulos
# ========================================================================================
def perfil(nombre, df, clave):
    total = df.count()
    distintos = df.select(clave).distinct().count()
    nulos = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).first().asDict()
    nulos = {c: n for c, n in nulos.items() if n}
    print(f"{nombre:12} filas={total:>6} | {clave} distintos={distintos:>6} | "
          f"claves repetidas={total - distintos} | nulos={nulos or 'ninguno'}")

for nombre, (df, clave) in RAW.items():
    perfil(nombre, df, clave)

print("\nFilas con clave repetida:")
for nombre, (df, clave) in RAW.items():
    rep = df.groupBy(clave).count().filter("count > 1").select(clave)
    if rep.count():
        df.join(rep, clave).show(truncate=False)


# ========================================================================================
# 2.2 Tipos y dominios (valores que no se pueden convertir o están fuera de rango)
# ========================================================================================
TS_FMT = "yyyy-MM-dd'T'HH:mm:ssX"          # 2026-01-08T22:54:00Z
EMAIL_RE = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

def ts(col):
    return F.try_to_timestamp(F.col(col), F.lit(TS_FMT))

print("Pedidos con fecha mal formada:")
orders_raw.filter(F.col("order_ts").isNotNull() & ts("order_ts").isNull()).show(truncate=False)

print("Valores de status (buscando errores tipográficos):")
orders_raw.groupBy("status").count().orderBy("count").show()

print("Productos con precio nulo o <= 0:")
precio = F.col("unit_price").try_cast("decimal(10,2)")
products_raw.filter(precio.isNull() | (precio <= 0)).show(truncate=False)

print("Líneas con cantidad <= 0 o descuento fuera de [0, 1]:")
qty, desc = F.col("quantity").try_cast("int"), F.col("discount_pct").try_cast("decimal(5,4)")
order_items_raw.filter(qty.isNull() | (qty <= 0) | desc.isNull() | (desc < 0) | (desc > 1)).show(truncate=False)

print("Clientes con email nulo o mal formado:")
customers_raw.filter(F.col("email").isNull() | ~F.col("email").rlike(EMAIL_RE)).show(truncate=False)

futuros = customers_raw.filter(F.to_date("signup_date") > F.current_date())
print(f"Clientes con signup_date en el futuro: {futuros.count()} "
      f"(máximo: {futuros.agg(F.max('signup_date')).first()[0]})")


# ========================================================================================
# 2.3 Integridad referencial (referencias huérfanas)
# ========================================================================================
def huerfanos(hijo, col_hijo, padre, col_padre, nombre):
    h = (hijo.filter(F.col(col_hijo).isNotNull())
             .join(padre.select(F.col(col_padre).alias(col_hijo)), col_hijo, "left_anti"))
    print(f"{nombre}: {h.count()}")
    h.show(truncate=False)

huerfanos(orders_raw, "customer_id", customers_raw, "customer_id", "orders.customer_id sin cliente")
huerfanos(order_items_raw, "order_id", orders_raw, "order_id", "order_items.order_id sin pedido")
huerfanos(order_items_raw, "product_id", products_raw, "product_id", "order_items.product_id sin producto")


# ========================================================================================
# 2.4 Anomalías encontradas y decisión
#
# | # | Tabla | Anomalía | Decisión | Por qué |
# |---|---|---|---|---|
# | 1 | customers | `C0020` duplicado (fila idéntica) | Corregir: deduplicar | Es la misma fila; no se pierde información |
# | 2 | orders | `O00012` duplicado (fila idéntica) | Corregir: quedarse con 1 por `order_id` (la de `updated_at` más reciente) | Si no, el join multiplica sus 3 líneas y duplica su importe |
# | 3 | orders | `O00030` con status `deliverd` | Corregir a `delivered` | Error tipográfico evidente |
# | 4 | orders | `O00025` con `order_ts = 2026-99-31` (mes 99) | Cuarentena | No se puede asignar a un mes; inventar la fecha sería falsear el dato |
# | 5 | orders | `O00011` sin `customer_id` | Cuarentena | Sin cliente no hay país |
# | 6 | orders | `O00007` apunta a `C9999`, que no existe | Cuarentena | Referencia huérfana |
# | 7 | products | `P088` con precio `-12.5` | Cuarentena de sus líneas | Un precio negativo daría ventas negativas |
# | 8 | products | `P089` sin precio | Cuarentena de sus líneas | No se puede calcular el importe |
# | 9 | order_items | `L0000001` cantidad 0 y `L0000002` cantidad -2 | Cuarentena | Cantidad fuera de rango (debe ser > 0) |
# | 10 | order_items | `L0000005` descuento `1.2` (120 %) | Cuarentena | Fuera del dominio [0, 1]; daría importe negativo |
# | 11 | order_items | `L0000003` → pedido `O99999` y `L0000004` → producto `P999` | Cuarentena | Referencias huérfanas |
# | 12 | customers | `C0013` sin email y `C0007` con `not-an-email` | Corregir a `null` + marca `email_valido` | No afecta a las ventas; no hay motivo para descartar al cliente |
# | 13 | customers | `signup_date` en el futuro (hasta 2059) | Marcar (`signup_futuro`) | No afecta a las ventas; se documenta para el dueño del dato |
#
# Otras decisiones de negocio
# - Pedidos `cancelled`: no son venta. Se conservan en el detalle (con su `status`) pero no suman a las ventas netas.
# - Productos inactivos (`active = false`): sus ventas históricas sí cuentan; inactivo significa que hoy no se vende, no que la venta pasada fuera falsa.
# - Las líneas de un pedido en cuarentena también van a cuarentena (motivo `pedido_en_cuarentena`), para que nada desaparezca sin explicación.
#
# Corregir vs. rechazar vs. cuarentena: *corregir* cuando el valor correcto es evidente; *rechazar* es descartar
# dejando constancia; *cuarentena* es apartar la fila con su motivo en `output/cuarentena/` para revisarla después.
# Aquí no se descarta ninguna fila en silencio: filas de entrada = filas válidas + filas en cuarentena (se comprueba en el control 3).
# ========================================================================================

# ========================================================================================
# 3. Limpieza, uniones y cálculo del importe neto
#
# Se definen como funciones porque el reto incremental (fase 6) reutiliza exactamente la misma lógica.
#
# Precisión elegida: precios `decimal(10,2)`, descuento `decimal(5,4)` e importe neto por línea redondeado a 2 decimales
# (`decimal(14,2)`). Con `decimal` las sumas son exactas: el total agregado debe coincidir al céntimo con el detalle.
# ========================================================================================
def limpiar_customers(df):
    email_ok = F.coalesce(F.col("email").rlike(EMAIL_RE), F.lit(False))
    out = (df.dropDuplicates()                                   # anomalía 1
             .withColumn("signup_date", F.try_to_date("signup_date", "yyyy-MM-dd"))
             .withColumn("email_valido", email_ok)
             .withColumn("email", F.when(email_ok, F.col("email")))   # anomalía 12
             .withColumn("signup_futuro", F.col("signup_date") > F.current_date()))  # anomalía 13
    return out

def limpiar_products(df):
    return (df.withColumn("unit_price", F.col("unit_price").try_cast("decimal(10,2)"))
              .withColumn("active", F.lower("active") == "true"))

def limpiar_orders(df):
    ultima = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc_nulls_last())
    return (df.withColumn("order_ts", ts("order_ts"))
              .withColumn("updated_at", ts("updated_at"))
              .withColumn("status", F.when(F.col("status") == "deliverd", "delivered")   # anomalía 3
                                     .otherwise(F.col("status")))
              .withColumn("_rn", F.row_number().over(ultima))         # anomalía 2: 1 fila por order_id
              .filter("_rn = 1").drop("_rn"))

def limpiar_items(df):
    return (df.withColumn("quantity", F.col("quantity").try_cast("int"))
              .withColumn("discount_pct", F.col("discount_pct").try_cast("decimal(5,4)"))
              .dropDuplicates(["line_id"]))

customers = limpiar_customers(customers_raw).cache()
products  = limpiar_products(products_raw).cache()
orders    = limpiar_orders(orders_raw)
items     = limpiar_items(order_items_raw)

assert customers.count() == customers.select("customer_id").distinct().count(), "customer_id no es único"
print("customers:", customers_raw.count(), "->", customers.count(),
      "| orders:", orders_raw.count(), "->", orders.count(),
      "| status corregidos:", orders_raw.filter("status = 'deliverd'").count())


# ========================================================================================
# 3.1 Validación y cuarentena
# ========================================================================================
def validar(orders, items):
    """Separa lo válido de la cuarentena (con motivo) y construye el detalle de ventas."""
    o = (orders.join(customers.select("customer_id", "country"), "customer_id", "left")
               .withColumn("motivo",
                   F.when(F.col("customer_id").isNull(), "cliente_nulo")
                    .when(F.col("country").isNull(), "cliente_inexistente")
                    .when(F.col("order_ts").isNull(), "fecha_pedido_invalida")))
    orders_ok = o.filter("motivo IS NULL").drop("motivo")
    orders_q  = o.filter("motivo IS NOT NULL").drop("country")

    # orders tiene order_id único (limpiar_orders), así que estos joins no multiplican líneas
    i = (items
         .join(orders.select("order_id", F.lit(True).alias("_existe")), "order_id", "left")
         .join(orders_ok.select("order_id", F.lit(True).alias("_ok")), "order_id", "left")
         .join(products.select("product_id", "unit_price", "category", F.lit(True).alias("_prod")),
               "product_id", "left")
         .withColumn("motivo",
             F.when(F.col("_existe").isNull(), "pedido_inexistente")
              .when(F.col("_ok").isNull(), "pedido_en_cuarentena")
              .when(F.col("_prod").isNull(), "producto_inexistente")
              .when(F.col("unit_price").isNull() | (F.col("unit_price") <= 0), "precio_invalido")
              .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "cantidad_invalida")
              .when(F.col("discount_pct").isNull() | (F.col("discount_pct") < 0)
                    | (F.col("discount_pct") > 1), "descuento_invalido"))
         .drop("_existe", "_ok", "_prod"))
    items_ok = i.filter("motivo IS NULL").drop("motivo")
    items_q  = i.filter("motivo IS NOT NULL")

    detalle = (items_ok
        .join(orders_ok.select("order_id", "customer_id", "country", "order_ts", "status", "channel"), "order_id")
        .withColumn("mes", F.date_format("order_ts", "yyyy-MM"))
        .withColumn("importe_neto",
            F.round(F.col("quantity") * F.col("unit_price") * (F.lit(1) - F.col("discount_pct")), 2)
             .cast("decimal(14,2)"))
        .select("line_id", "order_id", "customer_id", "product_id", "mes", "order_ts", "country",
                "channel", "category", "status", "quantity", "unit_price", "discount_pct", "importe_neto"))
    return orders_ok, orders_q, items_ok, items_q, detalle

orders_ok, orders_q, items_ok, items_q, ventas_detalle = validar(orders, items)
ventas_detalle = ventas_detalle.cache()

print("Pedidos en cuarentena:")
orders_q.show(truncate=False)
print("Líneas en cuarentena por motivo:")
items_q.groupBy("motivo").count().orderBy("motivo").show(truncate=False)
print("Detalle de ventas:", ventas_detalle.count(), "líneas")
ventas_detalle.show(5, truncate=False)


# ========================================================================================
# 4. Agregación con Spark SQL
#
# Ventas netas por mes, país, canal y categoría. Se agrega sobre `ventas_detalle`, que tiene una fila por línea
# (`line_id` único): como cada línea se unió con un solo pedido, un solo cliente y un solo producto, ningún importe se repite.
# Se excluyen los pedidos `cancelled`.
# ========================================================================================
def resumir(detalle):
    detalle.createOrReplaceTempView("ventas_detalle")
    return spark.sql("""
        SELECT mes, country, channel, category,
               COUNT(DISTINCT order_id) AS pedidos,
               COUNT(*)                 AS lineas,
               SUM(quantity)            AS unidades,
               SUM(importe_neto)        AS ventas_netas
        FROM ventas_detalle
        WHERE status <> 'cancelled'
        GROUP BY mes, country, channel, category
        ORDER BY mes, country, channel, category
    """)

resumen_mensual = resumir(ventas_detalle).cache()
print("Filas del resumen:", resumen_mensual.count())
resumen_mensual.show(10, truncate=False)

print("Ventas netas por mes:")
resumen_mensual.groupBy("mes").agg(F.sum("ventas_netas").alias("ventas_netas")).orderBy("mes").show()


# ========================================================================================
# 5. Salida en Parquet y controles de calidad
# ========================================================================================
def escribir(df, ruta):
    df.write.mode("overwrite").parquet(ruta)   # overwrite: reejecutar no duplica la salida

escribir(ventas_detalle, "output/ventas_detalle")
escribir(resumen_mensual.coalesce(1), "output/resumen_mensual")
escribir(orders_q, "output/cuarentena/orders")
escribir(items_q, "output/cuarentena/order_items")

for ruta in ["output/ventas_detalle", "output/resumen_mensual",
             "output/cuarentena/orders", "output/cuarentena/order_items"]:
    print(f"{ruta:32} {spark.read.parquet(ruta).count():>6} filas")

def controles(detalle, resumen, items_entrada, items_ok, items_q, etiqueta=""):
    print(f"===== Controles de calidad {etiqueta}=====")
    # Control 1: reconciliación. El total agregado = total del detalle válido (sin cancelados)
    total_detalle = detalle.filter("status <> 'cancelled'").agg(F.sum("importe_neto")).first()[0]
    total_resumen = resumen.agg(F.sum("ventas_netas")).first()[0]
    print(f"1) Reconciliación: detalle = {total_detalle} | resumen = {total_resumen} | "
          f"{'OK' if total_detalle == total_resumen else 'FALLA'}")
    assert total_detalle == total_resumen

    # Control 2: sin duplicar importes en los joins. 1 fila por line_id y mismas líneas que antes del join
    n, n_ids = detalle.count(), detalle.select("line_id").distinct().count()
    print(f"2) Unicidad: filas detalle = {n} | line_id distintos = {n_ids} | líneas válidas antes del join = "
          f"{items_ok.count()} | {'OK' if n == n_ids == items_ok.count() else 'FALLA'}")
    assert n == n_ids == items_ok.count()

    # Control 3: no se pierde ninguna fila en silencio
    entrada, validas, cuarentena = items_entrada.count(), items_ok.count(), items_q.count()
    print(f"3) Balance: líneas de entrada = {entrada} = válidas {validas} + cuarentena {cuarentena} | "
          f"{'OK' if entrada == validas + cuarentena else 'FALLA'}")
    assert entrada == validas + cuarentena

controles(spark.read.parquet("output/ventas_detalle"), spark.read.parquet("output/resumen_mensual"),
          items, items_ok, items_q)


# ========================================================================================
# 6. Reto: incremental idempotente
#
# El lote trae 100 pedidos nuevos (`O20001`–`O20100`, 200 líneas) y una actualización tardía de `O00005`.
#
# Estrategia (sin Delta Lake):
# 1. Unir el histórico limpio con el lote (`unionByName`).
# 2. Pedidos: quedarse con una fila por `order_id`, la de `updated_at` más reciente (`row_number` sobre una ventana).
# 3. Líneas: una fila por `line_id` (`dropDuplicates`).
# 4. Volver a validar y recalcular, y escribir con `mode("overwrite")`.
#
# Es idempotente: aplicar el mismo lote dos veces da exactamente el mismo resultado, porque la deduplicación
# por clave absorbe las filas repetidas y la escritura reemplaza la salida en vez de añadirle filas.
# ========================================================================================
orders_inc_raw = spark.read.json("datos/incremental/orders_incremental.jsonl")
items_inc_raw  = spark.read.json("datos/incremental/order_items_incremental.jsonl")
print("Lote:", orders_inc_raw.count(), "pedidos |", items_inc_raw.count(), "líneas")

# Mismas columnas y tipos que el histórico, y misma limpieza
lote_orders = limpiar_orders(orders_inc_raw.select(*[F.col(c).cast("string") for c in orders_raw.columns]))
lote_items  = limpiar_items(items_inc_raw.select(*[F.col(c).cast("string") for c in order_items_raw.columns]))

def aplicar_lote(orders_base, items_base, lote_orders, lote_items):
    ultima = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc_nulls_last())
    o = (orders_base.unionByName(lote_orders)
            .withColumn("_rn", F.row_number().over(ultima))
            .filter("_rn = 1").drop("_rn"))
    i = items_base.unionByName(lote_items).dropDuplicates(["line_id"])
    return o, i

# Primera aplicación del lote
orders_v1, items_v1 = aplicar_lote(orders, items, lote_orders, lote_items)
orders_v1, items_v1 = orders_v1.cache(), items_v1.cache()
# Reprocesar el MISMO lote sobre el resultado: no debe cambiar nada
orders_v2, items_v2 = aplicar_lote(orders_v1, items_v1, lote_orders, lote_items)

print(f"Pedidos: histórico {orders.count()} -> tras lote {orders_v1.count()} -> reprocesado {orders_v2.count()}")
print(f"Líneas:  histórico {items.count()} -> tras lote {items_v1.count()} -> reprocesado {items_v2.count()}")
assert orders_v1.count() == orders_v2.count() and orders_v1.exceptAll(orders_v2).count() == 0
assert items_v1.count() == items_v2.count() and items_v1.exceptAll(items_v2).count() == 0
assert orders_v1.groupBy("order_id").count().filter("count > 1").count() == 0
print("Idempotente: OK (reprocesar el lote no duplica ni cambia nada)")

print("Actualización tardía de O00005 (antes / después):")
orders.filter("order_id = 'O00005'").show(truncate=False)
orders_v1.filter("order_id = 'O00005'").show(truncate=False)

orders_ok_i, orders_q_i, items_ok_i, items_q_i, detalle_i = validar(orders_v1, items_v1)
resumen_i = resumir(detalle_i)

escribir(orders_v1, "output/incremental/orders_actual")
escribir(detalle_i, "output/incremental/ventas_detalle")
escribir(resumen_i.coalesce(1), "output/incremental/resumen_mensual")

controles(spark.read.parquet("output/incremental/ventas_detalle"),
          spark.read.parquet("output/incremental/resumen_mensual"),
          items_v1, items_ok_i, items_q_i, etiqueta="(con incremental) ")

print("Ventas netas por mes con el incremental:")
(spark.read.parquet("output/incremental/resumen_mensual")
      .groupBy("mes").agg(F.sum("ventas_netas").alias("ventas_netas")).orderBy("mes").show())


# ========================================================================================
# 7. Preguntas para la discusión
#
# - Granularidad y claves: clientes (`customer_id`), productos (`product_id`), pedidos (`order_id`), líneas (`line_id`). El hecho de ventas es la línea.
# - Corregir / rechazar / cuarentena: corregir cuando el valor bueno es evidente (`deliverd`); rechazar es descartar dejando constancia; cuarentena es apartar con su motivo para revisarlo (`output/cuarentena/`).
# - Transformaciones vs. acciones: `select`, `filter`, `withColumn`, `join`, `groupBy`, `union` son transformaciones (perezosas, solo construyen el plan). `count`, `show`, `first`, `collect` y `write` son acciones (ejecutan el plan).
# - Evitar multiplicar totales: dejar cada tabla con su clave única antes del join (ej.: `O00012` duplicado), agregar sobre la tabla de grano más fino (líneas) y comprobar que el número de filas no cambia tras el join (control 2).
# - Local vs. clúster: en `local[*]` todo corre en un proceso de una máquina; no hay red entre nodos, ni fallos de executors, ni datos que no quepan en un solo disco. Los tiempos no sirven como benchmark y algunos problemas (shuffles grandes, *skew*, memoria por executor) solo aparecen en un clúster.
# ========================================================================================
print("\nETL terminado. Resultados en output/")
spark.stop()
