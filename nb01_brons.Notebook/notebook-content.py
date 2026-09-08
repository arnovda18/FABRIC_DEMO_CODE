# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb01 · Brons inladen
# 
# Zet de aangeleverde bestanden ongewijzigd om in tabellen in `LH_BRONZE`. Alles
# blijft tekst: er wordt niets omgerekend, hernoemd of geraden. Per rij komen er
# vier technische kolommen bij die de herkomst vastleggen.
# 
# Twee redenen om brons bronvormig te houden en niet al te modelleren:
# 1. Blijkt een koppeling achteraf fout, dan herrekenen we zonder ZAS om een
#    nieuwe export te vragen (herlaadbaarheid, T2).
# 2. Elke feitrij in goud wijst terug naar `bestand#rij` in brons. Dat is de
#    lineage tot de bronrij (F3, UC29), door ontwerp en niet door een tool.
# 
# Waarom geen `inferSchema`: die maakt van `2026-04` een timestamp en van `2,5`
# een getal. Dat lijkt handig en is het niet: een join in nb03 matcht dan niet
# en een hele emissiedrager verdwijnt zonder foutmelding. Types worden in nb02
# expliciet gezet, op één plek.
# 
# Elk bestand wordt vergeleken met het datacontract in `cfg_datacontract`.
# Ontbreekt een verplichte kolom, dan wordt de levering geweigerd. Is er een
# kolom te veel, dan wordt ze geladen en gemeld.


# CELL ********************

# ===========================================================================
# OPZET — identiek in elk notebook. Vul de vier GUID's eenmalig in.
#
# Alles loopt via volledige OneLake-paden. Geen standaard-lakehouse, geen
# catalognamen: een relatief pad werkt alleen voor het standaard-lakehouse en
# een catalogverwijzing struikelt over de spaties en blokhaken in
# "[PRD][DATA] ZAS_DEMO". Met paden heb je geen van beide nodig.
#
#   workspace : browserbalk, het stuk na /groups/
#   lakehouse : open het lakehouse, het stuk na /lakehouses/
# ===========================================================================
WS_ID     = "c4e90c33-3863-4265-ae03-66c10a8fe2d4"                  # [PRD][DATA] ZAS_DEMO — browserbalk, het stuk na /groups/
BRONZE_ID = "0877a091-56e9-457b-809a-13220d2239b5"   # LH_BRONZE
SILVER_ID = "d3edf0f9-59d3-4ad9-9ffa-95d5c66f00a1"   # LH_SILVER
GOLD_ID   = "03986817-d159-4ea1-b170-d05487dc3876"   # LH_GOLD

CODEVERSIE = "2.0.0"   # versie van de berekeningslogica; komt in elk berekeningsregister

import notebookutils
from pyspark.sql import functions as F

if WS_ID.startswith("VUL-"):
    raise Exception("Vul WS_ID in: de GUID van de workspace [PRD][DATA] ZAS_DEMO (browserbalk, na /groups/).")

ONELAKE = f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com"
FORMAAT = "delta"

def _tabelmap(lh_id, naam):
    """Tabellen staan onder Tables/ of, bij een lakehouse met schema's, onder
    Tables/dbo/. We kijken gewoon wat er staat."""
    basis = f"{ONELAKE}/{lh_id}/Tables"
    try:
        namen = [f.name.strip("/") for f in notebookutils.fs.ls(basis)]
    except Exception as e:
        raise Exception(f"Kan {basis} niet lezen. Klopt de GUID van {naam}? {e}")
    return f"{basis}/dbo" if "dbo" in namen else basis

BRONS_F  = f"{ONELAKE}/{BRONZE_ID}/Files"
GOUD_F   = f"{ONELAKE}/{GOLD_ID}/Files"
BRONS_T  = _tabelmap(BRONZE_ID, "LH_BRONZE")
ZILVER_T = _tabelmap(SILVER_ID, "LH_SILVER")
GOUD_T   = _tabelmap(GOLD_ID,   "LH_GOLD")

def bestaat(pad, tabel):
    try:
        spark.read.format(FORMAAT).load(f"{pad}/{tabel}").limit(1).count()
        return True
    except Exception:
        return False

def lees(pad, tabel, alias=None):
    """Registreert een tabel als tijdelijke weergave, zodat SQL erbij kan
    zonder catalog- of lakehousenaam."""
    naam = alias or tabel
    spark.read.format(FORMAAT).load(f"{pad}/{tabel}").createOrReplaceTempView(naam)
    return naam

def bewaar(weergave, pad, tabel, modus="overwrite"):
    """Schrijft een weergave weg als tabel. Bij 'append' worden de kolommen in
    de volgorde van de doeltabel gezet, zodat een schema-mismatch niet stil
    voorbijgaat."""
    df = spark.table(weergave)
    if modus == "append" and bestaat(pad, tabel):
        doel = spark.read.format(FORMAAT).load(f"{pad}/{tabel}").columns
        ontbreekt = set(doel) - set(df.columns)
        if ontbreekt:
            raise Exception(f"{tabel}: kolommen ontbreken in {weergave}: {sorted(ontbreekt)}")
        df = df.select(*doel)
        df.write.format(FORMAAT).mode("append").save(f"{pad}/{tabel}")
    else:
        (df.write.format(FORMAAT).mode("overwrite")
           .option("overwriteSchema", "true").save(f"{pad}/{tabel}"))
    print(f"  {tabel:28s} {df.count():>8,} rijen  ({modus})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timezone
from pyspark.sql import Window

LANDING  = f"{BRONS_F}/landing"
CODERING = "utf-8"          # ZAS levert Windows-1252 (ICT-richtlijnen §6); dan "windows-1252"
SCHEIDER = ";"
LEVERING = datetime.now(timezone.utc).strftime("L-%Y%m%d-%H%M")

lees(ZILVER_T, "cfg_datacontract")
contract = spark.sql("""
    SELECT bronsysteem, bestand, tabel,
           COLLECT_SET(kolom) AS kolommen
    FROM cfg_datacontract WHERE actief = TRUE
    GROUP BY bronsysteem, bestand, tabel
""").collect()

print(f"Levering {LEVERING}: {len(contract)} bestanden verwacht in {LANDING}\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

leveringen = []

for c in contract:
    pad = f"{LANDING}/{c.bestand}"
    try:
        df = (spark.read.option("header", True).option("sep", SCHEIDER)
              .option("encoding", CODERING).option("inferSchema", False).csv(pad))
    except Exception as e:
        leveringen.append((LEVERING, c.bronsysteem, c.bestand, c.tabel, 0, None, None, "ontbreekt", str(e)[:200]))
        print(f"  {c.tabel:20s} ONTBREEKT")
        continue

    werkelijk = set(df.columns)
    verwacht  = set(c.kolommen)
    ontbreekt = sorted(verwacht - werkelijk)
    extra     = sorted(werkelijk - verwacht)

    if ontbreekt:
        status = "geweigerd"
    elif extra:
        status = "gewaarschuwd"
    else:
        status = "ok"

    n = df.count()
    if status != "geweigerd":
        # Vier technische kolommen. _rij is het rijnummer in het bestand en vormt
        # samen met de bestandsnaam de bronverwijzing die tot in goud meegaat.
        df = (df.withColumn("_rij", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
                .withColumn("_bestand", F.lit(c.bestand))
                .withColumn("_levering", F.lit(LEVERING))
                .withColumn("_ingeladen_op", F.current_timestamp()))
        df.createOrReplaceTempView(f"v_{c.tabel}")
        bewaar(f"v_{c.tabel}", BRONS_T, c.tabel)

    leveringen.append((LEVERING, c.bronsysteem, c.bestand, c.tabel, n,
                       ",".join(extra) or None, ",".join(ontbreekt) or None, status, None))
    if status != "ok":
        print(f"  {c.tabel:20s} {status.upper()}  extra={extra} ontbreekt={ontbreekt}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Leveringsregister. Het rijaantal per bestand gaat terug naar de data-eigenaar
# bij ZAS ter bevestiging: een half aangeleverd bestand geeft een cijfer dat er
# plausibel uitziet.
from pyspark.sql.types import StructType, StructField, StringType, LongType
schema = StructType([StructField(k, LongType() if k == "rijen" else StringType())
                     for k in ["levering", "bronsysteem", "bestand", "tabel", "rijen",
                               "kolommen_extra", "kolommen_ontbrekend", "status", "fout"]])
(spark.createDataFrame(leveringen, schema)
      .withColumn("ingeladen_op", F.current_timestamp())
      .createOrReplaceTempView("v_levering"))

bewaar("v_levering", ZILVER_T, "levering", "append")
spark.sql("SELECT bronsysteem, tabel, rijen, status, kolommen_extra, kolommen_ontbrekend FROM v_levering ORDER BY bronsysteem, tabel").show(30, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
