# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb00 · Instellingen laden
# 
# Haalt de configuratietabellen uit de SQL-database `co2_config` en zet ze als
# `cfg_*` in `LH_SILVER`. Alle andere notebooks lezen die.
# 
# Waarom een SQL-database en niet een CSV: ZAS beheert de emissiefactoren zelf,
# via een Planning-blad of de SQL-editor, met per wijziging gebruiker en tijdstip
# en een blokkade op sets die in gebruik zijn. Dat is de audittrail en het
# versiebeheer op emissiefactoren die het bestek vraagt (K1-A).
# 
# Gevolg: een wijziging in `co2_config` telt pas mee nadat dit notebook opnieuw
# gedraaid heeft. In de dagelijkse pipeline is dit de eerste stap.

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

# Verbindingsgegevens: plak de door Fabric gegenereerde JDBC-url van co2_config.
import com.microsoft.sqlserver.jdbc.spark
SQL_URL = ("jdbc:sqlserver://4y2tynbcufpuhpi5ija6sbipra-gmgotrddhbsuflqdm3aqvd7c2q.database.fabric.microsoft.com:1433;database={co2_config-196fe50c-6814-4749-bc47-a2b9a4acb6ac};encrypt=true;trustServerCertificate=false")

TABELLEN = ["emissiedrager", "factorenset", "emissiefactor", "emissiefactor_historiek",
            "allocatieregel", "kostenplaats", "datacontract",
            "scenario", "scenarioparameter", "doelstelling"]

for t in TABELLEN:
    df = spark.read.option("url", SQL_URL).mssql(f"dbo.{t}")
    df = df.withColumn("_geladen_op", F.current_timestamp())
    df.createOrReplaceTempView(f"v_{t}")
    bewaar(f"v_{t}", ZILVER_T, f"cfg_{t}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Controle: precies één factorenset in gebruik, anders weet nb03 niet welke
# MAGIC -- factoren te nemen.
# MAGIC SELECT factorenset, status, datum, goedgekeurd_door
# MAGIC FROM v_factorenset ORDER BY factorenset;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
