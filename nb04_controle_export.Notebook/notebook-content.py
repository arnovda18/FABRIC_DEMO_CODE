# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb04 · Controles en export
# 
# Vijf controles die bewijzen wat de offerte belooft, en een export in open
# formaten (T6, UC28). Elke controle geeft `OK` of `FOUT` met het cijfer erbij;
# de pipeline faalt op een `FOUT`.

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

lees(GOUD_T, "emissiefeit"); lees(GOUD_T, "berekening"); lees(GOUD_T, "datakwaliteit")
lees(ZILVER_T, "verbruik"); lees(ZILVER_T, "activiteit"); lees(ZILVER_T, "traject")

fouten = []
def controle(naam, ok, detail):
    print(f"  {'OK  ' if ok else 'FOUT'} {naam:45s} {detail}")
    if not ok: fouten.append(naam)

basis = spark.sql("SELECT * FROM berekening WHERE type = 'basislijn' ORDER BY uitgevoerd_op DESC").collect()
laatste = basis[0]

# 1. Aantallen: elk traject en elke activiteit is er, en niets is gedupliceerd.
r = spark.sql("""SELECT (SELECT COUNT(*) FROM traject) AS trajecten,
                        (SELECT COUNT(*) FROM activiteit) AS activiteiten,
                        (SELECT COUNT(DISTINCT activiteit) FROM activiteit) AS unieke""").first()
controle("aantallen", r.activiteiten == r.unieke, f"{r.trajecten} trajecten, {r.activiteiten} activiteiten")

# 2. Reconciliatie: gemeten energie = toegewezen + restpost, per kWh.
r = spark.sql("""
    SELECT ROUND(SUM(CASE WHEN status = 'ok' THEN hoeveelheid END), 3) AS gemeten FROM verbruik WHERE soort = 'energie'""").first()
s = spark.sql(f"""
    SELECT ROUND(SUM(hoeveelheid * aandeel), 3) AS totaal,
           ROUND(SUM(CASE WHEN toewijzing = 'toegewezen' THEN hoeveelheid * aandeel END), 3) AS toegewezen
    FROM emissiefeit WHERE soort = 'energie' AND scenario IS NULL AND berekening = '{laatste.berekening}'""").first()
controle("reconciliatie energie", abs(r.gemeten - s.totaal) < 0.01,
         f"gemeten {r.gemeten:,.0f} kWh, toegewezen {s.toegewezen:,.0f} kWh ({100*s.toegewezen/r.gemeten:.1f} %), verschil {r.gemeten - s.totaal:.3f}")

# 3. Elke rij met factor 0 is gemarkeerd als geschat.
r = spark.sql("SELECT COUNT(*) AS n FROM emissiefeit WHERE factor = 0 AND geschat = 'nee'").first()
controle("factor 0 altijd geschat", r.n == 0, f"{r.n} overtredingen")

# 4. Reproduceerbaarheid: de laatste basislijn geeft hetzelfde totaal als de vorige met dezelfde factorenset en codeversie.
vorige = [b for b in basis[1:] if b.factorenset == laatste.factorenset and b.codeversie == laatste.codeversie]
if vorige:
    controle("reproduceerbaarheid", abs(float(vorige[0].totaal_kg) - float(laatste.totaal_kg)) < 0.01,
             f"{vorige[0].berekening} {vorige[0].totaal_kg} kg vs {laatste.berekening} {laatste.totaal_kg} kg")
else:
    print("  --   reproduceerbaarheid: eerste run met deze factorenset en codeversie, geen vergelijking")

# 5. Elke feitrij wijst naar een bronrij en een berekening.
r = spark.sql("SELECT COUNT(*) AS n FROM emissiefeit WHERE bron IS NULL OR berekening IS NULL").first()
controle("lineage volledig", r.n == 0, f"{r.n} rijen zonder bron of berekening")

print()
spark.sql(f"""SELECT zorgpad, COUNT(DISTINCT traject) AS trajecten,
                     ROUND(SUM(kg_co2e) / COUNT(DISTINCT traject), 1) AS kg_per_traject
              FROM emissiefeit WHERE scenario IS NULL AND traject IS NOT NULL AND berekening = '{laatste.berekening}'
              GROUP BY zorgpad ORDER BY zorgpad""").show()
if fouten:
    raise Exception(f"Controles gefaald: {fouten}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Export
# 
# Alles wat ZAS nodig heeft om zonder ons verder te kunnen: de feittabel en
# dimensies als Parquet, de configuratie en het berekeningsregister als CSV. Het
# ontwerpdocument beschrijft de formule; met deze bestanden reproduceert een
# derde partij elk cijfer.

# CELL ********************

from datetime import datetime, timezone
EXPORT = f"{GOUD_F}/export/{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M')}"

PARQUET = ["emissiefeit", "berekening", "dim_traject", "dim_activiteit", "dim_item", "dim_locatie",
           "dim_zorgpadstap", "dim_emissiedrager", "dim_datum", "datakwaliteit"]
CSV = ["cfg_emissiefactor", "cfg_factorenset", "cfg_allocatieregel", "cfg_scenario",
       "cfg_scenarioparameter", "cfg_doelstelling"]

for t in PARQUET:
    spark.read.format(FORMAAT).load(f"{GOUD_T}/{t}").coalesce(1).write.mode("overwrite").parquet(f"{EXPORT}/{t}")
for t in CSV:
    (spark.read.format(FORMAAT).load(f"{GOUD_T}/{t}").coalesce(1).write.mode("overwrite")
       .option("header", True).option("sep", ";").csv(f"{EXPORT}/{t}"))
print("Export klaar in", EXPORT)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
