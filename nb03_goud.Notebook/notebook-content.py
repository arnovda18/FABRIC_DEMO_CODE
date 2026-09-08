# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb03 · Goud berekenen
# 
# De rekenmotor. Eén formule voor alle emissiedragers:
# 
#     kg CO2e = hoeveelheid × aandeel × emissiefactor
# 
# `hoeveelheid` uit `verbruik`, `aandeel` uit het allocatiepatroon, de factor uit
# het register. Er is geen aparte berekening voor energie, materiaal of
# medicatie. Daarom is een nieuw zorgpad configuratie en geen code.
# 
# Hetzelfde notebook draait de basislijn en elk scenario; het enige verschil is
# de parameter `SCENARIO`. De basislijn schrijft de feittabel opnieuw, een
# scenario voegt rijen toe met een scenariolabel.

# PARAMETERS CELL ********************

# PARAMETERCEL — in Fabric markeren als "Parameter cell", dan geeft de pipeline
# hier waarden aan mee.
SCENARIO    = ""     # leeg = basislijn; anders bv. "S-001"
FACTORENSET = ""     # leeg = de set met status 'in gebruik'; anders bv. "2026.1" voor een herberekening

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

for t in ["verbruik", "activiteit", "traject", "locatie", "item", "zorgpadstap", "datakwaliteit"]:
    lees(ZILVER_T, t, f"sv_{t}")
for t in ["cfg_emissiefactor", "cfg_factorenset", "cfg_allocatieregel", "cfg_emissiedrager",
          "cfg_scenario", "cfg_scenarioparameter", "cfg_doelstelling"]:
    lees(ZILVER_T, t)
for t in ["ref_bezetting", "ref_ingreepvolume"]:
    lees(BRONS_T, t, f"br_{t}")

if not FACTORENSET:
    sets = [r.factorenset for r in spark.sql("SELECT factorenset FROM cfg_factorenset WHERE status = 'in gebruik'").collect()]
    if len(sets) != 1:
        raise Exception(f"Verwacht precies één factorenset in gebruik, gevonden: {sets}")
    FACTORENSET = sets[0]

BEREKENING = datetime.now(timezone.utc).strftime("B-%Y%m%d-%H%M") + (f"-{SCENARIO}" if SCENARIO else "")

# De parameters als weergave, zodat de SQL hieronder ze kan opvragen.
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW parameters AS
    SELECT '{SCENARIO}' AS scenario, '{FACTORENSET}' AS factorenset,
           '{BEREKENING}' AS berekening, '{CODEVERSIE}' AS codeversie
""")
print(f"Berekening  {BEREKENING}\nFactorenset {FACTORENSET}\nScenario    {SCENARIO or '(basislijn)'}\nCode        {CODEVERSIE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1 · Factoren, met scenario-overrides van het soort `factor`
# 
# Een scenario wijzigt het register niet. Het legt tijdelijk een afwijking
# overheen; `cfg_emissiefactor` blijft ongemoeid en de basislijn kan er nooit
# door besmet raken.

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW factoren AS
# MAGIC SELECT f.item,
# MAGIC        COALESCE(o.waarde, CAST(f.waarde AS DOUBLE)) AS factor,
# MAGIC        f.scope, f.herkomst, f.betrouwbaarheid
# MAGIC FROM cfg_emissiefactor f
# MAGIC LEFT JOIN (SELECT item, CAST(waarde AS DOUBLE) AS waarde FROM cfg_scenarioparameter
# MAGIC            WHERE soort = 'factor' AND scenario = (SELECT scenario FROM parameters)) o
# MAGIC        ON f.item = o.item
# MAGIC WHERE f.factorenset = (SELECT factorenset FROM parameters);
# MAGIC 
# MAGIC SELECT COUNT(*) AS factoren FROM factoren;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2 · Verbruik, met scenario-overrides van het soort `vervanging`
# 
# De simulatiecase uit het bestek: een alternatief implantaat. Een vervanging
# splitst elke verbruiksregel van het item in twee: het oude item met
# `1 − aandeel`, het nieuwe met `aandeel`, en de massa van het nieuwe artikel.
# Alles daarna, allocatie en vermenigvuldiging, is identiek aan de basislijn.
# Dat is wat reproduceerbaar en transparant betekent: één motor.

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW vervanging AS
# MAGIC SELECT p.item, p.vervang_door, CAST(p.waarde AS DOUBLE) AS aandeel,
# MAGIC        n.massa_kg / o.massa_kg AS massaverhouding
# MAGIC FROM cfg_scenarioparameter p
# MAGIC JOIN sv_item o ON p.item = o.item
# MAGIC JOIN sv_item n ON p.vervang_door = n.item
# MAGIC WHERE p.soort = 'vervanging' AND p.scenario = (SELECT scenario FROM parameters);
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW verbruik_basis AS
# MAGIC SELECT v.verbruik, v.soort, v.item, v.hoeveelheid, v.eenheid, v.activiteit, v.zone, v.campus,
# MAGIC        v.periode, v.regel, v.koppeling, v.geschat, v.bronsleutel
# MAGIC FROM sv_verbruik v LEFT JOIN vervanging x ON v.item = x.item
# MAGIC WHERE v.status = 'ok' AND x.item IS NULL
# MAGIC UNION ALL
# MAGIC SELECT v.verbruik, v.soort, v.item, v.hoeveelheid * (1 - x.aandeel), v.eenheid, v.activiteit, v.zone, v.campus,
# MAGIC        v.periode, v.regel, v.koppeling, v.geschat, v.bronsleutel
# MAGIC FROM sv_verbruik v JOIN vervanging x ON v.item = x.item WHERE v.status = 'ok'
# MAGIC UNION ALL
# MAGIC SELECT CONCAT(v.verbruik, '~'), v.soort, x.vervang_door, v.hoeveelheid * x.aandeel * x.massaverhouding, v.eenheid,
# MAGIC        v.activiteit, v.zone, v.campus, v.periode, v.regel, v.koppeling, v.geschat, v.bronsleutel
# MAGIC FROM sv_verbruik v JOIN vervanging x ON v.item = x.item WHERE v.status = 'ok';
# MAGIC 
# MAGIC -- Het patroon komt uit de configuratie. Een regel zonder actief patroon wordt
# MAGIC -- niet toegewezen en blijft zichtbaar in de restpost.
# MAGIC CREATE OR REPLACE TEMP VIEW verbruik_ok AS
# MAGIC SELECT v.*, r.patroon, r.vast_variabel
# MAGIC FROM verbruik_basis v
# MAGIC LEFT JOIN cfg_allocatieregel r ON v.regel = r.regel AND r.actief = TRUE;
# MAGIC 
# MAGIC SELECT regel, patroon, COUNT(*) AS rijen FROM verbruik_ok GROUP BY regel, patroon ORDER BY regel;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3 · De noemers
# 
# Hier gaat het in de praktijk het vaakst mis. Een operatiezaal draait veel meer
# dan protheses. Deel je de zone-energie door alleen de ingrepen die wij
# modelleren, dan wijs je een factor tien te veel toe. De noemer is daarom de
# werkelijke bezetting die ZAS aanlevert. Zones met bezettingstype
# `niet_alloceerbaar` krijgen geen noemer en blijven volledig in de restpost.

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW noemer_zone AS
# MAGIC SELECT locatiecode AS zone, SUBSTRING(maand, 1, 7) AS periode,
# MAGIC        CAST(REPLACE(noemer_waarde, ',', '.') AS DOUBLE) AS noemer
# MAGIC FROM br_ref_bezetting WHERE noemer_type = 'zoneminuten';
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW noemer_campus AS
# MAGIC SELECT campuscode AS campus, SUBSTRING(maand, 1, 7) AS periode, CAST(totaal_ingrepen AS DOUBLE) AS noemer
# MAGIC FROM br_ref_ingreepvolume;
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW ok_activiteit AS
# MAGIC SELECT * FROM sv_activiteit WHERE fase = 'peroperatief';
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW noemer_campusdag AS
# MAGIC SELECT campus, datum AS periode, COUNT(*) AS noemer FROM ok_activiteit GROUP BY campus, datum;
# MAGIC 
# MAGIC SELECT 'zone' AS noemer, COUNT(*) AS rijen FROM noemer_zone
# MAGIC UNION ALL SELECT 'campus', COUNT(*) FROM noemer_campus
# MAGIC UNION ALL SELECT 'campus-dag', COUNT(*) FROM noemer_campusdag;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4 · De vier allocatiepatronen
# 
# Het scharnier van het platform. De motor kent geen regelnamen, alleen vier
# patronen; welke regel welk patroon gebruikt staat in `cfg_allocatieregel`. Een
# nieuwe zorgcontext voegt regels toe met een bestaand patroon en raakt dit
# notebook niet aan.
# 
# | patroon | aandeel | noemer |
# |---|---|---|
# | direct | 1 | — |
# | zone-noemer | duur_min ÷ zoneminuten | bezetting van de zone in die maand |
# | campus-noemer | 1 ÷ totaal ingrepen | alle ingrepen op de campus in die maand |
# | campus-dag | 1 ÷ n | onze operaties op de campus op die dag |

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW toegewezen AS
# MAGIC 
# MAGIC SELECT v.verbruik, v.activiteit, 1.0 AS aandeel
# MAGIC FROM verbruik_ok v
# MAGIC WHERE v.patroon = 'direct' AND v.activiteit IS NOT NULL
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT v.verbruik, a.activiteit, a.duur_min / n.noemer
# MAGIC FROM verbruik_ok v
# MAGIC JOIN sv_activiteit a ON v.zone = a.zone AND v.periode = a.maand
# MAGIC JOIN noemer_zone   n ON v.zone = n.zone AND v.periode = n.periode
# MAGIC WHERE v.patroon = 'zone-noemer'
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT v.verbruik, a.activiteit, 1.0 / n.noemer
# MAGIC FROM verbruik_ok v
# MAGIC JOIN ok_activiteit a ON v.campus = a.campus AND v.periode = a.maand
# MAGIC JOIN noemer_campus n ON v.campus = n.campus AND v.periode = n.periode
# MAGIC WHERE v.patroon = 'campus-noemer'
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT v.verbruik, a.activiteit, 1.0 / n.noemer
# MAGIC FROM verbruik_ok v
# MAGIC JOIN ok_activiteit    a ON v.campus = a.campus AND v.periode = a.datum
# MAGIC JOIN noemer_campusdag n ON v.campus = n.campus AND v.periode = n.periode
# MAGIC WHERE v.patroon = 'campus-dag';
# MAGIC 
# MAGIC SELECT v.patroon, COUNT(*) AS rijen, ROUND(SUM(t.aandeel), 2) AS som_aandeel
# MAGIC FROM toegewezen t JOIN verbruik_ok v ON t.verbruik = v.verbruik GROUP BY v.patroon;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5 · De restpost
# 
# Wat niet aan een traject toewijsbaar is, verdwijnt niet. Het krijgt een eigen
# rij met een leeg traject. Daardoor sluit de optelling per zone en campus op
# het metercijfer, en ziet de lezer precies hoeveel het model wél toewijst. Dat
# is het verschil tussen een model dat een auditor vertrouwt en een dat hij niet
# vertrouwt.

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW alles AS
# MAGIC SELECT verbruik, activiteit, aandeel, 'toegewezen' AS toewijzing FROM toegewezen
# MAGIC UNION ALL
# MAGIC SELECT v.verbruik, CAST(NULL AS STRING), GREATEST(0.0, 1.0 - COALESCE(s.som, 0.0)), 'restpost'
# MAGIC FROM verbruik_ok v
# MAGIC LEFT JOIN (SELECT verbruik, SUM(aandeel) AS som FROM toegewezen GROUP BY verbruik) s ON v.verbruik = s.verbruik
# MAGIC WHERE GREATEST(0.0, 1.0 - COALESCE(s.som, 0.0)) > 0.000000001;
# MAGIC 
# MAGIC SELECT toewijzing, COUNT(*) AS rijen FROM alles GROUP BY toewijzing;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6 · De vermenigvuldiging
# 
# Eén regel. Alles hierboven bepaalde het aandeel.
# 
# Ontbreekt een factor, dan rekenen we met nul en markeren we de rij als
# geschat: de post verdwijnt niet stilletjes, maar telt ook niet mee met een
# verzonnen getal.

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW gd_emissiefeit AS
# MAGIC SELECT x.verbruik,
# MAGIC        x.activiteit,
# MAGIC        a.traject,
# MAGIC        COALESCE(a.zorgpad, t.zorgpad)             AS zorgpad,
# MAGIC        a.stap, a.fase,
# MAGIC        COALESCE(a.zone, v.zone)                   AS zone,
# MAGIC        COALESCE(a.campus, v.campus)               AS campus,
# MAGIC        COALESCE(a.maand, SUBSTRING(v.periode, 1, 7)) AS maand,
# MAGIC        v.soort, v.item, v.hoeveelheid, v.eenheid,
# MAGIC        x.aandeel,
# MAGIC        COALESCE(f.factor, 0.0)                    AS factor,
# MAGIC        ROUND(v.hoeveelheid * x.aandeel * COALESCE(f.factor, 0.0), 6) AS kg_co2e,
# MAGIC        CASE WHEN f.factor IS NULL OR v.geschat = 'ja' THEN 'ja' ELSE 'nee' END AS geschat,
# MAGIC        f.scope, f.betrouwbaarheid,
# MAGIC        v.regel, v.koppeling, x.toewijzing,
# MAGIC        -- UC11: wat aan een activiteit hangt, beweegt mee met de activiteitsgraad; de restpost is de vaste last.
# MAGIC        CASE WHEN x.toewijzing = 'restpost' THEN 'vast' ELSE COALESCE(v.vast_variabel, 'variabel') END AS vast_variabel,
# MAGIC        v.verbruik                                 AS bron,          -- bestand#rij in brons
# MAGIC        v.bronsleutel,
# MAGIC        NULLIF((SELECT scenario FROM parameters), '') AS scenario,
# MAGIC        (SELECT factorenset FROM parameters)       AS factorenset,
# MAGIC        (SELECT berekening  FROM parameters)       AS berekening
# MAGIC FROM alles x
# MAGIC JOIN verbruik_ok v        ON x.verbruik = v.verbruik
# MAGIC LEFT JOIN factoren f      ON v.item = f.item
# MAGIC LEFT JOIN sv_activiteit a ON x.activiteit = a.activiteit
# MAGIC LEFT JOIN sv_traject t    ON a.traject = t.traject;
# MAGIC 
# MAGIC SELECT soort, toewijzing, COUNT(*) AS rijen, ROUND(SUM(kg_co2e), 1) AS kg_co2e
# MAGIC FROM gd_emissiefeit GROUP BY soort, toewijzing ORDER BY soort, toewijzing;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Wegschrijven. Basislijn vervangt de feittabel; een scenario voegt toe.
modus = "append" if SCENARIO else "overwrite"
print("Wegschrijven naar", GOUD_T, "\n")
bewaar("gd_emissiefeit", GOUD_T, "emissiefeit", modus)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7 · Het berekeningsregister
# 
# Wanneer, met welke factorenset, welk scenario, welke codeversie, en de totalen.
# Dit is het volledige versiebeheer dat een gebruiker moet kennen: één
# berekeningsnummer en elk cijfer is te reconstrueren.

# CELL ********************

spark.sql("""
    CREATE OR REPLACE TEMP VIEW gd_berekening AS
    SELECT (SELECT berekening FROM parameters)                            AS berekening,
           CASE WHEN (SELECT scenario FROM parameters) = '' THEN 'basislijn' ELSE 'scenario' END AS type,
           NULLIF((SELECT scenario FROM parameters), '')                  AS scenario,
           (SELECT factorenset FROM parameters)                           AS factorenset,
           (SELECT codeversie  FROM parameters)                           AS codeversie,
           CURRENT_TIMESTAMP()                                            AS uitgevoerd_op,
           COUNT(*)                                                       AS rijen,
           ROUND(SUM(kg_co2e), 2)                                         AS totaal_kg,
           ROUND(SUM(CASE WHEN toewijzing = 'toegewezen' THEN kg_co2e ELSE 0 END), 2) AS toegewezen_kg,
           ROUND(SUM(CASE WHEN toewijzing = 'restpost'   THEN kg_co2e ELSE 0 END), 2) AS restpost_kg,
           ROUND(SUM(CASE WHEN geschat = 'ja' THEN kg_co2e ELSE 0 END) / NULLIF(SUM(kg_co2e), 0), 4) AS aandeel_geschat
    FROM gd_emissiefeit
""")
bewaar("gd_berekening", GOUD_T, "berekening", "append")
lees(GOUD_T, "berekening", "reg")
spark.sql("SELECT * FROM reg ORDER BY uitgevoerd_op DESC").show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8 · Het stermodel
# 
# Alleen bij de basislijn: de dimensies en de configuratie gaan mee naar goud,
# zodat het semantische model uitsluitend uit `LH_GOLD` leest.

# CELL ********************

if SCENARIO:
    print("Scenariorun: dimensies blijven ongewijzigd.")
else:
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW dim_datum AS
        SELECT maand, TO_DATE(CONCAT(maand, '-01')) AS datum,
               CAST(SUBSTRING(maand, 1, 4) AS INT) AS jaar,
               CAST(SUBSTRING(maand, 6, 2) AS INT) AS maandnr,
               CONCAT('K', CAST(CEIL(CAST(SUBSTRING(maand, 6, 2) AS INT) / 3.0) AS INT)) AS kwartaal
        FROM (SELECT DISTINCT maand FROM gd_emissiefeit WHERE maand IS NOT NULL)""")
    spark.sql("CREATE OR REPLACE TEMP VIEW dim_traject AS SELECT traject, zorgpad, patient, campus, opnamedatum, ontslagdatum, ligdagen, bron FROM sv_traject")
    spark.sql("CREATE OR REPLACE TEMP VIEW dim_item AS SELECT item, soort, omschrijving, massa_kg FROM sv_item")
    spark.sql("CREATE OR REPLACE TEMP VIEW dim_locatie AS SELECT zone, campus, campusnaam, type, m2, submeter FROM sv_locatie")
    spark.sql("CREATE OR REPLACE TEMP VIEW dim_zorgpadstap AS SELECT zorgpad, volgnr, stap, fase, verrichtingcode FROM sv_zorgpadstap")
    spark.sql("CREATE OR REPLACE TEMP VIEW dim_emissiedrager AS SELECT drager, scope_indicatief, omschrijving, in_pilootscope FROM cfg_emissiedrager")
    spark.sql("CREATE OR REPLACE TEMP VIEW dim_activiteit AS SELECT activiteit, traject, zorgpad, volgnr, stap, fase, zone, campus, datum, maand, duur_min, bron FROM sv_activiteit")

    print("Stermodel wegschrijven naar", GOUD_T, "\n")
    for w, t in [("dim_datum", "dim_datum"), ("dim_traject", "dim_traject"), ("dim_activiteit", "dim_activiteit"),
                 ("dim_item", "dim_item"), ("dim_locatie", "dim_locatie"),
                 ("dim_zorgpadstap", "dim_zorgpadstap"), ("dim_emissiedrager", "dim_emissiedrager"),
                 ("cfg_emissiefactor", "cfg_emissiefactor"), ("cfg_factorenset", "cfg_factorenset"),
                 ("cfg_doelstelling", "cfg_doelstelling"), ("cfg_scenario", "cfg_scenario"),
                 ("cfg_scenarioparameter", "cfg_scenarioparameter"), ("cfg_allocatieregel", "cfg_allocatieregel"),
                 ("sv_datakwaliteit", "datakwaliteit")]:
        bewaar(w, GOUD_T, t)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Relaties in het semantische model
# 
# Veel-op-één, enkele richting, van de feittabel naar de dimensie:
# 
# | van `emissiefeit` | naar |
# |---|---|
# | traject | dim_traject[traject] |
# | activiteit | dim_activiteit[activiteit] |
# | item | dim_item[item] |
# | zone | dim_locatie[zone] |
# | maand | dim_datum[maand] |
# | soort | dim_emissiedrager[drager] |
# | berekening | berekening[berekening] |
# 
# `cfg_doelstelling` staat los; koppel in DAX met `SELECTEDVALUE` op zorgpad.
# Verberg op de feittabel de kolommen die ook op een dimensie staan (zorgpad,
# campus, stap).
