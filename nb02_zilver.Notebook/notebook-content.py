# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb02 · Zilver modelleren
# 
# Van twaalf brontabellen naar zeven modeltabellen die niet meer verraden uit
# welk systeem ze komen. Hier zit de systeemonafhankelijkheid (T1, T3, T5).
# 
# De kern is `verbruik`: zes bronnen krijgen dezelfde dertien kolommen. Daardoor
# volstaat in nb03 één berekening in plaats van zes.
# 
# Alle typeconversies staan hier, en nergens anders. Brons is tekst; vanaf zilver
# zijn getallen getallen en datums tekst in vaste vorm: maand `2026-04`, dag
# `2026-04-12`.

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
WS_ID     = "c4e90c33-3863-4265-ae03-66c10a8fe2d4"                 # [PRD][DATA] ZAS_DEMO — browserbalk, het stuk na /groups/
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

BRONTABELLEN = ["hix_opname", "hix_verrichting", "hix_medicatie",
                "erp_artikel", "erp_verbruik", "bms_meting", "tms_rit", "cssd_cyclus",
                "ref_locatie", "ref_zorgpadstap", "ref_bezetting", "ref_ingreepvolume"]
for t in BRONTABELLEN:
    lees(BRONS_T, t, f"br_{t}")
for t in ["cfg_emissiedrager", "cfg_emissiefactor", "cfg_factorenset", "cfg_kostenplaats"]:
    lees(ZILVER_T, t)
lees(ZILVER_T, "levering")
print("Bron- en configuratietabellen beschikbaar als weergaven.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1 · Structuur: locatie en zorgpadstap
# 
# Twee kleine tabellen die het hele flexibiliteitsverhaal dragen. Een campus,
# zone of zorgpad toevoegen is hier rijen bijzetten.
# 
# `zorgpadstap` is definitie, geen patiëntdata. De fase bepaalt straks welke
# activiteiten als operatie tellen: `peroperatief`. Er staat dus nergens een
# verrichtingcode in de code.

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW sv_locatie AS
# MAGIC SELECT locatiecode          AS zone,
# MAGIC        campuscode           AS campus,
# MAGIC        campusnaam,
# MAGIC        type,
# MAGIC        CAST(m2 AS INT)      AS m2,
# MAGIC        submeter                              -- ja = gemeten, nee = door het BMS verdeeld
# MAGIC FROM br_ref_locatie;
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW sv_zorgpadstap AS
# MAGIC SELECT zorgpad, CAST(volgnr AS INT) AS volgnr, stap, fase, verrichtingcode
# MAGIC FROM br_ref_zorgpadstap;
# MAGIC 
# MAGIC SELECT zorgpad, COUNT(*) AS stappen, SUM(CASE WHEN fase = 'peroperatief' THEN 1 ELSE 0 END) AS ok_stappen
# MAGIC FROM sv_zorgpadstap GROUP BY zorgpad;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2 · Traject en activiteit
# 
# Hier gaat definitie over in werkelijkheid. Een traject is één opname die één
# zorgpad doorloopt; het opnamenummer is de sleutel, dus een herhaalde run geeft
# dezelfde trajectnummers. Een activiteit is een stap die werkelijk plaatsvond.
# 
# De join op zorgpadstap gaat op verrichtingcode én zorgpad: de anesthesie-
# consultatie komt in meerdere zorgpaden voor.

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW sv_traject AS
# MAGIC SELECT opname_nr                                     AS traject,
# MAGIC        zorgprogramma                                 AS zorgpad,
# MAGIC        pat_pseudo                                    AS patient,
# MAGIC        campuscode                                    AS campus,
# MAGIC        SUBSTRING(opnamedatum, 1, 10)                 AS opnamedatum,
# MAGIC        SUBSTRING(ontslagdatum, 1, 10)                AS ontslagdatum,
# MAGIC        CAST(REPLACE(ligdagen, ',', '.') AS DOUBLE)   AS ligdagen,
# MAGIC        CONCAT(_bestand, '#', _rij)                   AS bron
# MAGIC FROM br_hix_opname;
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW sv_activiteit AS
# MAGIC SELECT CONCAT('A-', v.bronrij_id)                    AS activiteit,
# MAGIC        t.traject,
# MAGIC        t.zorgpad,
# MAGIC        s.volgnr,
# MAGIC        s.stap,
# MAGIC        s.fase,
# MAGIC        v.locatiecode                                 AS zone,
# MAGIC        t.campus,
# MAGIC        SUBSTRING(v.start, 1, 10)                     AS datum,
# MAGIC        SUBSTRING(v.start, 1, 7)                      AS maand,
# MAGIC        CAST(v.duur_min AS INT)                       AS duur_min,
# MAGIC        NULLIF(v.implantaat_lot, '')                  AS implantaat_lot,
# MAGIC        v.verrichtingcode,
# MAGIC        CONCAT(v._bestand, '#', v._rij)               AS bron
# MAGIC FROM br_hix_verrichting v
# MAGIC LEFT JOIN sv_traject     t ON v.opname_nr = t.traject
# MAGIC LEFT JOIN sv_zorgpadstap s ON v.verrichtingcode = s.verrichtingcode AND t.zorgpad = s.zorgpad;
# MAGIC 
# MAGIC -- Operatiestappen: de stap met fase peroperatief. Drie verbruikstakken hangen hieraan.
# MAGIC CREATE OR REPLACE TEMP VIEW ok_activiteit AS
# MAGIC SELECT * FROM sv_activiteit WHERE fase = 'peroperatief';
# MAGIC 
# MAGIC SELECT COUNT(*) AS activiteiten,
# MAGIC        SUM(CASE WHEN stap IS NULL THEN 1 ELSE 0 END)     AS zonder_stap,
# MAGIC        SUM(CASE WHEN traject IS NULL THEN 1 ELSE 0 END)  AS zonder_traject,
# MAGIC        (SELECT COUNT(*) FROM ok_activiteit)              AS ok_activiteiten
# MAGIC FROM sv_activiteit;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3 · Item
# 
# Eén catalogus voor alles wat uitstoot veroorzaakt. Materiaal komt uit de
# artikelstam, medicatie uit het EPD, en de dragers zonder artikel (energie,
# sterilisatie, transport) uit `cfg_emissiedrager`. Nergens een item in de code.

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW sv_item AS
# MAGIC SELECT artikelcode AS item, 'materiaal' AS soort, omschrijving,
# MAGIC        CAST(REPLACE(massa_kg, ',', '.') AS DOUBLE) AS massa_kg
# MAGIC FROM br_erp_artikel
# MAGIC UNION ALL
# MAGIC SELECT DISTINCT atc_code, 'medicatie', omschrijving, CAST(NULL AS DOUBLE)
# MAGIC FROM br_hix_medicatie
# MAGIC UNION ALL
# MAGIC SELECT standaarditem, drager, omschrijving, CAST(NULL AS DOUBLE)
# MAGIC FROM cfg_emissiedrager WHERE standaarditem IS NOT NULL AND standaarditem <> '';
# MAGIC 
# MAGIC SELECT soort, COUNT(*) AS items FROM sv_item GROUP BY soort ORDER BY soort;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4 · Verbruik: de kerntabel
# 
# Zes takken, één vorm. Drie kolommen doen het werk:
# 
# - `regel` zegt welke allocatieregel (en dus welk patroon) nb03 toepast;
# - `koppeling` zegt hoe hard het verband met de patiënt is;
# - `geschat` zegt of dit een aanname is. Dat is de basis voor het aandeel
#   hard versus geschat in elk rapport.
# 
# Sommige takken vullen een `activiteit` in, andere alleen `zone` of `campus`
# plus `periode`. Dat verschil is granulariteit; nb03 lost het op met de
# allocatiepatronen. Een maand is 7 tekens, een dag 10.

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW sv_verbruik AS
# MAGIC 
# MAGIC -- TAK 1 · Materiaal met lotnummer. Het lotnummer staat op de ERP-boeking én in
# MAGIC -- het operatieverslag: de enige harde brug tussen aankoop en patiënt.
# MAGIC SELECT CONCAT(e._bestand, '#', e._rij)                       AS verbruik,
# MAGIC        'materiaal'                                             AS soort,
# MAGIC        e.artikelcode                                           AS item,
# MAGIC        CAST(e.aantal AS DOUBLE) * i.massa_kg                   AS hoeveelheid,
# MAGIC        'kg'                                                    AS eenheid,
# MAGIC        ok.activiteit,
# MAGIC        ok.zone,
# MAGIC        COALESCE(ok.campus, k.campus)                           AS campus,
# MAGIC        SUBSTRING(e.boekdatum, 1, 10)                           AS periode,
# MAGIC        'materiaal-direct'                                      AS regel,
# MAGIC        'lotnummer'                                             AS koppeling,
# MAGIC        'nee'                                                   AS geschat,
# MAGIC        e.bronrij_id                                            AS bronsleutel
# MAGIC FROM br_erp_verbruik e
# MAGIC JOIN sv_item i                ON e.artikelcode = i.item
# MAGIC LEFT JOIN cfg_kostenplaats k  ON e.kostenplaats = k.kostenplaats
# MAGIC LEFT JOIN ok_activiteit ok    ON e.lotnummer = ok.implantaat_lot
# MAGIC WHERE NULLIF(e.lotnummer, '') IS NOT NULL
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC -- TAK 2 · Materiaal zonder lotnummer. Het ERP boekt op kostenplaats en dag.
# MAGIC -- Wordt in nb03 verdeeld over de operaties van die campus op die dag.
# MAGIC SELECT CONCAT(e._bestand, '#', e._rij), 'materiaal', e.artikelcode,
# MAGIC        CAST(e.aantal AS DOUBLE) * i.massa_kg, 'kg',
# MAGIC        CAST(NULL AS STRING), CAST(NULL AS STRING), k.campus,
# MAGIC        SUBSTRING(e.boekdatum, 1, 10),
# MAGIC        'materiaal-verdeeld', 'verbruiksprofiel', 'ja', e.bronrij_id
# MAGIC FROM br_erp_verbruik e
# MAGIC JOIN sv_item i                ON e.artikelcode = i.item
# MAGIC LEFT JOIN cfg_kostenplaats k  ON e.kostenplaats = k.kostenplaats
# MAGIC WHERE NULLIF(e.lotnummer, '') IS NULL
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC -- TAK 3 · Medicatie en anesthesie, geregistreerd op het opnamenummer en
# MAGIC -- gekoppeld aan de operatiestap van dat traject.
# MAGIC SELECT CONCAT(m._bestand, '#', m._rij), 'medicatie', m.atc_code,
# MAGIC        CAST(REPLACE(m.hoeveelheid, ',', '.') AS DOUBLE), m.eenheid,
# MAGIC        ok.activiteit, ok.zone, ok.campus,
# MAGIC        SUBSTRING(m.toediening, 1, 10),
# MAGIC        'medicatie-direct', 'registratie', 'nee', m.bronrij_id
# MAGIC FROM br_hix_medicatie m
# MAGIC LEFT JOIN ok_activiteit ok ON m.opname_nr = ok.traject
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC -- TAK 4 · Energie per zone per maand. Met submeter is het een meting; zonder
# MAGIC -- is het een verdeling door het gebouwbeheersysteem en dus een schatting.
# MAGIC SELECT CONCAT(b._bestand, '#', b._rij), 'energie', d.standaarditem,
# MAGIC        CAST(REPLACE(b.kwh, ',', '.') AS DOUBLE), d.standaardeenheid,
# MAGIC        CAST(NULL AS STRING), b.locatiecode, l.campus,
# MAGIC        SUBSTRING(b.maand, 1, 7),
# MAGIC        'energie-zone',
# MAGIC        CASE WHEN b.submeter = 'ja' THEN 'zonemeting' ELSE 'zoneschatting' END,
# MAGIC        CASE WHEN b.submeter = 'ja' THEN 'nee' ELSE 'ja' END,
# MAGIC        CONCAT(b.locatiecode, ' ', b.maand)
# MAGIC FROM br_bms_meting b
# MAGIC JOIN cfg_emissiedrager d ON d.drager = 'energie'
# MAGIC LEFT JOIN sv_locatie l   ON b.locatiecode = l.zone
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC -- TAK 5 · Sterilisatiecycli per campus per dag.
# MAGIC SELECT CONCAT(c._bestand, '#', c._rij), 'sterilisatie', d.standaarditem,
# MAGIC        CAST(c.aantal_sets AS DOUBLE), d.standaardeenheid,
# MAGIC        CAST(NULL AS STRING), CAST(NULL AS STRING), c.campuscode,
# MAGIC        SUBSTRING(c.datum, 1, 10),
# MAGIC        'sterilisatie-set', 'instrumentenset', 'ja', c.cyclus_id
# MAGIC FROM br_cssd_cyclus c
# MAGIC JOIN cfg_emissiedrager d ON d.drager = 'sterilisatie'
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC -- TAK 6 · Transportritten per campus per maand.
# MAGIC SELECT CONCAT(r._bestand, '#', r._rij), 'transport', d.standaarditem,
# MAGIC        CAST(REPLACE(r.km, ',', '.') AS DOUBLE), d.standaardeenheid,
# MAGIC        CAST(NULL AS STRING), CAST(NULL AS STRING), r.campuscode,
# MAGIC        SUBSTRING(r.maand, 1, 7),
# MAGIC        'transport-ingrepen', 'levering', 'ja', r.rit_id
# MAGIC FROM br_tms_rit r
# MAGIC JOIN cfg_emissiedrager d ON d.drager = 'transport';
# MAGIC 
# MAGIC -- Controle op de periode: 7 tekens voor maandregels, 10 voor dagregels.
# MAGIC SELECT soort, regel, koppeling, LENGTH(periode) AS lengte_periode, COUNT(*) AS rijen,
# MAGIC        ROUND(SUM(hoeveelheid), 1) AS hoeveelheid
# MAGIC FROM sv_verbruik GROUP BY soort, regel, koppeling, LENGTH(periode) ORDER BY soort, regel, koppeling;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5 · Datakwaliteit
# 
# Zes controles. Ernst `hoog` betekent quarantaine: de rij bereikt de rapporten
# niet. Ernst `middel` telt mee, gemarkeerd. Een onvolmaakt cijfer is
# bruikbaarder dan geen cijfer, zolang de onvolmaaktheid zichtbaar is.
# 
# De schemadrift-controle komt uit nb01 (`levering`), want kolomnamen zijn
# metadata en die vergelijk je bij het inladen.

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW sv_datakwaliteit AS
# MAGIC 
# MAGIC -- 1. Items zonder emissiefactor in de set die in gebruik is: tellen mee met 0, gemarkeerd.
# MAGIC SELECT 'geen emissiefactor' AS bevinding, v.item AS detail, COUNT(*) AS aantal,
# MAGIC        'middel' AS ernst, 'geschat, factor 0' AS actie, CAST(NULL AS STRING) AS verbruik
# MAGIC FROM sv_verbruik v
# MAGIC LEFT JOIN (SELECT DISTINCT f.item FROM cfg_emissiefactor f
# MAGIC            JOIN cfg_factorenset s ON f.factorenset = s.factorenset AND s.status = 'in gebruik') f
# MAGIC        ON v.item = f.item
# MAGIC WHERE f.item IS NULL GROUP BY v.item
# MAGIC 
# MAGIC UNION ALL
# MAGIC -- 2. Energiemeting ver buiten bereik (meer dan vijf keer de mediaan): quarantaine.
# MAGIC SELECT 'energiemeting buiten bereik', v.bronsleutel, 1, 'hoog', 'quarantaine', v.verbruik
# MAGIC FROM sv_verbruik v
# MAGIC WHERE v.soort = 'energie'
# MAGIC   AND v.hoeveelheid > 5 * (SELECT PERCENTILE_APPROX(hoeveelheid, 0.5) FROM sv_verbruik WHERE soort = 'energie')
# MAGIC 
# MAGIC UNION ALL
# MAGIC -- 3. Boeking op een onbekende kostenplaats: geen campus, dus niet toewijsbaar.
# MAGIC SELECT 'onbekende kostenplaats', e.kostenplaats, COUNT(*), 'middel', 'blijft in restpost', NULL
# MAGIC FROM br_erp_verbruik e LEFT JOIN cfg_kostenplaats k ON e.kostenplaats = k.kostenplaats
# MAGIC WHERE k.campus IS NULL GROUP BY e.kostenplaats
# MAGIC 
# MAGIC UNION ALL
# MAGIC -- 4. Lotnummer in het ERP dat in geen operatieverslag voorkomt.
# MAGIC SELECT 'lotnummer zonder operatie', v.bronsleutel, 1, 'middel', 'blijft in restpost', v.verbruik
# MAGIC FROM sv_verbruik v WHERE v.koppeling = 'lotnummer' AND v.activiteit IS NULL
# MAGIC 
# MAGIC UNION ALL
# MAGIC -- 5. Verrichting die niet in het zorgpad past.
# MAGIC SELECT 'verrichting zonder zorgpadstap', CONCAT(verrichtingcode, ' in ', COALESCE(zorgpad, '?')), COUNT(*),
# MAGIC        'middel', 'activiteit zonder stap, verbruik blijft in restpost', NULL
# MAGIC FROM sv_activiteit WHERE stap IS NULL GROUP BY verrichtingcode, zorgpad
# MAGIC 
# MAGIC UNION ALL
# MAGIC -- 6. Schemadrift uit het leveringsregister van deze run.
# MAGIC SELECT 'schemadrift', CONCAT(tabel, ': extra ', COALESCE(kolommen_extra, ''), ' ontbreekt ', COALESCE(kolommen_ontbrekend, '')),
# MAGIC        rijen, CASE WHEN status = 'geweigerd' THEN 'hoog' ELSE 'middel' END, status, NULL
# MAGIC FROM levering
# MAGIC WHERE status IN ('gewaarschuwd', 'geweigerd')
# MAGIC   AND levering = (SELECT MAX(levering) FROM levering);
# MAGIC 
# MAGIC SELECT bevinding, ernst, actie, SUM(aantal) AS aantal FROM sv_datakwaliteit
# MAGIC GROUP BY bevinding, ernst, actie ORDER BY ernst DESC, bevinding;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Status per verbruiksregel. Alleen ernst hoog geeft quarantaine.
# MAGIC CREATE OR REPLACE TEMP VIEW sv_verbruik_def AS
# MAGIC SELECT v.*, CASE WHEN q.verbruik IS NOT NULL THEN 'quarantaine' ELSE 'ok' END AS status
# MAGIC FROM sv_verbruik v
# MAGIC LEFT JOIN (SELECT DISTINCT verbruik FROM sv_datakwaliteit WHERE ernst = 'hoog' AND verbruik IS NOT NULL) q
# MAGIC        ON v.verbruik = q.verbruik;
# MAGIC 
# MAGIC SELECT status, COUNT(*) AS rijen FROM sv_verbruik_def GROUP BY status;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6 · Wegschrijven naar zilver

# CELL ********************

DOELEN = [("sv_locatie", "locatie"), ("sv_zorgpadstap", "zorgpadstap"),
          ("sv_traject", "traject"), ("sv_activiteit", "activiteit"),
          ("sv_item", "item"), ("sv_verbruik_def", "verbruik"),
          ("sv_datakwaliteit", "datakwaliteit")]
print("Wegschrijven naar", ZILVER_T, "\n")
for weergave, tabel in DOELEN:
    bewaar(weergave, ZILVER_T, tabel)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
