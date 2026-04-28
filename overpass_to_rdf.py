#!/usr/bin/env python3
"""
overpass_to_rdf.py
==================
Scarica tutti i defibrillatori (DAE) della Puglia da OpenStreetMap via Overpass API
e genera file RDF conformi alle ontologie ufficiali italiane dell'ecosistema
schema.gov.it / dati.gov.it (dati-semantic-assets).

Ontologie utilizzate (verificate su github.com/italia/dati-semantic-assets):
  - POI-AP_IT  (Point of Interest)
  - CLV-AP_IT  (Core Location Vocabulary)
  - COV-AP_IT  (Core Public Organization Vocabulary)
  - TI-AP_IT   (Time Indexed)
  - l0         (Top-level)
  - schema.org (orari, accessibilita')
  - GeoSPARQL  (geometria WKT)
  - WGS84 geo  (lat/long)

Uso:
    python overpass_to_rdf.py [--output-dir ./output] [--base-uri http://...]
    python overpass_to_rdf.py --mock            # test senza rete

Dipendenze:
    pip install rdflib requests
"""

import argparse
import csv
import json
import logging
import re
import sys
import time
from datetime import date
from pathlib import Path

import requests
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS, XSD, SKOS, DCTERMS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# === Namespace ufficiali (ecosistema schema.gov.it / dati.gov.it) ===
POI     = Namespace("https://w3id.org/italia/onto/POI/")
CLV     = Namespace("https://w3id.org/italia/onto/CLV/")
COV     = Namespace("https://w3id.org/italia/onto/COV/")
TI      = Namespace("https://w3id.org/italia/onto/TI/")
L0      = Namespace("https://w3id.org/italia/onto/l0/")
GEO     = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
GSP     = Namespace("http://www.opengis.net/ont/geosparql#")
SCHEMA  = Namespace("https://schema.org/")
ISTAT   = Namespace("https://w3id.org/italia/controlled-vocabulary/territorial-classifications/cities/")
COUNTRY = Namespace("http://publications.europa.eu/resource/authority/country/")

# === Endpoint Overpass ===
OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
]

PUGLIA_BBOX = (39.7, 14.9, 42.0, 18.6)

OVERPASS_QUERY = (
    "[out:json][timeout:60];"
    "(node[emergency=defibrillator]("
    + ",".join(str(x) for x in PUGLIA_BBOX)
    + ");way[emergency=defibrillator]("
    + ",".join(str(x) for x in PUGLIA_BBOX)
    + ");relation[emergency=defibrillator]("
    + ",".join(str(x) for x in PUGLIA_BBOX)
    + "););out center body;"
)


def fetch_overpass(query, retries=3, backoff=5.0):
    for endpoint in OVERPASS_ENDPOINTS:
        for attempt in range(1, retries + 1):
            try:
                log.info("Overpass %s (tentativo %d/%d)...", endpoint, attempt, retries)
                resp = requests.post(
                    endpoint, data={"data": query}, timeout=90,
                    headers={"Accept": "application/json"},
                )
                resp.raise_for_status()
                elements = resp.json().get("elements", [])
                log.info("Ricevuti %d elementi da OSM.", len(elements))
                return elements
            except requests.exceptions.RequestException as exc:
                log.warning("Errore %s: %s", endpoint, exc)
                if attempt < retries:
                    time.sleep(backoff * attempt)
    log.error("Tutti gli endpoint Overpass non raggiungibili.")
    sys.exit(1)


# === Codici Belfiore citta' Puglia (per URI ISTAT controlled vocabulary) ===
BELFIORE_MAP = {
    "bari": ("A662", "Bari"), "lecce": ("E506", "Lecce"),
    "taranto": ("L049", "Taranto"), "brindisi": ("B180", "Brindisi"),
    "foggia": ("D643", "Foggia"), "andria": ("A285", "Andria"),
    "barletta": ("A669", "Barletta"), "trani": ("L328", "Trani"),
    "altamura": ("A225", "Altamura"), "molfetta": ("F284", "Molfetta"),
    "gravina in puglia": ("E155", "Gravina in Puglia"),
    "bitonto": ("A892", "Bitonto"), "manfredonia": ("E885", "Manfredonia"),
    "cerignola": ("C514", "Cerignola"), "san severo": ("I158", "San Severo"),
    "monopoli": ("F376", "Monopoli"), "noci": ("F915", "Noci"),
    "conversano": ("C975", "Conversano"), "martina franca": ("E986", "Martina Franca"),
    "crispiano": ("D300", "Crispiano"), "copertino": ("C978", "Copertino"),
    "galatina": ("D861", "Galatina"), "gallipoli": ("D883", "Gallipoli"),
    "nardo": ("F842", "Nardo"), "maglie": ("E815", "Maglie"),
    "tricase": ("L419", "Tricase"), "ostuni": ("G187", "Ostuni"),
    "fasano": ("D508", "Fasano"), "grottaglie": ("E205", "Grottaglie"),
    "manduria": ("E882", "Manduria"),
    "francavilla fontana": ("D761", "Francavilla Fontana"),
    "mesagne": ("F152", "Mesagne"),
    "san giovanni rotondo": ("H926", "San Giovanni Rotondo"),
    "lucera": ("E716", "Lucera"), "vieste": ("L858", "Vieste"),
    "corato": ("C983", "Corato"), "ruvo di puglia": ("H645", "Ruvo di Puglia"),
    "gioia del colle": ("E038", "Gioia del Colle"),
    "castellana grotte": ("C136", "Castellana Grotte"),
    "locorotondo": ("E645", "Locorotondo"),
    "alberobello": ("A149", "Alberobello"), "cisternino": ("C741", "Cisternino"),
}


def city_uri(city_name: str):
    if not city_name:
        return None, ""
    key = city_name.lower().strip()
    if key in BELFIORE_MAP:
        code, label = BELFIORE_MAP[key]
        return ISTAT[code], label
    return None, city_name


def safe_id(osm_id, osm_type="node"):
    prefix = {"node": "N", "way": "W", "relation": "R"}.get(osm_type, "X")
    return f"OSM-{prefix}{osm_id}"


def slugify(text: str, maxlen: int = 50) -> str:
    """Slug ASCII per URI: lowercase, alfanum + hyphen."""
    s = re.sub(r"[^a-zA-Z0-9]+", "-", (text or "").strip().lower())
    s = re.sub(r"-+", "-", s).strip("-")
    return s[:maxlen] or "x"


def get_center(element):
    if "lat" in element:
        return element["lat"], element["lon"]
    c = element.get("center", {})
    return (c.get("lat"), c.get("lon")) if c else (None, None)


# === CAP -> sigla provincia (per metadati CSV; non genera triple errate) ===
CAP_PROVINCIA = {
    "70": "BA", "76": "BT", "72": "BR",
    "71": "FG", "73": "LE", "74": "TA",
}


def build_graph(elements, base_uri):
    DAE_NS    = Namespace(base_uri + "dae/")
    ADDR_NS   = Namespace(base_uri + "indirizzo/")
    GEOM_NS   = Namespace(base_uri + "geometria/")
    ORARI_NS  = Namespace(base_uri + "orari/")
    ENTE_NS   = Namespace(base_uri + "ente/")
    CAT_NS    = Namespace(base_uri + "categoria-poi/")

    g = Graph()
    for prefix, ns in [
        ("poi", POI), ("clv", CLV), ("cov", COV), ("ti", TI), ("l0", L0),
        ("geo", GEO), ("gsp", GSP), ("skos", SKOS), ("dct", DCTERMS),
        ("rdfs", RDFS), ("xsd", XSD), ("schema", SCHEMA), ("istat", ISTAT),
        ("country", COUNTRY), ("dae", DAE_NS), ("addr", ADDR_NS),
        ("geom", GEOM_NS), ("orari", ORARI_NS), ("ente", ENTE_NS),
        ("cat", CAT_NS),
    ]:
        g.bind(prefix, ns)

    # === Concept Scheme SKOS per categorie POI ===
    scheme = CAT_NS["SchemeCategoriePOI"]
    g.add((scheme, RDF.type, SKOS.ConceptScheme))
    g.add((scheme, SKOS.prefLabel, Literal("Categorie di Punti di Interesse - DAE", lang="it")))
    g.add((scheme, DCTERMS.title, Literal("Categorie di Punti di Interesse - DAE", lang="it")))

    # === Categoria POI: DAE ===
    # Multi-typing: e' sia poi:PointOfInterestCategory (classe POI ufficiale)
    # sia skos:Concept (per inserimento nello scheme)
    cat_dae = CAT_NS["DefibrillatoreDae"]
    g.add((cat_dae, RDF.type, POI.PointOfInterestCategory))
    g.add((cat_dae, RDF.type, SKOS.Concept))
    g.add((cat_dae, SKOS.inScheme, scheme))
    g.add((cat_dae, SKOS.prefLabel, Literal("Defibrillatore Automatico Esterno (DAE)", lang="it")))
    g.add((cat_dae, SKOS.prefLabel, Literal("Automated External Defibrillator (AED)", lang="en")))
    g.add((cat_dae, SKOS.notation, Literal("DAE")))
    # POIcategoryName e POIcategoryIdentifier sono datatypeProperties POI ufficiali
    g.add((cat_dae, POI.POIcategoryName, Literal("Defibrillatore Automatico Esterno", lang="it")))
    g.add((cat_dae, POI.POIcategoryIdentifier, Literal("DAE")))

    # === Orario H24 condiviso (TimeInterval) ===
    orario_h24 = ORARI_NS["H24"]
    g.add((orario_h24, RDF.type, TI.TimeInterval))
    g.add((orario_h24, RDFS.label, Literal("Accessibile 24 ore su 24, 7 giorni su 7", lang="it")))
    g.add((orario_h24, TI.startTime, Literal("00:00:00", datatype=XSD.time)))
    g.add((orario_h24, TI.endTime, Literal("23:59:59", datatype=XSD.time)))

    today = date.today().isoformat()
    country_ita = COUNTRY["ITA"]
    g.add((country_ita, RDF.type, CLV.Country))
    g.add((country_ita, RDFS.label, Literal("Italia", lang="it")))

    for elem in elements:
        tags = elem.get("tags", {})
        osm_id = elem["id"]
        osm_type = elem.get("type", "node")
        lat, lon = get_center(elem)
        if lat is None:
            continue

        node_id  = safe_id(osm_id, osm_type)
        dae_uri  = DAE_NS[node_id]
        addr_uri = ADDR_NS[node_id]
        geom_uri = GEOM_NS[node_id]

        name        = tags.get("name", "")
        description = tags.get("description", tags.get("note", ""))
        street      = tags.get("addr:street", "")
        housenumber = tags.get("addr:housenumber", "")
        city_raw    = tags.get("addr:city", tags.get("addr:municipality", ""))
        postcode    = tags.get("addr:postcode", "")
        phone       = tags.get("phone", tags.get("contact:phone", ""))
        opening     = tags.get("opening_hours", "")
        operator    = tags.get("operator", "")
        serial      = tags.get("ref", "")
        access      = tags.get("access", "")
        indoor      = tags.get("indoor", "")
        level       = tags.get("level", "")

        osm_url = f"https://www.openstreetmap.org/{osm_type}/{osm_id}"

        # Etichetta italiana
        if name:
            label_it = name
        elif street:
            label_it = f"DAE - {street} {housenumber}".strip()
            if city_raw:
                label_it += f", {city_raw}"
        elif city_raw:
            label_it = f"DAE - {city_raw}"
        else:
            label_it = f"DAE OSM:{osm_id}"

        # === POI: PointOfInterest ===
        g.add((dae_uri, RDF.type, POI.PointOfInterest))
        g.add((dae_uri, DCTERMS.identifier, Literal(f"osm:{osm_type}/{osm_id}")))
        g.add((dae_uri, RDFS.label, Literal(label_it, lang="it")))
        g.add((dae_uri, L0.name, Literal(label_it, lang="it")))
        # POIofficialName solo se name OSM presente
        if name:
            g.add((dae_uri, POI.POIofficialName, Literal(name, lang="it")))
        # poi:hasPOICategory (corretta - sostituisce hasPointOfInterestType inesistente)
        g.add((dae_uri, POI.hasPOICategory, cat_dae))

        # === Geo WGS84 (datatype xsd:decimal) ===
        g.add((dae_uri, GEO.lat, Literal(str(lat), datatype=XSD.decimal)))
        g.add((dae_uri, GEO["long"], Literal(str(lon), datatype=XSD.decimal)))

        # === Indirizzo CLV ===
        g.add((dae_uri, CLV.hasAddress, addr_uri))

        # === Geometria GeoSPARQL ===
        g.add((dae_uri, GSP.hasGeometry, geom_uri))

        # === Metadati Dublin Core ===
        g.add((dae_uri, DCTERMS.modified, Literal(today, datatype=XSD.date)))
        g.add((dae_uri, DCTERMS.source, URIRef(osm_url)))

        if description:
            g.add((dae_uri, DCTERMS.description, Literal(description, lang="it")))
            g.add((dae_uri, POI.POIdescription, Literal(description, lang="it")))

        # === Accessibilita' (schema.org - non esiste in POI italiana) ===
        if access:
            g.add((dae_uri, SCHEMA.publicAccess, Literal(access)))
        # Default DAE: free per legge (DM 18/03/2011 e successive)
        g.add((dae_uri, SCHEMA.isAccessibleForFree, Literal(True, datatype=XSD.boolean)))

        if phone:
            g.add((dae_uri, SCHEMA.telephone, Literal(phone)))

        if serial:
            g.add((dae_uri, SCHEMA.serialNumber, Literal(serial)))

        if indoor:
            g.add((dae_uri, SCHEMA.location, Literal(f"indoor={indoor}", lang="it")))
        if level:
            g.add((dae_uri, SCHEMA.floorLevel, Literal(str(level))))

        # === Orari di apertura ===
        # Pattern italiano: TI.TimeInterval collegato via dct:temporal (relazione generica)
        # In assenza di proprieta' POI ufficiale per gli orari, usiamo schema.org
        if opening:
            if opening == "24/7":
                g.add((dae_uri, SCHEMA.openingHours, Literal("Mo-Su 00:00-23:59")))
                g.add((dae_uri, DCTERMS.temporal, orario_h24))
            else:
                # schema.org openingHours: stringa human/machine readable
                g.add((dae_uri, SCHEMA.openingHours, Literal(opening)))
                # In aggiunta, modello TimeInterval per descrizione
                orario_uri = ORARI_NS[node_id]
                g.add((orario_uri, RDF.type, TI.TimeInterval))
                g.add((orario_uri, RDFS.label, Literal(opening, lang="it")))
                g.add((dae_uri, DCTERMS.temporal, orario_uri))

        # === Operatore / Ente gestore (COV.Organization, non l0:Agent) ===
        if operator:
            ente_slug = slugify(operator, 50)
            ente_uri = ENTE_NS[ente_slug]
            g.add((ente_uri, RDF.type, COV.Organization))
            g.add((ente_uri, RDFS.label, Literal(operator, lang="it")))
            g.add((ente_uri, L0.name, Literal(operator, lang="it")))
            g.add((ente_uri, COV.legalName, Literal(operator, lang="it")))
            # dct:rightsHolder e' la proprieta' standard per "ente che gestisce"
            # (sostituisce poi:isManagedBy che non esiste nell'ontologia POI ufficiale)
            g.add((dae_uri, DCTERMS.rightsHolder, ente_uri))

        # === Address node CLV ===
        g.add((addr_uri, RDF.type, CLV.Address))
        # Costruzione fullAddress (datatypeProperty CLV)
        full_parts = []
        if street:
            sp = f"{street} {housenumber}".strip()
            full_parts.append(sp)
        if postcode and city_raw:
            full_parts.append(f"{postcode} {city_raw}")
        elif city_raw:
            full_parts.append(city_raw)
        elif postcode:
            full_parts.append(postcode)
        if full_parts:
            g.add((addr_uri, CLV.fullAddress, Literal(" - ".join(full_parts), lang="it")))

        # streetNumber (datatypeProperty CLV ufficiale)
        if housenumber:
            g.add((addr_uri, CLV.streetNumber, Literal(housenumber)))

        # postCode (datatypeProperty CLV)
        if postcode:
            g.add((addr_uri, CLV.postCode, Literal(postcode)))

        # === Citta' (clv:hasCity e' ObjectProperty -> range clv:City) ===
        # Solo se abbiamo URI ISTAT noto. Altrimenti SKIP - mai literal su ObjectProperty.
        # La citta' resta nel fullAddress come testo.
        city_istat_uri, city_label = city_uri(city_raw)
        if city_istat_uri:
            g.add((addr_uri, CLV.hasCity, city_istat_uri))
            g.add((city_istat_uri, RDF.type, CLV.City))
            g.add((city_istat_uri, RDFS.label, Literal(city_label, lang="it")))
            g.add((city_istat_uri, L0.name, Literal(city_label, lang="it")))

        # === Country (clv:hasCountry -> clv:Country) ===
        g.add((addr_uri, CLV.hasCountry, country_ita))

        # === Geometria GeoSPARQL (WKT) ===
        g.add((geom_uri, RDF.type, GSP.Geometry))
        g.add((geom_uri, GSP.asWKT, Literal(f"POINT({lon} {lat})", datatype=GSP.wktLiteral)))

    log.info("Grafo: %d triple.", len(g))
    return g


def load_mock_data():
    return [
        {"type": "node", "id": 123456001, "lat": 40.3515, "lon": 18.1750,
         "tags": {"emergency": "defibrillator", "name": "DAE Municipio Lecce",
                  "addr:street": "Via Umberto I", "addr:housenumber": "13",
                  "addr:city": "Lecce", "addr:postcode": "73100",
                  "opening_hours": "Mo-Fr 08:30-13:30", "operator": "Comune di Lecce"}},
        {"type": "node", "id": 123456002, "lat": 40.3522, "lon": 18.1765,
         "tags": {"emergency": "defibrillator", "addr:street": "Piazza Sant'Oronzo",
                  "addr:city": "Lecce", "addr:postcode": "73100",
                  "opening_hours": "24/7", "operator": "Comune di Lecce"}},
        {"type": "node", "id": 123456003, "lat": 41.1171, "lon": 16.8719,
         "tags": {"emergency": "defibrillator", "name": "DAE Policlinico Bari",
                  "addr:street": "Piazza Giulio Cesare", "addr:housenumber": "11",
                  "addr:city": "Bari", "addr:postcode": "70124",
                  "opening_hours": "24/7", "operator": "AOU Policlinico Bari",
                  "phone": "+39 080 5592111"}},
        {"type": "node", "id": 123456004, "lat": 40.4764, "lon": 17.2290,
         "tags": {"emergency": "defibrillator", "name": "DAE Stazione FS Taranto",
                  "addr:city": "Taranto", "addr:postcode": "74121",
                  "opening_hours": "24/7", "operator": "RFI"}},
        {"type": "node", "id": 123456005, "lat": 40.6371, "lon": 17.9435,
         "tags": {"emergency": "defibrillator", "addr:street": "Corso Umberto I",
                  "addr:city": "Brindisi", "addr:postcode": "72100",
                  "opening_hours": "Mo-Sa 09:00-19:00"}},
        {"type": "node", "id": 123456006, "lat": 41.4622, "lon": 15.5449,
         "tags": {"emergency": "defibrillator", "name": "DAE Aeroporto Foggia",
                  "addr:city": "Foggia", "addr:postcode": "71100",
                  "opening_hours": "24/7", "operator": "ENAC"}},
    ]


def export_csv(elements: list, out_path: Path) -> None:
    """Esporta i dati DAE in formato CSV flat."""
    FIELDS = [
        "osm_id", "osm_type", "osm_url",
        "name", "description",
        "addr_street", "addr_housenumber", "addr_city",
        "addr_postcode", "addr_province",
        "lat", "lon",
        "opening_hours", "access",
        "operator", "phone", "ref",
        "indoor", "level",
        "last_updated",
    ]
    today = date.today().isoformat()
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        for elem in elements:
            tags = elem.get("tags", {})
            osm_id = elem["id"]
            osm_type = elem.get("type", "node")
            lat, lon = get_center(elem)
            if lat is None:
                continue
            postcode = tags.get("addr:postcode", "")
            province = tags.get("addr:province", tags.get("addr:state", ""))
            if not province and postcode:
                cap_prefix = postcode[:2] if len(postcode) >= 2 else ""
                province = CAP_PROVINCIA.get(cap_prefix, "")
            writer.writerow({
                "osm_id": osm_id,
                "osm_type": osm_type,
                "osm_url": f"https://www.openstreetmap.org/{osm_type}/{osm_id}",
                "name": tags.get("name", ""),
                "description": tags.get("description", tags.get("note", "")),
                "addr_street": tags.get("addr:street", ""),
                "addr_housenumber": tags.get("addr:housenumber", ""),
                "addr_city": tags.get("addr:city", tags.get("addr:municipality", "")),
                "addr_postcode": postcode,
                "addr_province": province,
                "lat": lat,
                "lon": lon,
                "opening_hours": tags.get("opening_hours", ""),
                "access": tags.get("access", ""),
                "operator": tags.get("operator", ""),
                "phone": tags.get("phone", tags.get("contact:phone", "")),
                "ref": tags.get("ref", ""),
                "indoor": tags.get("indoor", ""),
                "level": tags.get("level", ""),
                "last_updated": today,
            })


def main():
    parser = argparse.ArgumentParser(description="Overpass -> RDF DAE Puglia (ontologie schema.gov.it/dati.gov.it)")
    parser.add_argument("--output-dir", default="./output")
    parser.add_argument("--base-uri", default="http://dati.regione.puglia.it/resource/dae/")
    parser.add_argument("--mock", action="store_true")
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    elements = load_mock_data() if args.mock else fetch_overpass(OVERPASS_QUERY)
    if not elements:
        log.error("Nessun elemento trovato.")
        sys.exit(1)

    g = build_graph(elements, args.base_uri)

    ttl_path = out_dir / "dae_puglia.ttl"
    g.serialize(destination=str(ttl_path), format="turtle")
    log.info("Scritto: %s", ttl_path)

    rdf_path = out_dir / "dae_puglia.rdf"
    g.serialize(destination=str(rdf_path), format="xml")
    log.info("Scritto: %s", rdf_path)

    csv_path = out_dir / "dae_puglia.csv"
    export_csv(elements, csv_path)
    log.info("Scritto: %s", csv_path)

    (out_dir / "last_update.json").write_text(json.dumps({
        "updated": date.today().isoformat(),
        "elements_count": len(elements),
        "triples_count": len(g),
        "source": "OpenStreetMap via Overpass API",
        "bbox": PUGLIA_BBOX,
        "query_tag": "emergency=defibrillator",
        "ontologies": [
            "POI-AP_IT", "CLV-AP_IT", "COV-AP_IT", "TI-AP_IT", "l0",
            "schema.org", "GeoSPARQL", "WGS84-geo",
        ],
        "files": ["dae_puglia.ttl", "dae_puglia.rdf", "dae_puglia.csv"],
    }, indent=2, ensure_ascii=False))

    log.info("Completato.")


if __name__ == "__main__":
    main()
