#!/usr/bin/env python3
"""
overpass_to_rdf.py
==================
Scarica tutti i defibrillatori (DAE) della Puglia da OpenStreetMap via Overpass API
e genera file RDF conformi a OntoPiA (schema.gov.it).

Uso:
    python overpass_to_rdf.py [--output-dir ./output] [--base-uri http://...]
    python overpass_to_rdf.py --mock   # test senza rete

Dipendenze:
    pip install rdflib requests
"""

import argparse
import json
import logging
import sys
import time
from datetime import date
from pathlib import Path

import requests
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS, XSD, SKOS, DCTERMS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

POI     = Namespace("https://w3id.org/italia/onto/POI/")
CLV     = Namespace("https://w3id.org/italia/onto/CLV/")
TI      = Namespace("https://w3id.org/italia/onto/TI/")
L0      = Namespace("https://w3id.org/italia/onto/l0/")
GEO     = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
GSP     = Namespace("http://www.opengis.net/ont/geosparql#")
SCHEMA  = Namespace("https://schema.org/")
ISTAT   = Namespace("https://w3id.org/italia/controlled-vocabulary/territorial-classifications/cities/")
COUNTRY = Namespace("http://publications.europa.eu/resource/authority/country/")

OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
]

PUGLIA_BBOX = (39.7, 14.9, 42.0, 18.6)

OVERPASS_QUERY = """
[out:json][timeout:60];
(
  node[emergency=defibrillator]{bbox};
  way[emergency=defibrillator]{bbox};
  relation[emergency=defibrillator]{bbox};
);
out center body;
""".format(bbox="{},{},{},{}".format(*PUGLIA_BBOX))


def fetch_overpass(query, retries=3, backoff=5.0):
    for endpoint in OVERPASS_ENDPOINTS:
        for attempt in range(1, retries + 1):
            try:
                log.info("Overpass %s (tentativo %d/%d)...", endpoint, attempt, retries)
                resp = requests.post(endpoint, data={"data": query}, timeout=90,
                                     headers={"Accept": "application/json"})
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


BELFIORE_MAP = {
    "bari": ("A662","Bari"), "lecce": ("E506","Lecce"),
    "taranto": ("L049","Taranto"), "brindisi": ("B180","Brindisi"),
    "foggia": ("D643","Foggia"), "andria": ("A285","Andria"),
    "barletta": ("A669","Barletta"), "trani": ("L328","Trani"),
    "altamura": ("A225","Altamura"), "molfetta": ("F284","Molfetta"),
    "gravina in puglia": ("E155","Gravina in Puglia"),
    "bitonto": ("A892","Bitonto"), "manfredonia": ("E885","Manfredonia"),
    "cerignola": ("C514","Cerignola"), "san severo": ("I158","San Severo"),
    "monopoli": ("F376","Monopoli"), "noci": ("F915","Noci"),
    "conversano": ("C975","Conversano"), "martina franca": ("E986","Martina Franca"),
    "crispiano": ("D300","Crispiano"), "copertino": ("C978","Copertino"),
    "galatina": ("D861","Galatina"), "gallipoli": ("D883","Gallipoli"),
    "nardo": ("F842","Nardo"), "maglie": ("E815","Maglie"),
    "tricase": ("L419","Tricase"), "ostuni": ("G187","Ostuni"),
    "fasano": ("D508","Fasano"), "grottaglie": ("E205","Grottaglie"),
    "manduria": ("E882","Manduria"),
    "francavilla fontana": ("D761","Francavilla Fontana"),
    "mesagne": ("F152","Mesagne"),
    "san giovanni rotondo": ("H926","San Giovanni Rotondo"),
    "lucera": ("E716","Lucera"), "vieste": ("L858","Vieste"),
    "corato": ("C983","Corato"), "ruvo di puglia": ("H645","Ruvo di Puglia"),
    "gioia del colle": ("E038","Gioia del Colle"),
    "castellana grotte": ("C136","Castellana Grotte"),
    "locorotondo": ("E645","Locorotondo"),
    "alberobello": ("A149","Alberobello"), "cisternino": ("C741","Cisternino"),
}


def city_uri(city_name):
    key = city_name.lower().strip()
    if key in BELFIORE_MAP:
        code, label = BELFIORE_MAP[key]
        return ISTAT[code], label
    return None, city_name


def safe_id(osm_id, osm_type="node"):
    prefix = {'node': 'N', 'way': 'W', 'relation': 'R'}.get(osm_type, 'X')
    return f'OSM-{prefix}{osm_id}'


def get_center(element):
    if "lat" in element:
        return element["lat"], element["lon"]
    c = element.get("center", {})
    return (c.get("lat"), c.get("lon")) if c else (None, None)


def build_graph(elements, base_uri):
    DAE_NS   = Namespace(base_uri + "dae/")
    ADDR_NS  = Namespace(base_uri + "indirizzo/")
    GEOM_NS  = Namespace(base_uri + "geometria/")
    ORARI_NS = Namespace(base_uri + "orari/")
    ENTE_NS  = Namespace(base_uri + "ente/")
    TIPO_NS  = Namespace(base_uri + "tipo/")

    g = Graph()
    for prefix, ns in [("poi",POI),("clv",CLV),("ti",TI),("l0",L0),("geo",GEO),
                        ("gsp",GSP),("skos",SKOS),("dct",DCTERMS),("rdfs",RDFS),
                        ("xsd",XSD),("schema",SCHEMA),("istat",ISTAT),
                        ("country",COUNTRY),("dae",DAE_NS),("addr",ADDR_NS),
                        ("geom",GEOM_NS),("orari",ORARI_NS),("ente",ENTE_NS),
                        ("tipo",TIPO_NS)]:
        g.bind(prefix, ns)

    scheme = TIPO_NS["SchemePOI"]
    g.add((scheme, RDF.type, SKOS.ConceptScheme))
    g.add((scheme, SKOS.prefLabel, Literal("Tipi di punti di interesse - DAE", lang="it")))

    tipo_dae = TIPO_NS["DefibrillatoreDae"]
    g.add((tipo_dae, RDF.type, SKOS.Concept))
    g.add((tipo_dae, SKOS.inScheme, scheme))
    g.add((tipo_dae, SKOS.prefLabel, Literal("Defibrillatore Automatico Esterno (DAE)", lang="it")))
    g.add((tipo_dae, SKOS.prefLabel, Literal("Automated External Defibrillator (AED)", lang="en")))
    g.add((tipo_dae, SKOS.notation, Literal("DAE")))

    orario_h24 = ORARI_NS["H24"]
    g.add((orario_h24, RDF.type, TI.TimeInterval))
    g.add((orario_h24, RDFS.label, Literal("Accessibile 24 ore su 24, 7 giorni su 7", lang="it")))
    g.add((orario_h24, TI.startTime, Literal("00:00:00", datatype=XSD.time)))
    g.add((orario_h24, TI.endTime,   Literal("23:59:59", datatype=XSD.time)))

    today = date.today().isoformat()
    country_ita = COUNTRY["ITA"]

    for elem in elements:
        tags     = elem.get("tags", {})
        osm_id   = elem["id"]
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
        osm_url     = f"https://www.openstreetmap.org/{osm_type}/{osm_id}"

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

        g.add((dae_uri, RDF.type,                   POI.PointOfInterest))
        g.add((dae_uri, DCTERMS.identifier,         Literal(f"osm:{osm_type}/{osm_id}")))
        g.add((dae_uri, RDFS.label,                 Literal(label_it, lang="it")))
        g.add((dae_uri, POI.hasPointOfInterestType, tipo_dae))
        g.add((dae_uri, POI.isAccessibleForFree,    Literal(True, datatype=XSD.boolean)))
        g.add((dae_uri, GEO.lat,                    Literal(str(lat), datatype=XSD.decimal)))
        g.add((dae_uri, GEO["long"],                Literal(str(lon), datatype=XSD.decimal)))
        g.add((dae_uri, CLV.hasAddress,             addr_uri))
        g.add((dae_uri, GSP.hasGeometry,            geom_uri))
        g.add((dae_uri, DCTERMS.modified,           Literal(today, datatype=XSD.date)))
        g.add((dae_uri, DCTERMS.source,             URIRef(osm_url)))

        if description:
            g.add((dae_uri, DCTERMS.description, Literal(description, lang="it")))
        if phone:
            g.add((dae_uri, SCHEMA.telephone, Literal(phone)))
        if serial:
            g.add((dae_uri, SCHEMA.serialNumber, Literal(serial)))

        if opening == "24/7":
            g.add((dae_uri, POI.hasOpeningHoursSpecification, orario_h24))
        elif opening:
            orario_uri = ORARI_NS[node_id]
            g.add((orario_uri, RDF.type,   TI.TimeInterval))
            g.add((orario_uri, RDFS.label, Literal(opening, lang="it")))
            g.add((dae_uri, POI.hasOpeningHoursSpecification, orario_uri))

        if operator:
            ente_key = operator.lower().replace(" ", "_")[:40]
            ente_uri = ENTE_NS[ente_key]
            g.add((ente_uri, RDF.type,   L0.Agent))
            g.add((ente_uri, RDFS.label, Literal(operator, lang="it")))
            g.add((dae_uri,  POI.isManagedBy, ente_uri))

        g.add((addr_uri, RDF.type, CLV.Address))
        if street:
            full = f"{street} {housenumber}".strip()
            if city_raw:
                full += f" - {postcode} {city_raw}".strip()
            g.add((addr_uri, CLV.fullAddress,   Literal(full, lang="it")))
            g.add((addr_uri, CLV.streetAddress, Literal(f"{street} {housenumber}".strip())))
        if postcode:
            g.add((addr_uri, CLV["postCode"], Literal(postcode)))

        city_istat_uri, city_label = city_uri(city_raw)
        if city_istat_uri:
            g.add((addr_uri, CLV.hasCity, city_istat_uri))
            g.add((city_istat_uri, RDFS.label, Literal(city_label, lang="it")))
        elif city_raw:
            g.add((addr_uri, CLV.hasCity, Literal(city_raw, lang="it")))

        g.add((addr_uri, CLV.hasCountry, country_ita))
        g.add((geom_uri, RDF.type,  GSP.Geometry))
        g.add((geom_uri, GSP.asWKT, Literal(f"POINT({lon} {lat})", datatype=GSP.wktLiteral)))

    log.info("Grafo: %d triple.", len(g))
    return g


def load_mock_data():
    return [
        {"type":"node","id":123456001,"lat":40.3515,"lon":18.1750,
         "tags":{"emergency":"defibrillator","name":"DAE Municipio Lecce",
                 "addr:street":"Via Umberto I","addr:housenumber":"13",
                 "addr:city":"Lecce","addr:postcode":"73100",
                 "opening_hours":"Mo-Fr 08:30-13:30","operator":"Comune di Lecce"}},
        {"type":"node","id":123456002,"lat":40.3522,"lon":18.1765,
         "tags":{"emergency":"defibrillator","addr:street":"Piazza Sant'Oronzo",
                 "addr:city":"Lecce","addr:postcode":"73100",
                 "opening_hours":"24/7","operator":"Comune di Lecce"}},
        {"type":"node","id":123456003,"lat":41.1171,"lon":16.8719,
         "tags":{"emergency":"defibrillator","name":"DAE Policlinico Bari",
                 "addr:street":"Piazza Giulio Cesare","addr:housenumber":"11",
                 "addr:city":"Bari","addr:postcode":"70124",
                 "opening_hours":"24/7","operator":"AOU Policlinico Bari",
                 "phone":"+39 080 5592111"}},
        {"type":"node","id":123456004,"lat":40.4764,"lon":17.2290,
         "tags":{"emergency":"defibrillator","name":"DAE Stazione FS Taranto",
                 "addr:city":"Taranto","addr:postcode":"74121",
                 "opening_hours":"24/7","operator":"RFI"}},
        {"type":"node","id":123456005,"lat":40.6371,"lon":17.9435,
         "tags":{"emergency":"defibrillator","addr:street":"Corso Umberto I",
                 "addr:city":"Brindisi","addr:postcode":"72100",
                 "opening_hours":"Mo-Sa 09:00-19:00"}},
        {"type":"node","id":123456006,"lat":41.4622,"lon":15.5449,
         "tags":{"emergency":"defibrillator","name":"DAE Aeroporto Foggia",
                 "addr:city":"Foggia","addr:postcode":"71100",
                 "opening_hours":"24/7","operator":"ENAC"}},
    ]


def main():
    parser = argparse.ArgumentParser(description="Overpass -> RDF DAE Puglia")
    parser.add_argument("--output-dir", default="./output")
    parser.add_argument("--base-uri",   default="http://dati.regione.puglia.it/resource/dae/")
    parser.add_argument("--mock",       action="store_true")
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

    (out_dir / "last_update.json").write_text(json.dumps({
        "updated":        date.today().isoformat(),
        "elements_count": len(elements),
        "triples_count":  len(g),
        "source":         "OpenStreetMap via Overpass API",
        "bbox":           PUGLIA_BBOX,
        "query_tag":      "emergency=defibrillator",
    }, indent=2, ensure_ascii=False))
    log.info("Completato.")


if __name__ == "__main__":
    main()
