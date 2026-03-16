# DAE Puglia – RDF open data

Defibrillatori (DAE) della Regione Puglia estratti da **OpenStreetMap** via Overpass API
e pubblicati come Linked Open Data conformi a **OntoPiA** ([schema.gov.it](https://schema.gov.it)).

## File pubblicati

| File | Formato | Descrizione |
|------|---------|-------------|
| `output/dae_puglia.ttl` | RDF/Turtle | Grafo completo in Turtle |
| `output/dae_puglia.rdf` | RDF/XML | Grafo completo in RDF/XML |
| `output/last_update.json` | JSON | Metadati ultimo aggiornamento |

## Ontologie usate

| Prefisso | URI | Scopo |
|----------|-----|-------|
| `poi:` | `https://w3id.org/italia/onto/POI/` | Punti di Interesse (POI-AP_IT) |
| `clv:` | `https://w3id.org/italia/onto/CLV/` | Indirizzi e Luoghi (CLV-AP_IT) |
| `ti:` | `https://w3id.org/italia/onto/TI/` | Intervalli temporali (TI-AP_IT) |
| `l0:` | `https://w3id.org/italia/onto/l0/` | Agenti (L0) |
| `gsp:` | `http://www.opengis.net/ont/geosparql#` | Geometrie WKT (GeoSPARQL) |
| `geo:` | `http://www.w3.org/2003/01/geo/wgs84_pos#` | Coordinate WGS84 |

## Struttura di ogni risorsa DAE

```
dae:OSM-N{id}           poi:PointOfInterest
  dct:identifier        "osm:node/{id}"
  rdfs:label            "DAE - ..."@it
  poi:hasPointOfInterestType  tipo:DefibrillatoreDae
  poi:isAccessibleForFree     true
  poi:hasOpeningHoursSpecification  orari:...
  poi:isManagedBy             ente:...
  clv:hasAddress              addr:OSM-N{id}
  gsp:hasGeometry             geom:OSM-N{id}
  geo:lat / geo:long          decimal
  dct:source                  <https://www.openstreetmap.org/node/{id}>
  dct:modified                xsd:date

addr:OSM-N{id}          clv:Address
  clv:fullAddress / clv:streetAddress / clv:postCode
  clv:hasCity           istat:{BelfioreCode}
  clv:hasCountry        country:ITA

geom:OSM-N{id}          gsp:Geometry
  gsp:asWKT             "POINT(lon lat)"^^gsp:wktLiteral
```

## Aggiornamento automatico

Il workflow GitHub Actions `.github/workflows/update_dae.yml` esegue ogni giorno alle **03:00 UTC**:

1. Query Overpass API (`emergency=defibrillator`, bbox Puglia)
2. Generazione Turtle + RDF/XML
3. Validazione con rdflib
4. Commit automatico se ci sono modifiche

## Uso manuale

```bash
# Installa dipendenze
pip install -r requirements.txt

# Aggiornamento completo
python overpass_to_rdf.py

# Con opzioni
python overpass_to_rdf.py \
  --output-dir ./output \
  --base-uri http://dati.regione.puglia.it/resource/dae/

# Test locale senza rete (dati mock)
python overpass_to_rdf.py --mock
```

## Fonte dati

- **OpenStreetMap** © contributors, licenza [ODbL](https://opendatacommons.org/licenses/odbl/)
- Aggiornamento giornaliero via [Overpass API](https://overpass-api.de/)

## Licenza

I dati RDF prodotti sono rilasciati con licenza **CC BY 4.0**.
