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

## Aggiornamento automatico

Il workflow GitHub Actions `.github/workflows/update_dae.yml` esegue ogni giorno alle **03:00 UTC**:

1. Query Overpass API (`emergency=defibrillator`, bbox Puglia)
2. Generazione Turtle + RDF/XML
3. Validazione con rdflib
4. Commit automatico se ci sono modifiche

## Uso manuale

```bash
pip install -r requirements.txt
python overpass_to_rdf.py
python overpass_to_rdf.py --mock   # test senza rete
```

## Fonte dati

- **OpenStreetMap** contributors, licenza [ODbL](https://opendatacommons.org/licenses/odbl/)
- Aggiornamento via [Overpass API](https://overpass-api.de/)

## Licenza

Dati RDF rilasciati con licenza **CC BY 4.0**.
