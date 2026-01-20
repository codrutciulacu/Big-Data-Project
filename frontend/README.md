# STB Route Optimizer - Frontend

Dashboard interactiv pentru optimizarea frecventei autobuzelor STB pe baza cererii de pasageri.

## Rulare Rapida

```bash
# Instalare dependente
npm install

# Pornire dev server
npm run dev
```

Aplicatia va fi disponibila la `http://localhost:5173`

## Actualizare Date

Pentru a actualiza datele afisate in aplicatie:

1. **Genereaza datele din backend:**
   ```bash
   docker exec -it gtfs-ml python src/optimization/export_suggestions_json.py
   docker exec -it gtfs-ml python src/optimization/export_routes_geo.py
   ```

2. **Refresh in browser:**
   - Click pe butonul "Refresh Date" din header

## Structura

- **Dashboard** - KPI-uri si top rute prioritare
- **Harta** - Vizualizare interactiva cu filtre pe nivel de aglomerare
- **Grafice** - Evolutia pasagerilor si statistici

## Culori Harta

- 🔴 Rosu - Critic (>2000 pasageri/ora)
- 🟠 Portocaliu - Aglomerat (1200-2000)
- 🟡 Galben - Mediu (640-1200)
- 🟢 Verde - Normal (<640)
- ⚪ Gri - Fara date KPI

