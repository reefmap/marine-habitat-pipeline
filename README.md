# marine-habitat-pipeline
Automated pipeline for shallow-water habitat mapping via Google Earth Engine and local Python analysis.

## 1 Who is this for?

| Audience | Typical pain-point | How the pipeline helps |
|----------|-------------------|------------------------|
| **Coastal-zone managers & planners** | Need an up-to-date reef/lagoon map for zoning, EIAs or disaster response, but have no time for complex remote-sensing software. | Wizard hides the tech; delivers bilingual PDF map, vector contours and spreadsheet-ready habitat areas in <1 h (Cloud workflow). |
| **NGO field teams & community facilitators** | Want a **tactile 3-D model** for participatory planning sessions, often in low-bandwidth regions. | Local-only workflow runs fully offline once raw scenes are cached; outputs cardboard/PDF layer cuts or coloured OBJ mesh for 3-D print. |
| **Consultants & engineers** | Need a seamless land-sea DEM to feed hydrodynamic, dredge-plume or erosion models—fast, with QA logs. | Exports **topo-bathy merged GeoTIFF + uncertainty raster** and a full provenance JSON (dates, coefficients, bias match, % GEBCO infill). |
| **Academic & government scientists** | Require repeatable habitat layers (depth, wave, slope, CHL) with no licensing hurdles, plus an easy way to add LiDAR, multibeam or drone DEMs. | Open-source MIT licence; plug-in point for private DEM merge; NetCDF export option; PCA scree report to avoid over-fitting. |
| **Educators & outreach coordinators** | Need an engaging reef or coast model for schools, museums, VR or AR. | One checkbox produces a down-sampled 3-D mesh with vertex colours, plus ready-to-laser-cut contours. |

---
## 2 Why choose this pipeline instead of piecing data together by hand?

* **Hours or days of GIS steps → one guided form.**  
  The wizard asks a handful of plain-language questions, then automatically takes care of cloud masking, haze cleaning, bathymetry, deep-water gap-fill, wave energy, habitat clustering and export—no extra clicks or scripts.

* **Runs in the cloud *or* fully offline—same code-base.**  
  *Cloud* workflow exports only a few hundred MB, even for large AOIs;  
  *Local-only* workflow downloads raw Sentinel-2 scenes when buckets aren’t possible and can resume if the link drops.

* **Outputs designed for fieldwork and participatory mapping—not just pretty pictures.**  
  • `field_plan.mbtiles` = balanced GPS way-points for dive or snorkel teams.  
  • `sea_contours_model.gpkg` = ready-to-laser-cut layers for cardboard / plywood models.  
  • Bilingual legend comes from an editable CSV so you can swap in local language terms.

* **100 % open data, 100 % open source.**  
  Sentinel-2 imagery, global CHL (chlorophyll-a), free GEBCO bathymetry and open Copernicus elevation;  
  outputs are standard GeoTIFF, GeoPackage, MBTiles, GLB and PDF—no proprietary formats or licence fees.

* **Provenance you can hand to regulators.**  
  Every run writes a `provenance.json` with versions, date-ranges, coefficients, uncertainty metrics and processing times—making the workflow transparent and reproducible.

---

## 3 What is this and why would I use it?
You drop a shapefile of your **Area of Interest (AOI)** into the `data/` folder, run a short wizard, and the pipeline turns years of Sentinel-2 images into:

* **Print-ready map** (A4–A0) with bilingual legend and fine contours.  
* **Physical or digital 3-D model** (plywood/cardboard layers or coloured 3-D mesh).  
* **GIS package** (10 m depth grid, wave exposure, slope, reef-crest and river influence).  
* **GPS sampling plan** balanced by habitat and depth.  
* **Provenance log** so regulators can audit every step.

No manual coding, no deep remote‑sensing knowledge.

---

## 4 What you get (size per 100 km² AOI)

| File                         | Purpose                                  | Typical size per 100 km² |
| ---------------------------- | ---------------------------------------- | ------------------------ |
| `depth.tif`                  | 10 m bathymetry (0–25 m)                 | 2–3 MB                   |
| `topo_bathy_merged.tif`      | Land-sea DEM (sat + GEBCO + land DEM)    | 2–3 MB                   |
| `wave_energy_bed.tif`        | Depth-corrected wave-energy flux         | 2–3 MB                   |
| `dist_to_crest.tif`          | Distance to reef crest (if reef present) | 2–3 MB                   |
| `dist_to_river.tif`          | Distance to major river mouths (if any)  | 2–3 MB                   |
| `habitat.gpkg`               | Vector habitat polygons                  | 0.5–1 MB                 |
| `sea_contours_map.gpkg`      | 1 m contours for maps                    | 0.3–0.6 MB               |
| `sea_contours_model.gpkg`    | 20–30-layer model contours               | 0.1 MB                   |
| `reef_mesh.glb`              | 3-D mesh (coloured)                      | 2–5 MB                   |
| `field_plan.mbtiles`         | GPS points for survey apps               | < 0.05 MB                |
| Legend PDF & provenance JSON | Docs & recipe                            | 0.2 MB                   |

> **Rule of thumb:** finished outputs ≈ **10–15 MB per 100 km²**.  
> Temporary raw Sentinel‑2 downloads (Local‑only mode) can be 20–100 GB but are auto‑purged unless you keep them.

---
## 5 Data sources the pipeline uses

| Layer | Where it comes from | Mandatory? | Why it’s included |
|-------|--------------------|------------|-------------------|
| **Sentinel-2 L2A imagery (10 m)** | Google Earth Engine  / local ZIPs | **Yes** | Clear-water composite, bathymetry, habitat spectral info |
| **Chlorophyll-a (250 m, JAXA GCOM-C)** | Earth Engine collection | **Yes** | Tunes depth-inversion coefficients (m₀, m₁) and filters bloom days |
| **Copernicus global land DEM (30 m)** | Copernicus AWS | **Yes** | Land elevations for seamless land–sea DEM |
| **GEBCO 2023 bathymetry (≈463 m)** | GEBCO AWS | **Yes*** | Fills deep/no-data zones; bias-matched % blend |
| **HydroRIVERS (vector)** | WWF HydroRIVERS v1 | **Yes** | Distance-to-river driver (river influence) |
| **ERA5 Wave hindcast** | ECMWF Copernicus | **Yes** | Surface and depth-corrected wave-energy layers |
| **Allen Coral Atlas benthic & geomorphic** | ACA STAC API | Optional (toggle) | QC / comparison overlay |
| **ACOLITE-DSF corrected Sentinel-2** | Generated locally if selected | Optional | Extra haze & glint removal for sharper depth |
| **Private DEM (LiDAR / multibeam / chart grid)** | User GeoTIFF | Optional | Replaces GEBCO where available |
| **Depth calibration CSV** | User CSV (x,y,z) | Optional | Site-specific fit for m₀, m₁ |
| **Custom wave model (NetCDF)** | User file | Optional | Overrides ERA5 for high-res wave flux |
| **AOI shapefile** | You, the user | **Yes** | Processing footprint & map frame |

\* GEBCO is only pulled for cells deeper than ~25 m or where satellite depth is missing.

**Good to know**

* All compulsory layers are free and automatically downloaded the first time you run the tool.  
* Optional layers appear in the wizard as extra toggles or file-upload slots.  
* If you choose the Local-only workflow, Sentinel-2 raw scenes are cached locally after the first run; everything else is tiny and re-used.


## 5 Requirements

| Requirement | **Cloud workflow** (recommended) | **Local‑only** fallback |
|-------------|----------------------------------|-------------------------|
| AOI shapefile | ✔ | ✔ |
| Docker Desktop / Podman | ✔ | ✔ |
| Google Earth Engine account | ✔ | ✔ |
| Google Cloud‑Storage bucket | ✔ (one‑time) | ✖ |
| Internet speed | **10 Mbps+** (300–800 MB) | **10 Mbps+** if first download is large |
| Local disk | <2 GB | 20–100 GB (temporary) |

---

## 6 Quick start
```bash
git clone https://github.com/reefmap/marine-habitat-pipeline.git
cd marine-habitat-pipeline
docker build -t reefmap/mhp .            # one‑time build
# copy AOI shapefile into ./data/
docker run --rm -it -p 8888:8888   -v "$PWD/data:/data" -v "$PWD/tiles:/tiles" -v "$PWD/outputs:/outputs"   reefmap/mhp
```

Open JupyterLab and run:
```bash
python run_pipeline.py --web
```
The wizard:
1. Measures AOI area.
2. Shows download/time for *Cloud* vs *Local‑only* (10 Mbps assumption).
3. Asks purpose‑specific questions.
4. Runs workflow automatically.

Typical runtimes @10 Mbps: 15 km² reef → 30 min (Cloud) / 45 min (Local). 120 km² coast → 50 min / 3–4 h.

---

## 7 Cloud vs Local‑only at 10 Mbps

| | **Cloud workflow** | **Local‑only** |
|---|------------------|----------------|
| Download/100 km² | 300–800 MB | 4–20 GB (first) |
| Time @10 Mbps | 4–12 min | 1–4 h |
| Local CPU | 20–40 min | 1–8 h |
| Needs bucket | **Yes** | No |
| Fully offline after 1st run | No | **Yes** |

> **Recommendation:** pick **Cloud** whenever you can create a bucket.

---

## 8 Glossary
| Term | Meaning |
|------|---------|
| Clear‑water composite | Mosaic of only the clearest Sentinel‑2 days. |
| Haze cleaner | Optional ACOLITE‑DSF correction (removes haze & glint). |
| Deep‑water fill | GEBCO global bathymetry used below ~25 m. |
| Unsupervised clustering | Groups similar pixels with no training points. |

---

## 9 FAQ
| Q | Short answer |
|---|--------------|
| **Do I need a Google bucket?** | Only for Cloud workflow. |
| **Depth accuracy?** | 1–2 m RMSE (0–20 m) with haze cleaner; uncertainty raster included. |
| **Merge my LiDAR / multibeam?** | Yes – Advanced → Merge private DEM. |
| **Is Cloud faster at 10 Mbps?** | Yes—downloads MBs vs GBs. |
| **AOI crosses Date Line?** | Works fine (processed in Web‑Mercator). |
| **Legend in local language?** | Edit `legend_labels.csv`. |
| **Works in kelp areas?** | Yes. Algorithm auto‑adapts. |
| **Cloud‑storage cost?** | Few cents/run; first 5 GB/month free. |
| **NetCDF export?** | Choose raster format “NetCDF” in Analysis mode. |
| **Pause big downloads?** | Local downloads resume; Cloud exports keep running. |

---

## 10 Citation & licence
> Duncan Hume (2025). *Marine‑Habitat‑Pipeline* v1.0 — MIT Licence  
> https://github.com/reefmap/marine-habitat-pipeline  

Forks, stars ⭐ and pull‑requests welcome — **happy mapping!**  

## ⚠️ Earth Engine Authentication Required (One-Time Step)

This workflow requires access to the [Google Earth Engine API](https://earthengine.google.com/).
**Before using this pipeline (especially in Docker), you must authenticate your Google account and provide credentials.**

### **Step 1: Authenticate Earth Engine on Any Computer**

On any computer where you can install Python, run:

```sh
pip install earthengine-api
earthengine authenticate
```

* Follow the link and instructions in your browser to sign in with your Google account.
* This will create a credentials folder at:

  * **Windows:** `C:\Users\<YourUsername>\.config\earthengine\`
  * **Mac/Linux:** `~/.config/earthengine/`

---

### **Step 2: Copy Credentials to Your Docker Host**

If using Docker, copy the `.config/earthengine` folder from your authenticating computer to the same path on your Docker host machine.

---

### **Step 3: Mount Credentials When Starting Docker**

Add this to your `docker run` command:

**Windows:**

```sh
-v "C:\Users\<YourUsername>\.config\earthengine:/root/.config/earthengine"
```

**Mac/Linux:**

```sh
-v $HOME/.config/earthengine:/root/.config/earthengine
```

**Full example (Windows):**

```sh
docker run --rm -it ^
  -v "C:\Users\<YourUsername>\.config\earthengine:/root/.config/earthengine" ^
  -v "%cd%/data:/workflow/data" ^
  -v "%cd%/tiles:/workflow/tiles" ^
  -v "%cd%/outputs:/workflow/outputs" ^
  -p 8888:8888 ^
  reefmap/mhp
```

---

> **Note:**
>
> * You only need to authenticate once per Google account.
> * The credentials folder can be reused or shared between computers as needed.

---

### 🛑 If You Cannot Install Python on Your Computer

* Authenticate on a personal or other trusted computer, or ask your IT department for help.
* Copy the resulting `.config/earthengine` folder to your work/server environment.

---

### **Why is this step necessary?**

Google Earth Engine requires authentication for all API use.
All Docker, server, or cloud workflows with Earth Engine require this step due to Google’s security policies.

---

### **Troubleshooting**

If you see an error like

```
ERROR: Google Earth Engine credentials not found!
```

it means your Docker container cannot find your credentials.
Please review the steps above.

---
