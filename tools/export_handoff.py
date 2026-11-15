import os
from deltalake import DeltaTable

os.makedirs("data", exist_ok=True)
g = DeltaTable("lake/gold").to_pandas()
m = DeltaTable("lake/gold_materials").to_pandas()
q = DeltaTable("lake/quality_metrics").to_pandas().sort_values("run_ts")

g.to_parquet("data/gold.parquet", index=False)
m.to_parquet("data/gold_materials.parquet", index=False)
g.head(500).to_csv("data/gold_sample.csv", index=False)
m.drop(columns=["structure_json"]).head(500).to_csv("data/gold_materials_sample.csv", index=False)
q.to_csv("data/quality_metrics.csv", index=False)

multi = int((g["sources"].apply(len) > 1).sum())
text = f"""# ChemStream data handoff

## Files
- gold.parquet: {len(g)} molecules (one row per unique molecule). Structures only, NO property labels yet.
  Columns: inchikey, canonical_smiles, formula, mol_weight, sources, updated_ts.
  Molecules found in more than one source: {multi}.
- gold_materials.parquet: {len(m)} materials with crystal structure and labels.
  Columns: material_id, formula, reduced_formula, n_sites, volume, density, band_gap,
  formation_energy_per_atom, structure_json, has_band_gap, has_formation_energy, updated_ts.
  With band gap: {int(m.has_band_gap.sum())}. With formation energy: {int(m.has_formation_energy.sum())}.
- gold_sample.csv, gold_materials_sample.csv: first 500 rows for a quick look (no structures).
- quality_metrics.csv: pipeline data-quality history (for the dashboard).
- gold, gold_materials, quality_metrics folders: the original Delta tables (for version pinning).

## Read the data
import pandas as pd, json
from pymatgen.core import Structure
mols = pd.read_parquet("gold.parquet")
mats = pd.read_parquet("gold_materials.parquet")
s = Structure.from_dict(json.loads(mats.iloc[0].structure_json))

## Things to know
- Ions are merged with their neutral forms in the molecule table.
- Split materials by reduced_formula so related crystals do not land in both train and test.
- Check the materials sample for diversity (n_sites, elements) before training: it is the first
  part of the Materials Project in database order, not a random sample.
- Never train on anything from the exp/ folder (synthetic replay traffic).
"""
open("data/HANDOFF.md", "w", encoding="utf-8").write(text)
print("molecules", len(g), "| multi-source", multi, "| materials", len(m),
      "| with band gap", int(m.has_band_gap.sum()),
      "| with formation energy", int(m.has_formation_energy.sum()))