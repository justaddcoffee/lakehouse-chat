# Lakehouse Skills

![Lakehouse](.assets/lakehouse.png)

Skills for querying genomics data lakehouses including JGI (GOLD, IMG) and KBase/BERDL.

Skills are folders of instructions, scripts, and resources that Claude loads dynamically to improve performance on specialized tasks.

For more information, check out:
- [What are skills?](https://support.claude.com/en/articles/12512176-what-are-skills)
- [Using skills in Claude](https://support.claude.com/en/articles/12512180-using-skills-in-claude)
- [How to create custom skills](https://support.claude.com/en/articles/12512198-creating-custom-skills)

## Available Skills

| Skill | Description |
|-------|-------------|
| [kbase-query](./kbase-query/) | Query KBase/BERDL Datalake via REST API (pangenomics, NMDC, UniProt) |
| [jgi-lakehouse](./jgi-lakehouse/) | Query JGI Dremio Lakehouse (GOLD, IMG databases) |

## Databases Covered

### KBase/BERDL (via REST API)
- `kbase_ke_pangenome` - Pangenomic data with GTDB taxonomy
- `nmdc_core` - NMDC microbiome data
- `kbase_genomes` - KBase genome collection
- `kbase_uniprot_*` - UniProt reference data

### JGI Lakehouse (via Dremio SQL)
- GOLD - Genomes OnLine Database (genome project metadata)
- IMG Core - Integrated Microbial Genomes (genes, taxa, annotations)
- IMG Extended - Pathway and secondary metabolite data

## Use in Claude Code

### Option 1: Copy and paste (simplest)

Find the skill you need and copy the folder structure into the `.claude/skills/` folder of your repo.

- PROs: simplest, adaptable; if checked in to your repo, everyone (+actions) can use it
- CONs: harder to keep in sync with upstream changes

### Option 2: Via marketplace

Register this repository as a Claude Code Plugin marketplace:

```
/plugin marketplace add cmungall/lakehouse-skills
```

Then browse and install via `/plugin`:

```
> /plugin
  1. Browse and install plugins  <- select this
  2. Manage and uninstall plugins
  3. Add marketplace
  4. Manage marketplaces
```

Select `lakehouse-skills` and choose the skills you need.

### Dev use

Clone this repo and:

```
/plugin marketplace add /path/to/lakehouse-skills/.claude-plugin/marketplace.json
```

## Prerequisites

### KBase/BERDL
- `KBASE_TOKEN` environment variable (get from KBase JupyterHub: `BERDLSettings().KBASE_AUTH_TOKEN`)
- `jq` for JSON processing
- Tokens expire weekly

### JGI Lakehouse
- `pip install linkml-store[dremio]`
- `DREMIO_USER` and `DREMIO_PASSWORD` environment variables
- Access to JGI lakehouse (requires authorization)

## License

MIT
