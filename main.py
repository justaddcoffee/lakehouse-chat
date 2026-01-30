#!/usr/bin/env python3
"""
Lakehouse Chat - Natural language interface to the KBase/BERDL Data Lakehouse.

A demo app that converts natural language questions into SQL queries,
executes them against the lakehouse, and explains the results using Claude.

Usage:
    python main.py

Environment variables required:
    KB_AUTH_TOKEN - KBase authentication token
    ANTHROPIC_API_KEY - Anthropic API key for Claude (or OAuth token)
"""

import asyncio
import os
import httpx
from dotenv import load_dotenv
from nicegui import ui, run, background_tasks

load_dotenv()

BERDL_API_URL = "https://hub.berdl.kbase.us/apis/mcp/delta/tables/query"

SCHEMA_CONTEXT = """You are a SQL assistant for the BERDL/KBase Data Lakehouse.

## Available Databases

Key databases include:
- `nmdc_core` - NMDC microbiome data (traits, taxonomy, studies)
- `kbase_ke_pangenome` - Pangenomic data with GTDB taxonomy (genome, gene, gene_cluster)
- `kbase_genomes` - KBase genome collection
- `kbase_uniprot_*` - UniProt reference data

## nmdc_core Tables

1. trait_features - Predicted microbial traits per sample
   - sample_id (string)
   - 90+ columns like: "functional_group:plastic_degradation", "functional_group:methanogenesis",
     "functional_group:nitrogen_fixation", "functional_group:oil_bioremediation",
     "functional_group:human_pathogens_all", "functional_group:cellulolysis", etc.
   - Values are numeric (0 = absent, >0 = present/abundance)

2. abiotic_features - Environmental measurements per sample
   - sample_id, annotations_ph, annotations_temp_has_numeric_value,
     annotations_depth_has_numeric_value, annotations_tot_org_carb_has_numeric_value, etc.

3. taxonomy_dim - Taxonomy hierarchy (2.6M records)
   - taxid, kingdom, phylum, class, order, family, genus, species

4. study_table - Study metadata (48 studies)
   - study_id, name, ecosystem, ecosystem_type, ecosystem_subtype

5. cog_categories - COG functional categories
   - cog_id, category_code, category_name, description

## kbase_ke_pangenome Tables

- genome - genome_id, gtdb_species_clade_id, and other genome metadata
- gene - gene information
- gene_cluster - gene cluster data

## SQL Rules

- Always use fully qualified table names: database.table_name (e.g., nmdc_core.trait_features)
- Columns with special characters need double quotes: "functional_group:plastic_degradation"
- Keep queries simple and limit results (LIMIT 20 unless user asks for more)
- Use standard SQL syntax (SELECT, JOIN, WHERE, GROUP BY, ORDER BY, etc.)
- For aggregations, use COUNT(*), SUM(), AVG(), etc.
- Return ONLY the SQL query, no explanation, no markdown code blocks

## Example Queries

```sql
-- Count samples with a trait
SELECT COUNT(*) FROM nmdc_core.trait_features WHERE "functional_group:plastic_degradation" > 0

-- List kingdoms in taxonomy
SELECT DISTINCT kingdom FROM nmdc_core.taxonomy_dim

-- Count studies by ecosystem
SELECT ecosystem_type, COUNT(*) as count FROM nmdc_core.study_table GROUP BY ecosystem_type

-- Join example
SELECT a.sample_id, b.name FROM nmdc_core.trait_features a JOIN nmdc_core.study_table b ON a.study_id = b.study_id
```
"""


def query_berdl(sql: str) -> dict:
    """Execute SQL query against BERDL and return results."""
    token = os.getenv("KB_AUTH_TOKEN")
    if not token:
        return {"error": "KB_AUTH_TOKEN not set in .env"}

    # Basic SQL injection prevention
    if any(keyword in sql.upper() for keyword in ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER"]):
        return {"error": "Only SELECT queries are allowed"}

    try:
        response = httpx.post(
            BERDL_API_URL,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={"query": sql, "limit": 100},
            timeout=30.0,
        )
        data = response.json()

        # Check for API errors
        if data.get("error") or data.get("error_type"):
            return {"error": data.get("message", "Unknown API error")}

        return data
    except httpx.TimeoutException:
        return {"error": "Query timed out after 30 seconds"}
    except Exception as e:
        return {"error": str(e)}


def _run_claude_cli(prompt: str) -> str:
    """Run Claude CLI synchronously (called from thread pool)."""
    import subprocess
    import re

    env = os.environ.copy()
    oauth_token = os.getenv("ANTHROPIC_API_KEY", "")
    if oauth_token.startswith("sk-ant-oat"):
        env["CLAUDE_CODE_OAUTH_TOKEN"] = oauth_token
        env.pop("ANTHROPIC_API_KEY", None)

    try:
        result = subprocess.run(
            ["claude", "-p", "--output-format", "text", "--model", "sonnet", "--", prompt],
            capture_output=True,
            text=True,
            timeout=60,
            env=env,
        )

        if result.returncode != 0:
            return f"-- Error: Claude CLI failed: {result.stderr[:200]}"

        output = result.stdout.strip()

        # Extract SQL from markdown code blocks if present
        sql_match = re.search(r'```(?:sql)?\s*([\s\S]*?)```', output)
        if sql_match:
            return sql_match.group(1).strip()

        # If output starts with SELECT/WITH/EXPLAIN etc, it's probably raw SQL
        if output.upper().startswith(('SELECT', 'WITH', 'EXPLAIN', 'DESCRIBE', 'SHOW')):
            return output

        # Otherwise return as-is (might be an explanation for explain_results)
        return output
    except subprocess.TimeoutExpired:
        return "-- Error: Claude CLI timed out"
    except FileNotFoundError:
        return "-- Error: Claude CLI not found"
    except Exception as e:
        return f"-- Error: {e}"


async def generate_sql(user_question: str, conversation_history: list = None, previous_sql: str = None, error: str = None) -> str:
    """Use Claude CLI to generate SQL from natural language question."""
    history_context = ""
    if conversation_history:
        history_parts = []
        for entry in conversation_history[-5:]:  # Last 5 exchanges for context
            history_parts.append(f"User: {entry['question']}")
            history_parts.append(f"SQL: {entry['sql']}")
            if entry.get('result_summary'):
                history_parts.append(f"Result: {entry['result_summary']}")
        history_context = "\n\nPrevious conversation:\n" + "\n".join(history_parts)

    if previous_sql and error:
        prompt = f"""{SCHEMA_CONTEXT}{history_context}

The user asked: {user_question}

I tried this SQL but it failed:
{previous_sql}

Error: {error}

Fix the SQL query. Return ONLY the raw SQL, no explanation, no markdown code blocks."""
    else:
        prompt = f"""{SCHEMA_CONTEXT}{history_context}

Write a SQL query to answer: {user_question}

Return ONLY the raw SQL query, no explanation, no markdown code blocks, no backticks."""

    return await run.io_bound(_run_claude_cli, prompt)


async def explain_results(question: str, sql: str, results: dict) -> str:
    """Use Claude CLI to explain query results in plain English."""
    if "error" in results:
        return f"**Query failed:** {results['error']}"

    result_data = results.get("result", [])
    result_summary = str(result_data[:10])
    total_rows = results.get("pagination", {}).get("total_count", len(result_data))

    prompt = f"""The user asked: "{question}"

I ran this SQL: {sql}

Results ({total_rows} rows total, showing first 10):
{result_summary}

Explain what we found in 2-3 sentences. Be specific about numbers and findings."""

    return await run.io_bound(_run_claude_cli, prompt)


@ui.page('/')
async def main_page():
    """Main page with chat interface."""
    import json

    with ui.column().classes("w-full max-w-3xl mx-auto p-4"):
        ui.label("Lakehouse Chat").classes("text-2xl font-bold mb-2")
        ui.label("Ask questions about the KBase/BERDL data lakehouse in plain English").classes("text-gray-500 mb-2")

        # Connection status
        with ui.row().classes("items-center gap-2 mb-4"):
            status_dot = ui.icon("circle").classes("text-yellow-500 text-xs")
            status_text = ui.label("Checking connection...").classes("text-sm text-gray-500")

        async def check_connection():
            result = await run.io_bound(query_berdl, "SELECT 1 as test")
            if "error" in result:
                status_dot.classes("text-red-500", remove="text-yellow-500 text-green-500")
                status_text.set_text(f"Disconnected: {result['error'][:50]}")
            else:
                status_dot.classes("text-green-500", remove="text-yellow-500 text-red-500")
                status_text.set_text("Connected to BERDL")

        background_tasks.create(check_connection())

        # Chat message container
        chat_container = ui.column().classes("w-full space-y-4 mb-4")

        # Conversation history for context
        conversation_history = []

        # Input field (defined early so we can reference it)
        with ui.row().classes("w-full"):
            input_field = ui.input(placeholder="Ask about any data in the lakehouse...").classes("flex-grow")
            send_button = ui.button("Send").classes("ml-2")

        async def send_message():
            question = input_field.value
            if not question.strip():
                return

            input_field.value = ""

            # Add user message
            with chat_container:
                with ui.card().classes("w-full"):
                    ui.label(question).classes("font-medium")

            # Create progress card
            with chat_container:
                progress_card = ui.card().classes("w-full")
                with progress_card:
                    status_label = ui.label("Generating SQL...").classes("text-gray-500")
                    spinner = ui.spinner("dots")
                    sql_container = ui.column().classes("w-full")
                    response_container = ui.column().classes("w-full")
                    explanation_container = ui.column().classes("w-full")

            # Generate SQL with retry logic
            max_retries = 3
            sql = None
            results = None
            previous_sql = None
            error = None

            for attempt in range(max_retries):
                if attempt == 0:
                    status_label.set_text("Generating SQL...")
                else:
                    status_label.set_text(f"Fixing SQL (attempt {attempt + 1}/{max_retries})...")

                sql = await generate_sql(question, conversation_history, previous_sql, error)

                # Clear and update SQL display
                sql_container.clear()
                with sql_container:
                    with ui.expansion(f"Generated SQL (attempt {attempt + 1})", icon="code", value=True).classes("w-full mt-2"):
                        ui.code(sql, language="sql")

                status_label.set_text("Querying BERDL...")
                results = await run.io_bound(query_berdl, sql)

                # Check if query succeeded
                if "error" not in results:
                    break

                # Query failed - prepare for retry
                previous_sql = sql
                error = results.get("error", "Unknown error")

                # Show error in response container
                response_container.clear()
                with response_container:
                    ui.label(f"Attempt {attempt + 1} failed: {error[:100]}...").classes("text-red-500 text-sm")

            # Show final BERDL response
            response_container.clear()
            with response_container:
                with ui.expansion("BERDL Response", icon="data_object", value=True).classes("w-full mt-2"):
                    ui.code(json.dumps(results, indent=2), language="json")
            status_label.set_text("Generating explanation...")

            # Store in conversation history
            result_data = results.get("result", [])
            result_summary = f"{len(result_data)} rows returned"
            if result_data and len(result_data) <= 5:
                result_summary = str(result_data)
            elif result_data:
                result_summary = f"{len(result_data)} rows, first: {result_data[0]}"

            conversation_history.append({
                "question": question,
                "sql": sql,
                "result_summary": result_summary if "error" not in results else f"Error: {results['error'][:100]}"
            })

            # Explain results
            explanation = await explain_results(question, sql, results)

            # Show final explanation
            spinner.delete()
            status_label.delete()
            with explanation_container:
                ui.markdown(explanation)

        async def set_and_send(question: str):
            input_field.value = question
            await send_message()

        # Example questions
        ui.label("Try these:").classes("text-sm text-gray-500 mt-2")
        with ui.row().classes("flex-wrap gap-2 mb-4"):
            examples = [
                "How many samples have plastic degradation?",
                "What kingdoms are in the taxonomy?",
                "Show samples with methanogenesis",
                "Count studies by ecosystem type",
            ]
            for ex in examples:
                ui.button(ex, on_click=lambda e=ex: background_tasks.create(set_and_send(e))).props("flat dense").classes("text-xs")

        # Wire up input handlers
        input_field.on("keydown.enter", lambda: background_tasks.create(send_message()))
        send_button.on_click(lambda: background_tasks.create(send_message()))


ui.run(title="Lakehouse Chat", port=8081)
