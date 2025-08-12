from baml_client.async_client import b
import kuzu
import asyncio
import json
import os

DB_PATH = "data/resume_db"

async def retrieve(question: str, conn: kuzu.Connection) -> str:
    """
    Retrieves context from the Kuzu graph.
    1. Generates a Cypher query from the question using BAML.
    2. Executes the query on the Kuzu database.
    3. Formats the results as a string for the generation step.
    """
    print(f"1. Generating Cypher for question: '{question}'")
    cypher_query_obj = await b.GenerateCypher(question)
    query = cypher_query_obj.query
    print(f"   - Generated Query: {query}")

    print("2. Executing query on Kuzu...")
    results = conn.execute(query).get_as_df() # Get results as a pandas DataFrame
    print(f"   - Found {len(results)} results.")

    # Format results as a JSON string to pass to the next LLM call
    context = results.to_json(orient='records')
    return context

async def main():
    # Add a check to ensure the database exists before connecting.
    if not os.path.exists(DB_PATH):
        print(f"Error: Kuzu database not found at '{DB_PATH}'.")
        print("Please run the 'ingest.py' script first to create and populate the database.")
        return

    # Connect to the existing database
    db = kuzu.Database(DB_PATH)
    conn = kuzu.Connection(db)

    user_question = "What position did John Doe hold at Google?"

    # Step 1: Retrieve context from the graph
    graph_context = await retrieve(user_question, conn)

    # Step 2: Augment and Generate the final answer
    print("3. Synthesizing final answer...")
    final_answer_obj = await b.SynthesizeAnswer(question=user_question, context=graph_context)
    print("\n--- FINAL ANSWER ---")
    print(final_answer_obj.answer)

if __name__ == "__main__":
    asyncio.run(main())
