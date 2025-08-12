// We can import classes from other baml files
// import resume from ./resume.baml

class CypherQuery {
  query string @description("A valid Cypher query for the Kuzu database.")
}

class FinalAnswer {
  answer string @description("The final, user-facing answer in natural language.")
}


// This function translates a natural language question into a Cypher query
// by showing the LLM the database schema.
function GenerateCypher(question: string) -> CypherQuery {
  client CustomGemini2Flash

  prompt #"
    You are an expert Cypher query writer.
    Given the Kuzu graph schema below, convert the user's question into a single, valid Cypher query.
    Only output the query. Do not add any explanation.

    ## Kuzu Graph Schema:
    - Node: Person(name STRING, email STRING)
    - Node: Company(name STRING)
    - Node: Skill(name STRING)
    - Relationship: WORKED_AT(FROM Person TO Company, title STRING, duration STRING)
    - Relationship: HAS_SKILL(FROM Person TO Skill, level STRING)

    ## User Question:
    {{ question }}

    ## Cypher Query:
    {{ ctx.output_format }}
  "#
}

// This function takes the retrieved graph data and the original question
// to synthesize a final answer.
function SynthesizeAnswer(question: string, context: string) -> FinalAnswer {
  client CustomGemini2Flash

  prompt #"
    You are a helpful assistant.
    Answer the user's question based *only* on the provided context information.
    If the context does not contain the answer, say "I do not have enough information to answer that."

    ## Context from Graph Database:
    {{ context }}

    ## User Question:
    {{ question }}

    ## Final Answer:
    {{ ctx.output_format }}
  "#
}
