from baml_client.async_client import b
import kuzu
import os

# Sample resume data from your baml test case
RESUME_TEXT = """
John Doe
SOFTWARE ENGINEER

CONTACT
john.doe@email.com
(555) 123-4567
San Francisco, CA
linkedin.com/in/johndoe
github.com/johndoe

PROFILE
Experienced software engineer with 7+ years in backend development, cloud infrastructure, and machine learning.

EXPERIENCE
2019 – PRESENT
Senior Software Engineer | Google
- Led development of scalable backend systems for cloud products.

2016 – 2019
Software Engineer | Facebook
- Built RESTful APIs for social media analytics.

SKILLS
- Programming: Python (Advanced), Java (Intermediate)
- Cloud: AWS (Advanced)
"""

DB_PATH = "data/resume_db"

def create_schema(conn: kuzu.Connection):
    """Creates the graph schema in Kuzu if it doesn't exist."""
    print("Creating Kuzu schema...")
    conn.execute("CREATE NODE TABLE IF NOT EXISTS Person(name STRING, email STRING, PRIMARY KEY (name))")
    conn.execute("CREATE NODE TABLE IF NOT EXISTS Company(name STRING, PRIMARY KEY (name))")
    conn.execute("CREATE NODE TABLE IF NOT EXISTS Skill(name STRING, PRIMARY KEY (name))")
    conn.execute("CREATE REL TABLE IF NOT EXISTS WORKED_AT(FROM Person TO Company, title STRING, duration STRING)")
    conn.execute("CREATE REL TABLE IF NOT EXISTS HAS_SKILL(FROM Person TO Skill, level STRING)")
    print("Schema created successfully.")

async def main():
    # 1. Extract structured data using BAML
    print("Extracting data with BAML...")
    extracted_resume = await b.ExtractResume(resume=RESUME_TEXT)

    # 2. Setup Kuzu DB
    if not os.path.exists(DB_PATH):
        os.makedirs(DB_PATH)
    db = kuzu.Database(DB_PATH)
    conn = kuzu.Connection(db)

    # 3. Create Schema
    create_schema(conn)

    # 4. Load data into Kuzu using MERGE to avoid duplicates
    print("Loading data into Kuzu graph...")
    person_name = f"{extracted_resume.name.first} {extracted_resume.name.last}"
    conn.execute("MERGE (p:Person {name: $name, email: $email})", {"name": person_name, "email": extracted_resume.email})

    for exp in extracted_resume.experience:
        conn.execute("MERGE (c:Company {name: $company_name})", {"company_name": exp.company})
        conn.execute("MATCH (p:Person {name: $person_name}), (c:Company {name: $company_name}) MERGE (p)-[:WORKED_AT {title: $title, duration: $duration}]->(c)",
                     {"person_name": person_name, "company_name": exp.company, "title": exp.title, "duration": exp.duration})

    for skill in extracted_resume.skills:
        conn.execute("MERGE (s:Skill {name: $skill_name})", {"skill_name": skill.name})
        conn.execute("MATCH (p:Person {name: $person_name}), (s:Skill {name: $skill_name}) MERGE (p)-[:HAS_SKILL {level: $level}]->(s)",
                     {"person_name": person_name, "skill_name": skill.name, "level": skill.level})

    print(f"Graph populated for {person_name}.")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
