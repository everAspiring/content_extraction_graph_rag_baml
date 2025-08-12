// Defining a data model.
class Resume {
  name Person
  email string
  experience Experience[]
  skills Skill[]
  answer string? @description("Answer to the question asked.")
}

class Experience {
  title string
  company string
  duration string
  description string
}

class Skill {
  name string
  level string @description(#"
    The proficiency level of the skill i.e., Beginner, Intermediate, Advanced
  "#)// e.g., Beginner, Intermediate, Advanced
}

class Person {
  first string @alias("first name") @description("The first name of the person.")

  last string @alias("last name") @description("The last name of the person.")

  middle string? @alias("middle name") @description("The middle name of the person.")

} 

// Create a function to extract the resume from a string.
function ExtractResume(resume: string, question: string?) -> Resume {
  // Specify a client as provider/model-name
  // you can use custom LLM params with a custom client name from clients.baml like "client CustomHaiku"
  client CustomGemini2Flash // Set GOOGLE_API_KEY to use this client.
  
  prompt #"
    Extract from this content and answer the question: 
    {{ resume }}

    Question: {{ question }}

    {{ ctx.output_format }}

    {{ _.role('user') }}
  "#
}



// Test the function with a sample resume. Open the VSCode playground to run this.


test unstructured_resume {
  functions [ExtractResume]
  args {
    resume #"
      John Doe, Software Engineer, lives in San Francisco. Email: john.doe@email.com, phone (555) 123-4567. 
Worked at Google as Senior Software Engineer since 2019, before that at Facebook from 2016 to 2019 as Software Engineer. 
Studied Computer Science at Stanford University, graduated in 2016. 
Skills include Python, Java, SQL, Node.js, AWS, Docker, Kubernetes, Machine Learning, NLP, CI/CD, Git, Jenkins. 
LinkedIn: linkedin.com/in/johndoe, GitHub: github.com/johndoe. 
Enjoys mentoring junior engineers and building scalable backend systems.
  "#,
    question #"
    What is the person's area based on his phone area code?
  "#  
  }
}


test structured_resume {
  functions [ExtractResume]
  args {
    resume #"
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

EDUCATION
2012-2016
Stanford University, CA
Bachelor of Science in Computer Science

TECHNICAL SKILLS
- Python, Java, SQL, Node.js
- AWS, Docker, Kubernetes
- Machine Learning, NLP
- CI/CD, Git, Jenkins

EXPERIENCE
2019 – PRESENT
Senior Software Engineer | Google
- Led development of scalable backend systems for cloud products.
- Designed and implemented CI/CD pipelines for microservices.
- Mentored junior engineers and led code reviews.

2016 – 2019
Software Engineer | Facebook
- Built RESTful APIs for social media analytics.
- Collaborated with cross-functional teams to deliver new features.
- Improved system reliability and reduced downtime by 30%.

SKILLS
- Programming: Python, Java, SQL
- Cloud: AWS, Docker, Kubernetes
- ML: NLP, Deep Learning
- DevOps: CI/CD, Git, Jenkins
"#,
question #"
    What is the person's name?
  "#  
  }
}


test ask_question {
  functions [ExtractResume]
  args {
    resume #"
      John Doe, Software Engineer, lives in San Francisco. Email: john.doe@email.com, phone (555) 123-4567.
Worked at Google as Senior Software Engineer since 2019, before that at Facebook from 2016 to 2019 as Software Engineer.
Studied Computer Science at Stanford University, graduated in 2016.
Skills include Python, Java, SQL, Node.js, AWS, Docker, Kubernetes, Machine Learning, NLP, CI/CD, Git, Jenkins.
LinkedIn: linkedin.com/in/johndoe, GitHub: github.com/johndoe.
Enjoys mentoring junior engineers and building scalable backend systems.
    "#,
    question #"
    what role do you think the person is suitable for?
  "#  
  }
}