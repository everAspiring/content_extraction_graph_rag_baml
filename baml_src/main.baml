class PersonName {
  first string? @alias("first name") @description("The first name of the person.")

  last string? @alias("last name") @description("The last name of the person.")

  middle string? @alias("middle name") @description("The middle name of the person.")

} 


function ExtractPerson(record: string) -> PersonName {
  client CustomGemini2Flash
  prompt #"
    Extract the person's information using ONLY the information provided in the note.

    
    {{ ctx.output_format }}

    {{ _.role('user') }}

    {{ record }}
  "#
}


test ExtractPerson {
  functions [ExtractPerson]
  args {
    record #"
    This is about Ms. Swathi P Pattapurathi.
    "#
  }

  @@assert(middle, {{ this.name.middle == "P" }})
}


