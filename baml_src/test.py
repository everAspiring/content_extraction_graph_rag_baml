
import {b} from "@/baml_client";

async function main() {
    const response = await b.ExtractPerson("");
    print(response);
}

main()