import os
import sys
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import asyncio
from baml_client.async_client import BamlAsyncClient
from baml_client.types import MarketSentimentEntity
from baml_client.runtime import DoNotUseDirectlyCallManager

os.environ["GOOGLE_API_KEY"] = ""

article = """
A heavy blow was dealt to OmniCure Health Ltd. today as the pharmaceutical
giant announced that its highly anticipated Alzheimer's drug, "CogniClear,"
failed to meet its primary endpoints in its Phase 3 clinical trial.
"""

async def main():
    client = BamlAsyncClient(DoNotUseDirectlyCallManager({}))
    result: MarketSentimentEntity = await client.ExtractSentiment(article)
    print(result)

if __name__ == "__main__":
    asyncio.run(main())