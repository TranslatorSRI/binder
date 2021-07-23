import asyncio
import logging

from async_lru import alru_cache
import httpx


async def run_cypher(query):
    """Run Cypher query on robokopkg.renci.org."""
    url = "https://robokopkg.renci.org/db/data/transaction/commit"
    async with httpx.AsyncClient(verify=False) as client:
        response = await client.post(
            url,
            json={
                "statements": [
                    {
                        "statement": query,
                    },
                ],
            },
            timeout=None,
        )
        return response.json()


@alru_cache()
async def get_degree(curie: str, logger=logging.getLogger(__name__)):
    """Get degree of node in ROBOKOP KG."""
    query = f"MATCH (n:named_thing {{id: \"{curie}\"}}) RETURN size((n)--())"
    logger.debug(f"Getting degree of {curie}")
    response = await run_cypher(query)
    degree = response["results"][0]["data"][0]["row"][0]
    logger.debug(f"Got degree of {curie}: {degree}")
    return degree


async def main():
    """Do the thing."""
    print(await get_degree("MONDO:0005737"))


if __name__ == "__main__":
    asyncio.run(main())
