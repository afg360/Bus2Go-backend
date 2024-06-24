from urllib.parse import quote
import asyncio
import aiohttp


base_url = "http://127.0.0.1:8000/api/realtime/stm"

async def test(route_id: str, trip_headsign: str, stop_name: str):
    stop_name = quote(stop_name)
    url = f"{base_url}/?route_id={route_id}&trip_headsign={trip_headsign}&stop_name={stop_name}"
    print(url)
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()


async def main():
    test_cases = [
        {
            "route_id": "165",
            "trip_headsign": "Nord",
            "stop_name": "Côte-des-Neiges / Jean-Talon"
        },
        {
            "route_id": "103",
            "trip_headsign": "Est",
            "stop_name": "de Monkland / Royal"
        },
        {
            "route_id": "23984",
            "trip_headsign": "Est",
            "stop_name": "Côte-des-Neiges / Jean-Talon"
        }
    ]
    tasks = [test(test_case["route_id"],
                  test_case["trip_headsign"],
                  test_case["stop_name"]) for test_case in test_cases]
    results = await asyncio.gather(*tasks)
    print(results)


if __name__ == "__main__":
    asyncio.run(main())
