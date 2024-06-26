from urllib.parse import quote
import asyncio
import aiohttp
import pdb

#for stm
base_url = "http://127.0.0.1:8000/api/realtime/v1"

async def test(agency: str, route_id: str, trip_headsign: str, stop_name: str, expected_code: int):
    route_id = quote(route_id)
    trip_headsign = quote(trip_headsign)
    stop_name = quote(stop_name)
    url = f"{base_url}/?agency={agency}&route_id={route_id}&trip_headsign={trip_headsign}&stop_name={stop_name}"
    #print(url)
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return {"answer": await response.text(), "expected": expected_code, "received": response.status}


async def main():
    test_cases = [
        # in this case, the stop DOES NOT exist in that direction, only in the opposite
        {
            "route_id": "165",
            "trip_headsign": "Sud",
            "stop_name": "Côte-des-Neiges / Jean-Talon",
            "expected_code": 404
        },
        {
            "route_id": "165",
            "trip_headsign": "Nord",
            "stop_name": "Côte-des-Neiges / Mackenzie",
            "expected_code": 200
        },
        {
            "route_id": "103",
            "trip_headsign": "Est",
            "stop_name": "de Monkland / Royal",
            "expected_code": 200
        },
        {
            "route_id": "23984",
            "trip_headsign": "Est",
            "stop_name": "Côte-des-Neiges / Jean-Talon",
            "expected_code": 418
        },
        {
            "route_id": "103o",
            "trip_headsign": "Est",
            "stop_name": "ghol",
            "expected_code": 418
        }
    ]
    tasks = [test("STM",
                  test_case["route_id"],
                  test_case["trip_headsign"],
                  test_case["stop_name"],
                  test_case["expected_code"]) for test_case in test_cases]
    results = await asyncio.gather(*tasks)
    print(results)


async def test2():
    while True:
        print(await test("STM", "103", "Est", "de Monkland / Royal", 200))
        await asyncio.sleep(12)


if __name__ == "__main__":
    asyncio.run(test2())
