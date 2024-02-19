import asyncio
import datetime

import aiohttp
from aiohttp import ClientSession
from more_itertools import chunked

from models import People, engine, Session

MAX_CHUNK = 10


async def get_chunk_size(client: ClientSession):
    http_response = await client.get('https://swapi.dev/api/people/')
    json_data = await http_response.json()
    return int(json_data['count']) + 1


async def get_person(client, people_id: int):
    http_response = await client.get(f'https://swapi.dev/api/people/{people_id}')
    json_data = await http_response.json()
    json_data['id'] = people_id
    return json_data


async def get_details(client, list_links):
    lists = []
    for link in list_links:
        http_response = await client.get(link)
        json_data = await http_response.json()
        lists.append(list(json_data.keys())[0])
    return ', '.join(lists)


async def insert_db(people_list):
    models = []
    for json_item in people_list:
        models.append(People(
            id = json_item['id'],
            birth_year = json_item['birth_year'],
            eye_color = json_item['eye_color'],
            films = await get_details(client, json_item['films']),
            gender = json_item['gender'],
            hair_color = json_item['hair_color'],
            height = json_item['height'],
            homeworld = json_item['homeworld'],
            mass = json_item['mass'],
            name = json_item['name'],
            skin_color = json_item['skin_color'],
            species = await get_details(client, json_item['species']),
            starships = await get_details(client, json_item['starships']),
            vehicles = await get_details(client, json_item['vehicles'])
        ))
    async with Session() as session:
        session.add_all(models)
        await session.commit()


async def main():
    await init_db()
    client = aiohttp.ClientSession()
    total_people = await get_chunk_size(client)
    for chunk in chunked(range(1, total_people), MAX_CHUNK):
        coros = [get_person(client, people_id) for people_id in chunk]
        result = await asyncio.gather(*coros)
        await asyncio.create_task(insert_db(result))

    task_set = asyncio.all_tasks() - {asyncio.current_task()}
    await asyncio.gather(*task_set)

    await client.close()
    await engine.dispose()


start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
