import asyncio
import datetime

import aiohttp
from aiohttp import ClientSession
from more_itertools import chunked

from models import People, engine, Session, init_db

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


async def get_details(client, link):
    if link is None:
        return None
    else:
        http_response = await client.get(link)
        json_data = await http_response.json()
        return json_data


async def insert_db(client, people_list):
    models = []

    for json_item in people_list:
        if json_item.get('films') is None:
            films = None
        else:
            coros_films = [get_details(client, i) for i in json_item.get('films')]
            result_films = await asyncio.gather(*coros_films)
            films = ' ,'.join([i.get('title') for i in result_films])

        if json_item.get('species') is None:
            species = None
        else:
            coros_species = [get_details(client, i) for i in json_item.get('species')]
            result_species = await asyncio.gather(*coros_species)
            species = ' ,'.join([i.get('name') for i in result_species])

        if json_item.get('starships') is None:
            starships = None
        else:
            coros_starships = [get_details(client, i) for i in json_item.get('starships')]
            result_starships = await asyncio.gather(*coros_starships)
            starships = ' ,'.join([i.get('name') for i in result_starships])

        if json_item.get('vehicles') is None:
            vehicles = None
        else:
            coros_vehicles = [get_details(client, i) for i in json_item.get('vehicles')]
            result_vehicles = await asyncio.gather(*coros_vehicles)
            vehicles = ' ,'.join([i.get('name') for i in result_vehicles])
        models.append(People(
            id=json_item['id'],
            birth_year=json_item.get('birth_year'),
            eye_color=json_item.get('eye_color'),
            films=films,
            gender=json_item.get('gender'),
            hair_color=json_item.get('hair_color'),
            height=json_item.get('height'),
            homeworld=json_item.get('homeworld'),
            mass=json_item.get('mass'),
            name=json_item.get('name'),
            skin_color=json_item.get('skin_color'),
            species=species,
            starships=starships,
            vehicles=vehicles
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

        await asyncio.create_task(insert_db(client, result))

    task_set = asyncio.all_tasks() - {asyncio.current_task()}
    await asyncio.gather(*task_set)

    await client.close()
    await engine.dispose()


start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
