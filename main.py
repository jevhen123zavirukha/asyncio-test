import asyncio
import aiosqlite
import random
import os
import time

DB_PATH = "test.db"
TOTAL_RECORDS = 1000
CHUNK_SIZE = 200


async def create_table(db):
    await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            age INTEGER NOT NULL,
            city TEXT NOT NULL
        )
    """)


async def insert_user(db, name, age, city):
    await db.execute(
        "INSERT INTO users (name, age, city) VALUES (?, ?, ?)",
        (name, age, city)
    )


async def main():
    # Start clean (optional)
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    start_time = time.perf_counter()

    async with aiosqlite.connect(DB_PATH) as db:
        await create_table(db)

        names = ["Alice", "Bob", "Charlie", "Diana", "Ethan", "Fiona", "George", "Hannah"]
        cities = ["Prague", "Kyiv", "Warsaw", "Berlin", "Paris", "Madrid", "Rome", "Vienna"]

        tasks = []
        inserted = 0

        for _ in range(TOTAL_RECORDS):
            name = random.choice(names)
            age = random.randint(15, 70)
            city = random.choice(cities)
            tasks.append(asyncio.create_task(insert_user(db, name, age, city)))
            inserted += 1

            # Batch commit
            if len(tasks) == CHUNK_SIZE or inserted == TOTAL_RECORDS:
                await asyncio.gather(*tasks)
                await db.commit()
                tasks.clear()
                print(f" {inserted}/{TOTAL_RECORDS} records inserted")

        # Check total count
        async with db.execute("SELECT COUNT(*) FROM users") as cursor:
            total = (await cursor.fetchone())[0]
            print(f"\n Total rows in table: {total}")

        # Show few sample rows
        print("\n Sample records:")
        async with db.execute("SELECT * FROM users LIMIT 5") as cursor:
            async for row in cursor:
                print(row)

    end_time = time.perf_counter()
    print(f"\n Completed in {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(main())
