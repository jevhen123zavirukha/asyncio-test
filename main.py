import asyncio
import aiosqlite


async def main():
    # Connect to (or create) a local SQLite database file
    async with aiosqlite.connect("test.db") as db:

        # Create a table if it does not exist yet
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                age INTEGER
            )
        """)

        # Define an async insert function
        async def make_request(name, age):
            # Insert one row into the users table
            await db.execute("INSERT INTO users (name, age) VALUES (?, ?)", (name, age))

        chunk = 200  # How many tasks to run at the same time
        tasks = []
        pended = 0

        # Create and run 10,000 async insert tasks
        for x in range(10000):
            tasks.append(asyncio.create_task(make_request(f"User{x}", x % 100)))
            pended += 1

            # Once we have 200 tasks, execute them together
            if len(tasks) == chunk or pended == 10000:
                await asyncio.gather(*tasks)  # Run all 200 at once
                await db.commit()  # Save to the database
                tasks = []  # Clear task list
                print(f"{pended} records inserted")


# Run the async main() function
asyncio.run(main())
