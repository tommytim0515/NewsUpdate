# Get News Updated from Tushare and Stored in MongoDB

## MongoDB Module (mongodb.py)

- Authentication control: access the database through connection string (URL), you can generate the connection string in two ways:
  - Inputting relevant information such as user, password, host, port, etc.
  - Input the connection string directly.
- Choosing which database and which collection to work on.
- Data insertion: most of the time, the data are got in the form of DataFrame. Process the DataFrame format and generate index with date. Store the data into the database in concurrency.
- Data inquire: We need to get the latest date of the source in the database, so every time you want to fetch data, you need to check the time information in the database.

---

## Pipeline Module (pipeline.py)

- ```Pipeline``` is used for an intermediate of transferring the data. Obtaining data and storing data are both I/O bound tasks, in order to improve the performance of the program, I am trying to implement concurrency method. ```Pipeline``` derived from ```queue.Queue``` can help store the data temporarily and reduce race condition.

---

## Tushare Module (tushare_get.py)

- Get the news from correct source and convert it into correct form.

