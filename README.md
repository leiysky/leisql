# LeiSQL

**LeiSQL** is a toy project written by a drunk programmer [`leiysky`](https://github.com/leiysky).

So far, it only supports limited SQL syntax, and can not even provide data persistence. So it's not recommended to use it in production environment.

## Usage

You can build it from source code, with `cargo`:

```bash
cargo build
```

There is only a single binary executable file `leisql` in `target/debug` directory. You can run it with:

```bash
./target/debug/leisql
```

Which will start a `LeiSQL` server, listening on `localhost:5432` by default(and cannot be configured at all).

After starting the server, you can connect to it with `psql`:

```bash
psql -h localhost -p 5432
```

Then you can run SQL statements in `psql`, here's an example:

```sql
leiysky=> create table t(a int);
Something good happened
leiysky=> insert into t values(1),(2),(3);
Something good happened
leiysky=> select * from t;
 a 
---
 1
 2
 3
(3 rows)
```

## FAQ

Q: Why is it called LeiSQL?
A: Lei is my last name, and SQL is a database query language. LeiSQL is a SQL database written by Lei.

Q: Why is it written in Rust?
A: I have no idea.

Q: Why the data is gone after restarting the server?
A: Because it's a toy project, and I'm too lazy to implement data persistence.

Q: Why is it so slow?
A: Ditto.

Q: Why is it so buggy?
A: Ditto.

Q: Why is it so ugly?
A: I don't think so.

P.S. the last three `Q&A`s are written by `Copilot` automatically.