use chrono::Local;
use sqlx::sqlite::SqlitePool;
use structopt::StructOpt;

#[derive(StructOpt)]
struct Args {
    #[structopt(subcommand)]
    cmd: Option<HizteryCmd>,
}

#[derive(StructOpt)]
enum HizteryCmd {
    Add { history_item: String },
    Upd { history_id: u64 },
    Del { history_id: u64 },
    Lst {},
}

#[async_std::main]
#[paw::main]
async fn main(args: Args) -> anyhow::Result<()> {
    // let db_url = env::var("DATABASE_URL").unwrap_or("sqlite:hiztery.db?mode=rwc".to_string());
    // let pool = SqlitePool::connect(&db_url).await?;
    let pool = SqlitePool::connect("sqlite:hiztery.db?mode=rwc").await?;

    let history_table = r#"
    CREATE TABLE IF NOT EXISTS "history"
    (
        "history_id"      INTEGER PRIMARY KEY NOT NULL,
        "session_id"      INTEGER             NOT NULL,
        "history_item"    TEXT                NOT NULL,
        "datetime"        DATETIME            NOT NULL,
        "executions"      INTEGER             NOT NULL
    );
    "#;

    let performance_table = r#"
    CREATE TABLE IF NOT EXISTS "performance" (
        "perf_id"     INTEGER NOT NULL PRIMARY KEY,
        "metrics"     FLOAT NOT NULL,
        "history_id"  INTEGER NOT NULL
        REFERENCES "history"(history_id) ON DELETE CASCADE ON UPDATE CASCADE
      );
    "#;

    let mut conn = pool.acquire().await?;
    sqlx::query(history_table).execute(&mut conn).await?;
    sqlx::query(performance_table).execute(&mut conn).await?;

    // let populate = r#"
    // INSERT INTO history (session_id, history_item, datetime, executions) VALUES
    // (101, 'now is the time1', '2021-07-13 15:52:01', 1),
    // (101, 'now is the time2', '2021-07-13 15:52:02', 1),
    // (101, 'now is the time3', '2021-07-13 15:52:03', 1),
    // (101, 'now is the time4', '2021-07-13 15:52:04', 1),
    // (101, 'now is the time5', '2021-07-13 15:52:05', 1);
    // "#;
    // sqlx::query(populate).execute(&mut conn).await?;

    match args.cmd {
        Some(HizteryCmd::Add { history_item }) => {
            for x in 1000..2000 {
                let hist_item_clone = history_item.clone();
                println!("Adding new history item description '{}'", &hist_item_clone);
                let start_time = Local::now();
                // println!("formatting time");
                let formatted_start_time = start_time.format("%Y-%m-%d %H:%M:%S").to_string();
                // println!("running add_bogus_entry");
                let history_id =
                    add_bogus_entry(&pool, x, hist_item_clone, formatted_start_time).await?;
                // println!("calculating end time");
                let end_time = Local::now();
                // println!("calculating duration");
                let insert_ms = end_time - start_time;
                println!(
                    "Added new history entry with id: {:?} in: {} ms",
                    history_id,
                    insert_ms.num_milliseconds()
                );
                let perf_update_id = update_perf(
                    &pool,
                    history_id,
                    (insert_ms.num_microseconds().unwrap() as f64 / 1000.0 as f64),
                )
                .await?;
                println!(
                    "Successfully updated the perf table with id: [{}]",
                    perf_update_id
                );
            }
        }
        Some(HizteryCmd::Upd { history_id }) => {
            println!("Marking todo {} as done", history_id);
            // if complete_todo(&pool, id).await? {
            //     println!("Todo {} is marked as done", id);
            // } else {
            //     println!("Invalid id {}", id);
            // }
        }
        Some(HizteryCmd::Del { history_id }) => {}
        Some(HizteryCmd::Lst {}) | None => {
            println!("Printing list of all todos");
            // list_todos(&pool).await?;
        }
    }

    Ok(())
}

async fn update_perf(pool: &SqlitePool, history_id: i64, insert_ms: f64) -> anyhow::Result<i64> {
    let mut conn = pool.acquire().await?;

    // println!("creating query");
    let query = format!(
        "INSERT INTO performance (metrics, history_id) VALUES ({}, {})",
        insert_ms, history_id
    );
    // println!("executing query {}", &query);
    let id = sqlx::query(&query)
        .execute(&mut conn)
        .await?
        .last_insert_rowid();

    Ok(id)
}

async fn add_bogus_entry(
    pool: &SqlitePool,
    session_id: i64,
    history_item: String,
    time_str: String,
) -> anyhow::Result<i64> {
    let mut conn = pool.acquire().await?;

    // println!("creating query");
    let query = format!(
        "INSERT INTO history (session_id, history_item, datetime, executions) VALUES (123, '{}', '{}', 1)",
        history_item, time_str
    );
    // println!("executing query {}", &query);
    let id = sqlx::query(&query)
        .execute(&mut conn)
        .await?
        .last_insert_rowid();

    // println!("returning id {}", id);

    // Insert the task, then obtain the ID of this row
    // let id = sqlx::query!(
    //     r#"
    //     INSERT INTO todos ( description )
    //     VALUES ( ?1 )
    //     "#,
    //     description
    // )
    // .execute(&mut conn)
    // .await?
    // .last_insert_rowid();

    Ok(id)
}
