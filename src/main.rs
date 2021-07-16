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
    Insert {
        history_item: String,
        rows_to_insert: i64,
    },
    Update {
        history_id: i64,
        history_item: String,
    },
    Delete {
        history_id: i64,
    },
    Select {},
}

#[derive(Debug, sqlx::FromRow)]
struct HistoryTable {
    history_id: i64,
    session_id: i64,
    history_item: String,
    // This is why datetime is a string
    // https://www.sqlite.org/datatype3.html
    // https://www.sqlite.org/lang_datefunc.html
    datetime: String,
    executions: i64,
}

#[async_std::main]
#[paw::main]
async fn main(args: Args) -> anyhow::Result<()> {
    // let db_url = env::var("DATABASE_URL").unwrap_or("sqlite:hiztery.db?mode=rwc".to_string());
    // let pool = SqlitePool::connect(&db_url).await?;
    let pool = SqlitePool::connect("sqlite:hiztery.db?mode=rwc").await?;
    initialize_db(&pool).await?;

    match args.cmd {
        // cargo run -- insert "some history message" 5
        Some(HizteryCmd::Insert {
            history_item,
            rows_to_insert,
        }) => {
            for row in 0..rows_to_insert {
                let hist_item_clone = history_item.clone();
                let start_time = Local::now();
                // println!("formatting time");
                let formatted_start_time = start_time.format("%Y-%m-%d %H:%M:%S").to_string();
                // println!("running add_bogus_entry");
                let history_id =
                    insert_history_item(&pool, row + 100, &hist_item_clone, formatted_start_time)
                        .await?;
                // println!("calculating end time");
                let end_time = Local::now();
                // println!("calculating duration");
                let insert_time_ms = if let Some(time) = (end_time - start_time).num_microseconds()
                {
                    time as f64 / 1000.0 as f64
                } else {
                    0.0 as f64
                };
                print!(
                    "Insert new history item: [{}] with id: [{:?}] in: [{}] ms.",
                    &hist_item_clone, history_id, insert_time_ms
                );
                let perf_update_id = insert_perf_with(&pool, history_id, insert_time_ms).await?;
                println!(
                    " Successfully updated perf table with id: [{}] perf: [{}].",
                    perf_update_id, insert_time_ms
                );
            }
        }
        Some(HizteryCmd::Update {
            history_id,
            history_item,
        }) => {
            // cargo run -- update 1 "some string"
            println!(
                "Updating history_id: [{}] with history_item: [{}]",
                history_id, history_item
            );
            let row = update_history_item(&pool, history_id, history_item).await?;
            println!("Updated row: [{}]", row);
        }
        Some(HizteryCmd::Delete { history_id }) => {
            // cargo run -- delete 3
            println!("Deleting history item: [{}]", history_id);
            let res = delete_history_item(&pool, history_id).await?;
            println!("Deleted row count: [{}]", res);
        }
        Some(HizteryCmd::Select {}) | None => {
            // cargo run -- select
            println!("List of top 5 history items");
            let output = select_star(&pool).await?;
            let mut count = 0;
            for x in output {
                if count > 5 {
                    break;
                } else {
                    println!("ItemNum: [{}] Row: [{:?}]", count, x);
                    count += 1;
                }
            }
        }
    }

    Ok(())
}

async fn delete_history_item(pool: &SqlitePool, id: i64) -> anyhow::Result<u64> {
    let mut conn = pool.acquire().await?;

    let query = format!("DELETE FROM history WHERE history_id = '{}';", id);

    let res = sqlx::query(&query)
        .execute(&mut conn)
        .await?
        .rows_affected();

    Ok(res)
}

async fn update_history_item(pool: &SqlitePool, id: i64, text: String) -> anyhow::Result<u64> {
    let mut conn = pool.acquire().await?;

    let query = format!(
        "UPDATE history SET (history_item) = '{}' WHERE history_id = {}",
        text, id
    );

    let res = sqlx::query(&query)
        .execute(&mut conn)
        .await?
        .rows_affected();

    Ok(res)
}

async fn select_star(pool: &SqlitePool) -> anyhow::Result<Vec<HistoryTable>> {
    let mut conn = pool.acquire().await?;

    let query = r#"
    SELECT history_id,  session_id,  history_item,  datetime,  executions
    FROM history;
    "#;

    // let query_output: Vec<HistoryTable> = sqlx::query_as!(HistoryTable, "select * from history")
    //     .fetch_all(&mut conn)
    //     .await?;

    let query_output: Vec<HistoryTable> = sqlx::query_as(&query).fetch_all(&mut conn).await?;
    Ok(query_output)
}

async fn initialize_db(pool: &SqlitePool) -> anyhow::Result<bool> {
    let history_table = r#"
    CREATE TABLE IF NOT EXISTS "history"
    (
        "history_id"      INTEGER PRIMARY KEY NOT NULL,
        "session_id"      INTEGER             NOT NULL,
        "history_item"    TEXT                NOT NULL,
        "datetime"        TEXT                NOT NULL,
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

    Ok(true)
}

async fn insert_perf_with(
    pool: &SqlitePool,
    history_id: i64,
    insert_ms: f64,
) -> anyhow::Result<i64> {
    let mut conn = pool.acquire().await?;

    // println!("creating query");
    let query = format!(
        "INSERT INTO performance (metrics, history_id) VALUES ({}, {})",
        insert_ms, history_id
    );
    // println!("executing query '{}'", &query);
    let id = sqlx::query(&query)
        .execute(&mut conn)
        .await?
        .last_insert_rowid();

    Ok(id)
}

async fn insert_history_item(
    pool: &SqlitePool,
    session_id: i64,
    history_item: &String,
    time_str: String,
) -> anyhow::Result<i64> {
    let mut conn = pool.acquire().await?;

    // println!("creating query");
    let query = format!(
        "INSERT INTO history (session_id, history_item, datetime, executions) VALUES ({}, '{}', '{}', 1)",
        session_id, history_item, time_str
    );
    // println!("executing query {}", &query);
    let id = sqlx::query(&query)
        .execute(&mut conn)
        .await?
        .last_insert_rowid();

    // println!("returning id {}", id);

    Ok(id)
}
