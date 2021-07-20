#![allow(dead_code)]

pub mod database;
pub mod history_item;

use std::io::{BufReader, Read};
use std::{fs::File, path::PathBuf};
// use async_std::io::BufReader;
use chrono::Local;
use database::{Database, Sqlite};
use log::debug;
use simplelog::*;
// use eyre::Result;
use crate::history_item::HistoryItem;
use sqlx::sqlite::SqlitePool;
use std::convert::TryInto;
use std::io::BufRead;
use std::io::{Seek, SeekFrom};
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
    Import {
        nushell_history_filepath: String,
    },
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
    CombinedLogger::init(vec![
        TermLogger::new(
            LevelFilter::Debug,
            Config::default(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        ),
        WriteLogger::new(
            LevelFilter::Debug,
            Config::default(),
            File::create("my_rust_binary.log").unwrap(),
        ),
    ])
    .unwrap();

    debug!("starting main");
    // let result = first_attempt(args).await?;
    let results = second_attempt(args).await?;

    Ok(results)
}

async fn second_attempt(args: Args) -> anyhow::Result<()> {
    debug!("starting second_attempt");
    // let pool = SqlitePool::connect("sqlite:hiztery.db?mode=rwc").await?;
    // initialize_db(&pool).await?;

    let db_path =
        PathBuf::from("C:\\Users\\dschroeder\\source\\repos\\forks\\sql\\hiztery\\hizzy.db");
    // let sqlite = Sqlite::new(db_path).await?;
    let mut sqlite = match Sqlite::new(db_path).await {
        Ok(r) => r,
        Err(e) => anyhow::bail!("unexpected error: {}", e),
    };

    match args.cmd {
        // cargo run -- insert "some history message" 5
        Some(HizteryCmd::Insert {
            history_item,
            rows_to_insert,
        }) => {
            debug!("Insert with {} {}", &history_item, rows_to_insert);
            // for row in 0..rows_to_insert {
            //     let hist_item_clone = history_item.clone();
            //     let start_time = Local::now();
            //     // println!("formatting time");
            //     let formatted_start_time = start_time.format("%Y-%m-%d %H:%M:%S").to_string();
            //     // println!("running add_bogus_entry");
            //     let history_id =
            //         insert_history_item(&pool, row + 100, &hist_item_clone, formatted_start_time)
            //             .await?;
            //     // println!("calculating end time");
            //     let end_time = Local::now();
            //     // println!("calculating duration");
            //     let insert_time_ms = if let Some(time) = (end_time - start_time).num_microseconds()
            //     {
            //         time as f64 / 1000.0 as f64
            //     } else {
            //         0.0 as f64
            //     };
            //     print!(
            //         "Insert new history item: [{}] with id: [{:?}] in: [{}] ms.",
            //         &hist_item_clone, history_id, insert_time_ms
            //     );
            //     let perf_update_id = insert_perf_with(&pool, history_id, insert_time_ms).await?;
            //     println!(
            //         " Successfully updated perf table with id: [{}] perf: [{}].",
            //         perf_update_id, insert_time_ms
            //     );
            // }
        }
        Some(HizteryCmd::Update {
            history_id,
            history_item,
        }) => {
            debug!("Update with id: {} item: {}", history_id, &history_item);
            // // cargo run -- update 1 "some string"
            // println!(
            //     "Updating history_id: [{}] with history_item: [{}]",
            //     history_id, history_item
            // );
            // let row = update_history_item(&pool, history_id, history_item).await?;
            // println!("Updated row: [{}]", row);
        }
        Some(HizteryCmd::Delete { history_id }) => {
            debug!("Delete with id: {}", history_id);
            // // cargo run -- delete 3
            // println!("Deleting history item: [{}]", history_id);
            // let res = delete_history_item(&pool, history_id).await?;
            // println!("Deleted row count: [{}]", res);
        }
        Some(HizteryCmd::Select {}) | None => {
            debug!("Select");
            // // cargo run -- select
            // println!("List of top 5 history items");
            // let output = select_star(&pool).await?;
            // let mut count = 0;
            // for x in output {
            //     if count > 5 {
            //         break;
            //     } else {
            //         println!("ItemNum: [{}] Row: [{:?}]", count, x);
            //         count += 1;
            //     }
            // }
        }
        Some(HizteryCmd::Import {
            nushell_history_filepath,
        }) => {
            debug!("Import with file: {}", &nushell_history_filepath);
            let file = File::open(nushell_history_filepath);
            let mut reader = BufReader::new(file.unwrap());
            let lines = count_lines(&mut reader)?;
            debug!("Lines: {}", lines);

            let mut history_vec = vec![];

            for (idx, line) in reader.lines().enumerate() {
                // println!("{}", line?);
                let time = chrono::Utc::now();
                let offset = chrono::Duration::seconds(idx.try_into().unwrap());
                let time = time - offset;

                // self.counter += 1;

                history_vec.push(HistoryItem::new(
                    None,
                    line?.trim_end().to_string(),
                    String::from("unknown"),
                    -1,
                    -1,
                    None,
                    time,
                ));
            }

            debug!("Preparing for save_bulk");
            let result = sqlite.save_bulk(&history_vec).await;
            let cnt = match sqlite.history_count().await {
                Ok(c) => c,
                _ => 0i64,
            };
            debug!("Imported [{}] history entries", cnt);
        }
    }

    Ok(())
}

fn count_lines(buf: &mut BufReader<impl Read + Seek>) -> anyhow::Result<usize> {
    let lines = buf.lines().count();
    buf.seek(SeekFrom::Start(0))?;

    Ok(lines)
}

async fn first_attempt(args: Args) -> anyhow::Result<()> {
    // Get the pid
    // let pid = process::id();

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
        Some(HizteryCmd::Import {
            nushell_history_filepath,
        }) => {}
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
