#![allow(dead_code)]
#![allow(unused_variables)]

pub mod database;
pub mod history_item;

use crate::history_item::HistoryItem;
use chrono::{DateTime, NaiveDate};
use database::{Database, SearchMode, Sqlite};
use lazy_static::lazy_static;
use log::debug;
use simplelog::*;
use std::convert::TryInto;
use std::io::BufRead;
use std::io::{self, BufReader, Read};
use std::io::{Seek, SeekFrom};
use std::{fs::File, path::PathBuf};
use structopt::StructOpt;

lazy_static! {
    static ref PID: i64 = std::process::id().into();
}

#[derive(StructOpt)]
struct Args {
    #[structopt(subcommand)]
    cmd: Option<HizteryCmd>,
}

#[derive(StructOpt)]
#[structopt(about = "sql commands used with history")]
enum HizteryCmd {
    Insert {
        #[structopt(short = "t", long = "text")]
        history_item: String,
        #[structopt(short = "r", long = "rows_to_insert")]
        rows_to_insert: i64,
    },
    Update {
        #[structopt(short = "i", long = "id")]
        history_id: i64,
        // #[structopt(short = "u", long = "update_text")]
        // history_item: String,
    },
    Delete {
        #[structopt(short = "i", long = "id")]
        history_id: i64,
    },
    Select {
        #[structopt(short = "m", long = "max")]
        max: Option<usize>,
        #[structopt(short = "u", long = "unique")]
        unique: bool,
    },
    Import {
        #[structopt(short = "f", long = "file", name = "file path")]
        nushell_history_filepath: String,
    },
    Search {
        #[structopt(short = "m", long = "mode")]
        search_mode: String,
        #[structopt(short = "l", long = "limit")]
        limit: Option<i64>,
        #[structopt(short = "q", long = "query")]
        query: String,
    },
    Count {},
    Last {},
    First {},
    Load {
        #[structopt(short = "i", long = "id")]
        id: String,
    },
    Range {
        #[structopt(short = "f", long = "from")]
        from_date: String,
        #[structopt(short = "t", long = "to")]
        to_date: String,
    },
    Before {
        #[structopt(short = "f", long = "from")]
        from_date: String,
        #[structopt(short = "c", long = "count")]
        count: i64,
    },
    All {},
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

async fn second_attempt(args: Args) -> Result<(), sqlx::Error> {
    debug!("starting second_attempt");
    // let pool = SqlitePool::connect("sqlite:hiztery.db?mode=rwc").await?;
    // initialize_db(&pool).await?;

    let db_path =
        PathBuf::from("C:\\Users\\dschroeder\\source\\repos\\forks\\sql\\hiztery\\hizzy.db");
    // let sqlite = Sqlite::new(db_path).await?;
    let mut sqlite = match Sqlite::new(db_path).await {
        Ok(r) => r,
        // Err(e) => anyhow::bail!("unexpected error: {}", e),
        Err(e) => return Err(e),
    };

    match args.cmd {
        Some(HizteryCmd::Insert {
            history_item,
            rows_to_insert,
        }) => {
            // cargo run -- insert --text "happy birthday" --rows_to_insert 5
            debug!("Insert with {} {}", &history_item, rows_to_insert);
            for row in 0..rows_to_insert {
                let hi = HistoryItem::new(
                    None,
                    history_item.clone(),
                    "i_give_up".to_string(),
                    0,
                    0,
                    Some(*PID),
                    chrono::Utc::now(),
                );

                let result = sqlite.save(&hi).await?;
                // match result {
                //     Ok(r) => r,
                //     Err(e) => return Err(e),
                // }
            }
        }
        Some(HizteryCmd::Update {
            history_id,
            // history_item,
        }) => {
            // cargo run -- update -i 1
            debug!("Update with id: {}", history_id);
            let hi = HistoryItem::new(
                Some(history_id),
                "some | updated | command".to_string(),
                "i_updated".to_string(),
                1,
                0,
                Some(*PID),
                chrono::Utc::now(),
            );

            let result = sqlite.update(&hi).await?;
            // match result {
            //     Ok(r) => r,
            //     Err(e) => return Err(e),
            // }
        }
        Some(HizteryCmd::Delete { history_id }) => {
            // cargo run -- delete -i 3
            debug!("Deleting history item: [{}]", history_id);
            let res = sqlite.delete_history_item(history_id).await?;
            debug!("Deleted row count: [{}]", res);
        }
        Some(HizteryCmd::Select { max, unique }) => {
            // cargo run -- select -m 5 -u
            debug!("Selecting max: [{:?}] with unique: [{}]", max, unique);
            let output = sqlite.list(max, unique).await?;
            for (idx, item) in output.iter().enumerate() {
                debug!("ItemNum: [{}] Row: [{:?}]", idx, item);
            }
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
            let result = sqlite.save_bulk(&history_vec).await?;
            let cnt = sqlite.history_count().await?;
            //  {
            //     Ok(c) => c,
            //     _ => 0i64,
            // };
            debug!("Imported [{}] history entries", cnt);
        }
        Some(HizteryCmd::Search {
            search_mode,
            limit,
            query,
        }) => {
            // cargo run -- search -m "p" -q "code"
            debug!(
                "Searching with phrase: {}, limit: {:?}, mode: {}",
                &query, limit, &search_mode
            );
            let s_mode = match search_mode.as_ref() {
                "p" => SearchMode::Prefix,
                "f" => SearchMode::FullText,
                "z" => SearchMode::Fuzzy,
                _ => SearchMode::FullText,
            };

            let result = sqlite.search(limit, s_mode, &query).await;
            match result {
                Ok(r) => {
                    debug!("Found {} hits", r.len());
                    for (idx, hit) in r.iter().enumerate() {
                        debug!("Hit # [{}] History: [{}]", idx + 1, hit.command);
                    }
                }
                _ => debug!("No hits found for phrase: {}", &query),
            }
        }
        Some(HizteryCmd::Count {}) => {
            // cargo run -- count
            debug!("Counting history items.");
            let result = sqlite.history_count().await?;
            debug!("Found [{}] history items.", result);
        }
        Some(HizteryCmd::Last {}) => {
            // cargo run -- last
            debug!("Looking for the last history item.");
            let result = sqlite.last().await?;
            debug!("Found [{:?}] history items.", result);
        }
        Some(HizteryCmd::First {}) => {
            // cargo run -- first
            debug!("Looking for the first history item.");
            let result = sqlite.first().await?;
            debug!("Found [{:?}] history items.", result);
        }
        Some(HizteryCmd::Load { id }) => {
            // cargo run -- load -i 2800
            debug!("Looking for history item [{}].", &id);
            let result = sqlite.load(&id).await?;
            debug!("Found [{:?}] history items.", result);
        }
        Some(HizteryCmd::Range { from_date, to_date }) => {
            // cargo run -- range -f "2021-07-21" -t "2021-07-25"
            debug!(
                "Looking for history item between [{}] and [{}].",
                &from_date, &to_date
            );
            let f = NaiveDate::parse_from_str(&from_date, "%Y-%m-%d").unwrap();
            let t = NaiveDate::parse_from_str(&to_date, "%Y-%m-%d").unwrap();
            let f_utc = DateTime::<chrono::Utc>::from_utc(f.and_hms(0, 0, 0), chrono::Utc);
            let t_utc = DateTime::<chrono::Utc>::from_utc(t.and_hms(0, 0, 0), chrono::Utc);
            let result = sqlite.range(f_utc, t_utc).await?;

            debug!("Found {} hits", result.len());
            for (idx, hit) in result.iter().enumerate() {
                debug!("Hit # [{}] History: [{:?}]", idx + 1, hit);
            }
        }
        Some(HizteryCmd::Before { from_date, count }) => {
            // cargo run -- before -f "2021-07-21" -c 25
            debug!(
                "Looking for history item after [{}] with max [{}].",
                &from_date, count,
            );
            let f = NaiveDate::parse_from_str(&from_date, "%Y-%m-%d").unwrap();
            let f_utc = DateTime::<chrono::Utc>::from_utc(f.and_hms(0, 0, 0), chrono::Utc);
            let result = sqlite.before(f_utc, count).await?;

            debug!("Found {} hits", result.len());
            for (idx, hit) in result.iter().enumerate() {
                debug!("Hit # [{}] History: [{:?}]", idx + 1, hit);
            }
        }
        Some(HizteryCmd::All {}) => {
            // cargo run -- last
            debug!("Looking for all the history items.");
            let result = sqlite.query_history("select * from history_items").await?;
            debug!("Found {} hits", result.len());
            for (idx, hit) in result.iter().enumerate() {
                debug!("Hit # [{}] History: [{:?}]", idx + 1, hit);
            }
        }
        None => {}
    }

    Ok(())
}

fn count_lines(buf: &mut BufReader<impl Read + Seek>) -> Result<usize, io::Error> {
    let lines = buf.lines().count();
    buf.seek(SeekFrom::Start(0))?;

    Ok(lines)
}

////////////////////////////////////////////////////////////////
// everything below here is a hand-cranked test that works great
// i just switched to using the atuin trait/impl to make things
// easier for implementers
////////////////////////////////////////////////////////////////

// async fn first_attempt(args: Args) -> anyhow::Result<()> {
//     // Get the pid
//     // let pid = process::id();

//     // let db_url = env::var("DATABASE_URL").unwrap_or("sqlite:hiztery.db?mode=rwc".to_string());
//     // let pool = SqlitePool::connect(&db_url).await?;
//     let pool = SqlitePool::connect("sqlite:hiztery.db?mode=rwc").await?;
//     initialize_db(&pool).await?;

//     match args.cmd {
//         // cargo run -- insert "some history message" 5
//         Some(HizteryCmd::Insert {
//             history_item,
//             rows_to_insert,
//         }) => {
//             for row in 0..rows_to_insert {
//                 let hist_item_clone = history_item.clone();
//                 let start_time = Local::now();
//                 // println!("formatting time");
//                 let formatted_start_time = start_time.format("%Y-%m-%d %H:%M:%S").to_string();
//                 // println!("running add_bogus_entry");
//                 let history_id =
//                     insert_history_item(&pool, row + 100, &hist_item_clone, formatted_start_time)
//                         .await?;
//                 // println!("calculating end time");
//                 let end_time = Local::now();
//                 // println!("calculating duration");
//                 let insert_time_ms = if let Some(time) = (end_time - start_time).num_microseconds()
//                 {
//                     time as f64 / 1000.0 as f64
//                 } else {
//                     0.0 as f64
//                 };
//                 print!(
//                     "Insert new history item: [{}] with id: [{:?}] in: [{}] ms.",
//                     &hist_item_clone, history_id, insert_time_ms
//                 );
//                 let perf_update_id = insert_perf_with(&pool, history_id, insert_time_ms).await?;
//                 println!(
//                     " Successfully updated perf table with id: [{}] perf: [{}].",
//                     perf_update_id, insert_time_ms
//                 );
//             }
//         }
//         Some(HizteryCmd::Update {
//             history_id,
//             // history_item,
//         }) => {
//             // cargo run -- update 1 "some string"
//             println!(
//                 "Updating history_id: [{}] with history_item: [{}]",
//                 history_id, history_item
//             );
//             let row = update_history_item(&pool, history_id, history_item).await?;
//             println!("Updated row: [{}]", row);
//         }
//         Some(HizteryCmd::Delete { history_id }) => {
//             // cargo run -- delete 3
//             println!("Deleting history item: [{}]", history_id);
//             let res = delete_history_item(&pool, history_id).await?;
//             println!("Deleted row count: [{}]", res);
//         }
//         Some(HizteryCmd::Select {}) | None => {
//             // cargo run -- select
//             println!("List of top 5 history items");
//             let output = select_star(&pool).await?;
//             let mut count = 0;
//             for x in output {
//                 if count > 5 {
//                     break;
//                 } else {
//                     println!("ItemNum: [{}] Row: [{:?}]", count, x);
//                     count += 1;
//                 }
//             }
//         }
//         Some(HizteryCmd::Import {
//             nushell_history_filepath,
//         }) => {}
//         Some(HizteryCmd::Search {
//             limit,
//             query,
//             search_mode,
//         }) => {}
//     }

//     Ok(())
// }

// async fn delete_history_item(pool: &SqlitePool, id: i64) -> anyhow::Result<u64> {
//     let mut conn = pool.acquire().await?;

//     let query = format!("DELETE FROM history WHERE history_id = '{}';", id);

//     let res = sqlx::query(&query)
//         .execute(&mut conn)
//         .await?
//         .rows_affected();

//     Ok(res)
// }

// async fn update_history_item(pool: &SqlitePool, id: i64, text: String) -> anyhow::Result<u64> {
//     let mut conn = pool.acquire().await?;

//     let query = format!(
//         "UPDATE history SET (history_item) = '{}' WHERE history_id = {}",
//         text, id
//     );

//     let res = sqlx::query(&query)
//         .execute(&mut conn)
//         .await?
//         .rows_affected();

//     Ok(res)
// }

// async fn select_star(pool: &SqlitePool) -> anyhow::Result<Vec<HistoryTable>> {
//     let mut conn = pool.acquire().await?;

//     let query = r#"
//     SELECT history_id,  session_id,  history_item,  datetime,  executions
//     FROM history;
//     "#;

//     // let query_output: Vec<HistoryTable> = sqlx::query_as!(HistoryTable, "select * from history")
//     //     .fetch_all(&mut conn)
//     //     .await?;

//     let query_output: Vec<HistoryTable> = sqlx::query_as(&query).fetch_all(&mut conn).await?;
//     Ok(query_output)
// }

// async fn initialize_db(pool: &SqlitePool) -> anyhow::Result<bool> {
//     let history_table = r#"
//     CREATE TABLE IF NOT EXISTS "history"
//     (
//         "history_id"      INTEGER PRIMARY KEY NOT NULL,
//         "session_id"      INTEGER             NOT NULL,
//         "history_item"    TEXT                NOT NULL,
//         "datetime"        TEXT                NOT NULL,
//         "executions"      INTEGER             NOT NULL
//     );
//     "#;

//     let performance_table = r#"
//     CREATE TABLE IF NOT EXISTS "performance" (
//         "perf_id"     INTEGER NOT NULL PRIMARY KEY,
//         "metrics"     FLOAT NOT NULL,
//         "history_id"  INTEGER NOT NULL
//         REFERENCES "history"(history_id) ON DELETE CASCADE ON UPDATE CASCADE
//       );
//     "#;

//     let mut conn = pool.acquire().await?;
//     sqlx::query(history_table).execute(&mut conn).await?;
//     sqlx::query(performance_table).execute(&mut conn).await?;

//     Ok(true)
// }

// async fn insert_perf_with(
//     pool: &SqlitePool,
//     history_id: i64,
//     insert_ms: f64,
// ) -> anyhow::Result<i64> {
//     let mut conn = pool.acquire().await?;

//     // println!("creating query");
//     let query = format!(
//         "INSERT INTO performance (metrics, history_id) VALUES ({}, {})",
//         insert_ms, history_id
//     );
//     // println!("executing query '{}'", &query);
//     let id = sqlx::query(&query)
//         .execute(&mut conn)
//         .await?
//         .last_insert_rowid();

//     Ok(id)
// }

// async fn insert_history_item(
//     pool: &SqlitePool,
//     session_id: i64,
//     history_item: &String,
//     time_str: String,
// ) -> anyhow::Result<i64> {
//     let mut conn = pool.acquire().await?;

//     // println!("creating query");
//     let query = format!(
//         "INSERT INTO history (session_id, history_item, datetime, executions) VALUES ({}, '{}', '{}', 1)",
//         session_id, history_item, time_str
//     );
//     // println!("executing query {}", &query);
//     let id = sqlx::query(&query)
//         .execute(&mut conn)
//         .await?
//         .last_insert_rowid();

//     // println!("returning id {}", id);

//     Ok(id)
// }
