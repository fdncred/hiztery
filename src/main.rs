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
    // let pool = SqlitePool::connect(&env::var("DATABASE_URL")?).await?;
    let pool = SqlitePool::connect("sqlite:hiztery.db?mode=rwc").await?;

    let table = r#"
    CREATE TABLE IF NOT EXISTS history
    (
        history_id      INTEGER PRIMARY KEY NOT NULL,
        session_id      INTEGER             NOT NULL,
        history_item    TEXT                NOT NULL,
        datetime        DATETIME            NOT NULL,
        executions      INTEGER             NOT NULL
    );
    "#;

    let mut conn = pool.acquire().await?;
    sqlx::query(table).execute(&mut conn).await?;

    let populate = r#"
    INSERT INTO history (session_id, history_item, datetime, executions) VALUES
    (101, 'now is the time1', '2021-07-13 15:52:01', 1),
    (101, 'now is the time2', '2021-07-13 15:52:02', 1),
    (101, 'now is the time3', '2021-07-13 15:52:03', 1),
    (101, 'now is the time4', '2021-07-13 15:52:04', 1),
    (101, 'now is the time5', '2021-07-13 15:52:05', 1);
    "#;

    sqlx::query(populate).execute(&mut conn).await?;

    match args.cmd {
        Some(HizteryCmd::Add { history_item }) => {
            println!("Adding new history item description '{}'", &history_item);
            // let todo_id = add_todo(&pool, description).await?;
            // println!("Added new todo with id {}", todo_id);
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
