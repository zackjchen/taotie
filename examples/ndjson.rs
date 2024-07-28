use anyhow::Result;
use arrow::{
    array::AsArray,
    datatypes::{DataType, Field, Schema},
    json::ReaderBuilder,
};
use std::{fs::File, io::BufReader, sync::Arc};

fn main() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("email", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("gender", DataType::Utf8, false),
        Field::new("created_at", DataType::Date32, false),
        Field::new("last_visited_at", DataType::Date64, true),
        Field::new("last_watched_at", DataType::Date64, true),
        Field::new(
            "recent_watched",
            DataType::List(Arc::new(Field::new(
                "recent_watched",
                DataType::Int64,
                false,
            ))),
            false,
        ),
        Field::new(
            "viewed_but_not_started",
            DataType::List(Arc::new(Field::new(
                "recent_watched",
                DataType::Int64,
                false,
            ))),
            false,
        ),
        Field::new(
            "started_but_not_finished",
            DataType::List(Arc::new(Field::new(
                "recent_watched",
                DataType::Int64,
                false,
            ))),
            false,
        ),
        Field::new(
            "finished",
            DataType::List(Arc::new(Field::new(
                "recent_watched",
                DataType::Int64,
                false,
            ))),
            false,
        ),
        Field::new("last_email_notification", DataType::Date64, false),
        Field::new("last_in_app_notification", DataType::Date64, false),
        Field::new("last_sms_notification", DataType::Date64, false),
    ]);

    let reader = BufReader::new(File::open("assets/users.ndjson")?);
    let reader = ReaderBuilder::new(Arc::new(schema)).build(reader)?;
    for batch in reader {
        let batch = batch?;
        let email = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let name = batch.column(1).as_string::<i32>();
        println!("{:?}", email);
        println!("{:?}", name);
    }

    Ok(())
}
