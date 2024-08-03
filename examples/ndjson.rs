use anyhow::Result;
use arrow::{
    array::AsArray,
    datatypes::{DataType, Field, Schema, TimeUnit},
    json::{writer::JsonArray, ArrayWriter, ReaderBuilder, WriterBuilder},
};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use std::{fs::File, io::BufReader, sync::Arc};

#[derive(Debug, Deserialize, Serialize)]
struct User {
    email: String,
    name: String,
    gender: String,
    // 自定义解析函数
    #[serde(deserialize_with = "deserialize_string_date")]
    created_at: DateTime<Utc>,

    // #[serde(deserialize_with = "deserialize_string_date_opt")]
    last_visited_at: Option<DateTime<Utc>>,

    #[serde(deserialize_with = "deserialize_string_date_opt")]
    last_watched_at: Option<DateTime<Utc>>,

    recent_watched: Vec<i64>,
    viewed_but_not_started: Vec<i64>,
    started_but_not_finished: Vec<i64>,
    finished: Vec<i64>,
    #[serde(deserialize_with = "deserialize_string_date_opt")]
    last_email_notification: Option<DateTime<Utc>>,

    #[serde(deserialize_with = "deserialize_string_date_opt")]
    last_in_app_notification: Option<DateTime<Utc>>,

    #[serde(deserialize_with = "deserialize_string_date_opt")]
    last_sms_notification: Option<DateTime<Utc>>,
}
// fn deserialize_string_date() {

// }
fn deserialize_string_date<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let date = NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.3f").unwrap();
    Ok(Utc.from_utc_datetime(&date))
}
fn deserialize_string_date_opt<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = Option::<String>::deserialize(deserializer)?;
    // println!("{:?}", s);
    match s {
        Some(s) => {
            let date = NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.3f").unwrap();
            Ok(Some(Utc.from_utc_datetime(&date)))
        }
        None => Ok(None),
    }
}
fn main() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("email", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("gender", DataType::Utf8, false),
        Field::new("created_at", DataType::Date64, false),
        Field::new(
            "last_visited_at",
            DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into())),
            true,
        ),
        Field::new("last_watched_at", DataType::Date64, true),
        Field::new(
            "recent_watched",
            DataType::List(Arc::new(Field::new(
                "recent_watched",
                DataType::Int64,
                false,
            ))),
            true,
        ),
        Field::new(
            "viewed_but_not_started",
            DataType::List(Arc::new(Field::new(
                "recent_watched",
                DataType::Int64,
                false,
            ))),
            true,
        ),
        Field::new(
            "started_but_not_finished",
            DataType::List(Arc::new(Field::new(
                "recent_watched",
                DataType::Int64,
                false,
            ))),
            true,
        ),
        Field::new(
            "finished",
            DataType::List(Arc::new(Field::new(
                "recent_watched",
                DataType::Int64,
                false,
            ))),
            true,
        ),
        Field::new("last_email_notification", DataType::Date64, true),
        Field::new("last_in_app_notification", DataType::Date64, true),
        Field::new("last_sms_notification", DataType::Date64, true),
    ]);

    let reader = BufReader::new(File::open("assets/users.ndjson")?);
    let reader = ReaderBuilder::new(Arc::new(schema)).build(reader)?;

    let data: Vec<u8> = Vec::new();
    // 这两个类型声明是一样的
    let mut writer: ArrayWriter<Vec<u8>> = WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, JsonArray>(data);

    for batch in reader {
        let batch = batch?;
        writer.write(&batch)?;

        // 转成列数组
        let _email: &arrow::array::GenericByteArray<arrow::datatypes::GenericStringType<i32>> =
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
        let _name: &arrow::array::GenericByteArray<arrow::datatypes::GenericStringType<i32>> =
            batch.column(1).as_string::<i32>();
        let _date = batch.column(4); //.as_any().downcast_ref::<Date64Array>().unwrap();
                                     // println!("{:?}", email);
                                     // println!("{:?}", name);
                                     // println!("{:?}", date);
    }
    writer.finish()?;
    let data = writer.into_inner();
    let ret = serde_json::from_slice::<Vec<User>>(&data).unwrap();
    // println!("{:?}", from_utf8(&data)?);

    print!("{:?}", ret);

    Ok(())
}
