use std::fs::File;

use anyhow::Result;
use arrow::{
    array::{Array, BinaryArray},
    util::pretty::pretty_format_batches,
};
use datafusion::prelude::{col, ParquetReadOptions, SessionContext};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use polars::{prelude::LazyFrame, sql::SQLContext};

const PQ_FILE: &str = "assets/sample.parquet";

#[tokio::main]
async fn main() -> Result<()> {
    read_with_parquet(PQ_FILE)?;
    read_with_datafusion(PQ_FILE).await?;
    read_with_polars(PQ_FILE)?;
    Ok(())
}

fn read_with_parquet(file_path: &str) -> Result<()> {
    let file = File::open(file_path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(8129)
        .with_limit(3)
        .build()?;
    for record_batch in reader {
        let record_batch = record_batch?;
        // 打印它的类型，然后转换为BinaryArray
        println!("{:?}", record_batch.column(0).data_type());
        let users = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        for user in users {
            let user = String::from_utf8_lossy(user.unwrap());
            println!("{}", user);
        }
        // println!("{:?}", users);
    }
    Ok(())
}

async fn read_with_datafusion(file_path: &str) -> Result<()> {
    let ctx = SessionContext::new();
    let opt = ParquetReadOptions::default();
    // 读取parquet文件
    let df = ctx.read_parquet(file_path, opt).await?;
    // 注册成表
    ctx.register_parquet("users", file_path, Default::default())
        .await?;

    // 操作dataframe
    let df = df.select(vec![col("email")])?;
    // 在select的时候转换类型
    let df2 = ctx.sql("select email::text from users").await?;
    assert!(df.schema().fields().len() == df2.schema().fields().len());

    // 打印schema
    println!("{:?}", df.schema());

    // collect and pretty print results
    let res = df.collect().await?;
    let pretty_results = pretty_format_batches(&res)?;
    println!("{}", pretty_results);

    // 打印email列，这里的email列是BinaryArray类型，需要转换为String
    for batch in res {
        let emails = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        for email in emails {
            let email = String::from_utf8_lossy(email.unwrap());
            println!("{:?}", email);
        }
    }

    Ok(())
}

fn read_with_polars(file_path: &str) -> Result<()> {
    use polars::prelude::col;
    let df = LazyFrame::scan_parquet(file_path, Default::default())?;
    let df2 = df.clone().select(&[col("email")]);
    let df3 = df2.collect()?;
    // println!("{:?}", df3);

    let mut ctx = SQLContext::new();
    ctx.register("users", df);
    let df = ctx.execute("SELECT email::text FROM users")?;
    let res = df.collect()?;
    assert_eq!(df3, res);
    println!("{:?}", res);
    Ok(())
}
