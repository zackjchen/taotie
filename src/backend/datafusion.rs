use std::ops::Deref;

use arrow::{array::RecordBatch, util::pretty::pretty_format_batches};
use datafusion::prelude::{
    CsvReadOptions, DataFrame, NdJsonReadOptions, SessionConfig, SessionContext,
};

use crate::{
    cli::{
        connect::{ConnectOpts, DatabaseConn},
        describe::DescribeOpts,
        head::HeadOpts,
        schema::SchemaOpts,
        sql::SqlOpts,
    },
    Backend, ReplDisplay,
};

use super::describe::DescribeDataFrame;

pub struct DataFusionBackend(SessionContext);

impl DataFusionBackend {
    pub fn new() -> Self {
        let mut cfg = SessionConfig::new();
        cfg.options_mut().catalog.information_schema = true;
        let ctx = SessionContext::new_with_config(cfg);
        DataFusionBackend(ctx)
    }
}

impl Default for DataFusionBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for DataFusionBackend {
    type Target = SessionContext;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl ReplDisplay for DataFrame {
    async fn display(&self) -> anyhow::Result<String> {
        // datafusion::dataframe::DataFrame::show(self.clone()).await?;
        let rows = self.clone().collect().await?;
        let data = pretty_format_batches(&rows)?;
        Ok(data.to_string())
    }
}

impl ReplDisplay for RecordBatch {
    async fn display(&self) -> anyhow::Result<String> {
        let data = pretty_format_batches(&[self.clone()])?;
        Ok(data.to_string())
    }
}

impl Backend for DataFusionBackend {
    // type DataFrame = DataFrame;
    async fn connect(&mut self, opts: &ConnectOpts) -> anyhow::Result<()> {
        println!("Connect to dataset: {:?}", opts);
        match &opts.conn {
            DatabaseConn::Postgres(_) => {
                println!("Postgres is not supported yet");
            }
            DatabaseConn::Csv(path) => {
                let options = CsvReadOptions::new();
                self.register_csv(&opts.name, path, options).await?;
            }
            DatabaseConn::Parquet(path) => {
                self.register_parquet(&opts.name, path, Default::default())
                    .await?;
            }
            DatabaseConn::Json(path) => {
                let ndjson_opts = NdJsonReadOptions::default().file_extension(".ndjson");
                self.register_json(&opts.name, path, ndjson_opts).await?;
            }
        }
        Ok(())
    }

    async fn list(&self) -> anyhow::Result<impl ReplDisplay> {
        let sql = "select table_name, table_type from information_schema.tables where table_schema = 'public'";
        let df = self.0.sql(sql).await?;
        Ok(df)
    }

    async fn describe(&self, opts: &DescribeOpts) -> anyhow::Result<impl ReplDisplay> {
        let df = self.0.sql(&format!("select * from {}", opts.name)).await?;
        // let df = df.describe().await?;
        let df1 = DescribeDataFrame::new(df.clone());
        let batchs = df1.to_record_batch().await.unwrap();
        // print!("{}",pretty_format_batches(&[batchs])?);
        Ok(batchs)
    }
    async fn schema(&self, opts: &SchemaOpts) -> anyhow::Result<impl ReplDisplay> {
        let df = self.0.sql(&format!("DESCRIBE {}", opts.name)).await?;
        Ok(df)
    }

    async fn head(&self, opts: &HeadOpts) -> anyhow::Result<impl ReplDisplay> {
        let n = opts.n.unwrap_or(5);
        let df = self
            .0
            .sql(&format!("SELECT * FROM {} LIMIT {}", opts.name, n))
            .await?;
        Ok(df)
    }

    async fn sql(&self, opts: &SqlOpts) -> anyhow::Result<impl ReplDisplay> {
        let df = self.0.sql(&opts.query).await?;
        Ok(df)
    }
}
