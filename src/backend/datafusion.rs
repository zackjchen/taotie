use std::ops::Deref;

use arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::{CsvReadOptions, DataFrame, SessionConfig, SessionContext};

use crate::{
    cli::{
        connect::{ConnectOpts, DatabaseConn},
        describe::DescribeOpts,
        head::HeadOpts,
        sql::SqlOpts,
    },
    Backend, ReplDisplay,
};

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

impl Backend for DataFusionBackend {
    type DataFrame = DataFrame;
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
                self.register_json(&opts.name, path, Default::default())
                    .await?;
            }
        }
        Ok(())
    }

    async fn list(&self) -> anyhow::Result<Self::DataFrame> {
        let sql = "show tables";
        let df = self.0.sql(sql).await?;
        Ok(df)
    }

    async fn describe(&self, opts: &DescribeOpts) -> anyhow::Result<Self::DataFrame> {
        let df = self.0.sql(&format!("DESCRIBE {}", opts.name)).await?;

        Ok(df)
    }

    async fn head(&self, opts: &HeadOpts) -> anyhow::Result<Self::DataFrame> {
        let n = opts.n.unwrap_or(5);
        let df = self
            .0
            .sql(&format!("SELECT * FROM {} LIMIT {}", opts.name, n))
            .await?;
        Ok(df)
    }

    async fn sql(&self, opts: &SqlOpts) -> anyhow::Result<Self::DataFrame> {
        let df = self.0.sql(&opts.query).await?;
        Ok(df)
    }
}
