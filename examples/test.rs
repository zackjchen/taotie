use datafusion::{
    functions_aggregate::expr_fn::avg,
    prelude::{col, SessionContext},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ctx = SessionContext::new();
    let df = ctx
        .read_csv("assets/juventus.csv", Default::default())
        .await?;
    let original_schema_fields = df.schema().fields().iter();
    let df = df
        .clone()
        .aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| f.data_type().is_numeric())
                .map(|f| avg(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )
        .unwrap();
    df.show().await?;
    // let res = df.collect().await?;

    // println!("{:?}", res[0]);
    Ok(())
}
