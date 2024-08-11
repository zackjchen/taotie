use std::sync::Arc;

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray},
    compute::{cast, concat},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use datafusion::{
    error::DataFusionError,
    functions_aggregate::{count::count, expr_fn::avg, median::median, stddev::stddev, sum::sum},
    prelude::{case, col, is_null, lit, max, min, DataFrame},
};

pub struct DescribeDataFrame {
    df: DataFrame,
    functions: &'static [&'static str],
    schema: SchemaRef,
}
impl DescribeDataFrame {
    pub fn new(df: DataFrame) -> Self {
        let functions = &["count", "null_count", "mean", "std", "min", "max", "median"];
        let original_schema_fields = df.schema().fields().iter();

        //define describe column
        let mut describe_schemas = vec![Field::new("describe", DataType::Utf8, false)];
        describe_schemas.extend(original_schema_fields.clone().map(|field| {
            if field.data_type().is_numeric() {
                Field::new(field.name(), DataType::Float64, true)
            } else {
                Field::new(field.name(), DataType::Utf8, true)
            }
        }));
        DescribeDataFrame {
            df,
            functions,
            schema: SchemaRef::new(Schema::new(describe_schemas)),
        }
    }

    pub fn aggregate(&self) -> Result<Vec<Result<DataFrame, DataFusionError>>, anyhow::Error> {
        let original_schema_fields = self.df.schema().fields().iter();
        //collect recordBatch
        let describe_record_batch = vec![
            // count aggregation
            self.df.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .map(|f| count(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            ),
            // null_count aggregation
            self.df.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .map(|f| {
                        sum(case(is_null(col(f.name())))
                            .when(lit(true), lit(1))
                            .otherwise(lit(0))
                            .unwrap())
                        .alias(f.name())
                    })
                    .collect::<Vec<_>>(),
            ),
            // mean aggregation
            self.df.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .filter(|f| f.data_type().is_numeric())
                    .map(|f| avg(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            ),
            // std aggregation
            self.df.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .filter(|f| f.data_type().is_numeric())
                    .map(|f| stddev(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            ),
            // min aggregation
            self.df.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .filter(|f| !matches!(f.data_type(), DataType::Binary | DataType::Boolean))
                    .map(|f| min(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            ),
            // max aggregation
            self.df.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .filter(|f| !matches!(f.data_type(), DataType::Binary | DataType::Boolean))
                    .map(|f| max(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            ),
            // median aggregation
            self.df.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .filter(|f| f.data_type().is_numeric())
                    .map(|f| median(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            ),
        ];

        Ok(describe_record_batch)
    }

    pub async fn to_record_batch(&self) -> Result<RecordBatch, anyhow::Error> {
        let dfs = self.aggregate().unwrap();
        let original_schema_fields = self.df.schema().fields().iter();
        // first column with function names
        let mut describe_ref_vec: Vec<ArrayRef> =
            vec![Arc::new(StringArray::from(self.functions.to_vec()))];

        for field in original_schema_fields {
            let mut array_datas = vec![];
            for result in dfs.iter() {
                let array_ref = match result {
                    Ok(df) => {
                        let batchs = df.clone().collect().await;
                        match batchs {
                            Ok(batchs)
                                if batchs.len() == 1
                                    && batchs[0].column_by_name(field.name()).is_some() =>
                            {
                                let column = batchs[0].column_by_name(field.name()).unwrap();
                                if field.data_type().is_numeric() {
                                    cast(column, &DataType::Float64)?
                                } else {
                                    cast(column, &DataType::Utf8)?
                                }
                            }
                            _ => Arc::new(StringArray::from(vec!["null"])),
                        }
                    }
                    Err(e)
                        if e.to_string().contains(
                            "Aggregate requires at least one grouping or aggregate expression",
                        ) =>
                    {
                        Arc::new(StringArray::from(vec!["null"]))
                    }
                    Err(e) => {
                        panic!("Error: {}", e)
                    }
                };
                array_datas.push(array_ref);
            }

            let slice: Vec<_> = array_datas.iter().map(|af| af.as_ref()).collect();
            let slice = slice.as_slice();
            describe_ref_vec.push(concat(slice)?);
        }

        let describe_record_batch = RecordBatch::try_new(self.schema.clone(), describe_ref_vec)?;

        Ok(describe_record_batch)
    }
}
