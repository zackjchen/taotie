//! 自己实现describe
//! 接受一个dataframe, 将其转换为一个新的dataframe
//!
//! 新的dataframe 将会将string转为len(string), 将date转为bigint

use std::{fmt::Display, sync::Arc};

use anyhow::Result;
use arrow::datatypes::{DataType, Field};
use datafusion::{
    functions_aggregate::{
        approx_percentile_cont, count::count, expr_fn::avg, median::median, stddev::stddev,
    },
    prelude::{array_length, case, cast, col, length, lit, max, min, DataFrame},
};

#[derive(Debug)]
pub struct DataFrameDescriber {
    original: DataFrame,
    transformed: DataFrame,
    methods: Vec<DescribeMethod>,
}
#[derive(Debug)]
pub enum DescribeMethod {
    Count,
    NullCount,
    Mean,
    Std,
    Min,
    Max,
    Median,
    Percentile(u8),
}

impl Display for DescribeMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DescribeMethod::Count => write!(f, "count"),
            DescribeMethod::NullCount => write!(f, "null_count"),
            DescribeMethod::Mean => write!(f, "mean"),
            DescribeMethod::Std => write!(f, "std"),
            DescribeMethod::Min => write!(f, "min"),
            DescribeMethod::Max => write!(f, "max"),
            DescribeMethod::Median => write!(f, "median"),
            DescribeMethod::Percentile(p) => write!(f, "percentile_{}", p),
        }
    }
}
macro_rules! impl_describe_method {
    ($method:ident) => {
        fn $method(&self) -> Result<DataFrame> {
            let fields = self.transformed.schema().fields().iter();
            let expr = fields
                .map(|field| $method(col(field.name())).alias(field.name()))
                .collect();
            let df = self.transformed.clone().aggregate(vec![], expr);
            Ok(df?)
        }
    };
}
impl DataFrameDescriber {
    pub fn try_new(df: DataFrame) -> Result<Self> {
        let fields = df.schema().fields().iter();
        let expr = fields
            .map(|field| {
                let dt = field.data_type();
                let expr = match dt {
                    dt if dt.is_temporal() => cast(col(field.name()), DataType::Float64),
                    dt if dt.is_numeric() => col(field.name()),
                    DataType::List(_) => array_length(col(field.name())),
                    _ => length(col(field.name())),
                };
                expr.alias(field.name())
            })
            .collect();

        let transformed = df.clone().select(expr)?;
        Ok(Self {
            original: df,
            transformed,
            methods: vec![
                DescribeMethod::Count,
                DescribeMethod::NullCount,
                DescribeMethod::Mean,
                DescribeMethod::Std,
                DescribeMethod::Min,
                DescribeMethod::Max,
                DescribeMethod::Median,
                DescribeMethod::Percentile(25),
                DescribeMethod::Percentile(50),
                DescribeMethod::Percentile(75),
            ],
        })
    }

    // fn count(&self) -> Result<DataFrame> {
    //     let fields = self.transformed.schema().fields().iter();
    //     let expr = fields.map(|field| {
    //         count(col(field.name())).alias(field.name())
    //     }).collect();
    //     let df = self.transformed.clone().aggregate(vec![], expr);
    //     Ok(df?)
    // }

    fn null_count(&self) -> Result<DataFrame> {
        let fields = self.transformed.schema().fields().iter();
        let expr = fields
            .map(|field| {
                count(
                    case(col(field.name()).is_null())
                        .when(lit(true), lit(1))
                        .otherwise(lit(0))
                        .unwrap(),
                )
                .alias(field.name())
            })
            .collect();
        let df = self.transformed.clone().aggregate(vec![], expr);
        Ok(df?)
    }

    // fn mean(&self) -> Result<DataFrame> {
    //     let fields = self.transformed.schema().fields().iter();
    //     let expr = fields.map(|field| {
    //             avg(col(field.name())).alias(field.name())
    //     }).collect();
    //     let df = self.transformed.clone().aggregate(vec![], expr);
    //     Ok(df?)

    // }

    // fn std(&self) -> Result<DataFrame> {
    //     let fields = self.transformed.schema().fields().iter();
    //     let expr = fields.map(|field|{
    //         stddev(col(field.name())).alias(field.name())
    //     }).collect();
    //     let df = self.transformed.clone().aggregate(vec![], expr);
    //     Ok(df?)
    // }

    // fn min(&self) -> Result<DataFrame> {
    //     let fields = self.transformed.schema().fields().iter();
    //     let expr = fields.map(|field|{
    //         min(col(field.name())).alias(field.name())
    //     }).collect();
    //     let df = self.transformed.clone().aggregate(vec![], expr);
    //     Ok(df?)

    // }

    // fn max(&self) -> Result<DataFrame>{
    //     let fields = self.transformed.schema().fields().iter();
    //     let expr = fields.map(|field|{
    //         max(col(field.name())).alias(field.name())
    //     }).collect();
    //     let df = self.transformed.clone().aggregate(vec![], expr);
    //     Ok(df?)
    // }

    // fn median(&self) -> Result<DataFrame> {
    //     let fields = self.transformed.schema().fields().iter();
    //     let expr = fields.map(|field|{
    //         median(col(field.name())).alias(field.name())
    //     }).collect();
    //     let df = self.transformed.clone().aggregate(vec![], expr);
    //     Ok(df?)
    // }

    fn percentile(&self, p: u8) -> Result<DataFrame> {
        let fields = self.transformed.schema().fields().iter();
        let expr = fields
            .map(|field| {
                let pecentfile = lit(p as f64 / 100.0);
                approx_percentile_cont::approx_percentile_cont(col(field.name()), pecentfile)
                    .alias(field.name())
            })
            .collect();
        let df = self.transformed.clone().aggregate(vec![], expr);
        Ok(df?)
    }

    impl_describe_method!(count);
    impl_describe_method!(avg);
    impl_describe_method!(stddev);
    impl_describe_method!(min);
    impl_describe_method!(max);
    impl_describe_method!(median);

    pub(crate) async fn describe(&self) -> anyhow::Result<DataFrame> {
        let df: Option<DataFrame> = self.methods.iter().fold(None, |acc, method| {
            let stat_df = match method {
                DescribeMethod::Count => self.count(),
                DescribeMethod::NullCount => self.null_count(),
                DescribeMethod::Mean => self.avg(),
                DescribeMethod::Std => self.stddev(),
                DescribeMethod::Min => self.min(),
                DescribeMethod::Max => self.max(),
                DescribeMethod::Median => self.median(),
                DescribeMethod::Percentile(n) => self.percentile(*n),
            };
            let stat_df = stat_df.unwrap();
            let mut select_expr = vec![lit(method.to_string()).alias("describe")];
            select_expr.extend(stat_df.schema().fields().iter().map(|f| col(f.name())));

            let stat_df = stat_df.select(select_expr).unwrap();

            match acc {
                Some(acc) => Some(acc.union(stat_df).unwrap()),
                None => Some(stat_df),
            }
        });

        df.ok_or_else(|| anyhow::anyhow!("No describe found"))
    }

    /// if the original col type is date, then we will convert it to date
    pub fn cast_back(&self, df: DataFrame) -> DataFrame {
        let desc = Arc::new(Field::new("describe", DataType::Utf8, true));
        let mut fields = vec![&desc];
        fields.extend(self.original.schema().fields().iter());
        let expr = fields
            .iter()
            .map(|field| {
                let dt = field.data_type();
                let expr = match dt {
                    dt if dt.is_temporal() => cast(col(field.name()), dt.clone()),
                    DataType::List(_) | DataType::LargeList(_) => {
                        cast(col(field.name()), DataType::Int32)
                    }
                    _ => col(field.name()),
                };
                expr.alias(field.name())
            })
            .collect();

        df.select(expr)
            .unwrap()
            .sort(vec![col("describe").sort(true, true)])
            .unwrap()
    }
}
