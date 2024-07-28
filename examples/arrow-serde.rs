use std::sync::Arc;

use arrow::{
    array::AsArray,
    datatypes::{DataType, Field, Int32Type, Schema},
    json::ReaderBuilder,
};
use serde::Serialize;

#[derive(Serialize)]
struct MyStruct {
    int32: i32,
    string: String,
}

fn main() {
    let schema = Schema::new(vec![
        Field::new("int32", DataType::Int32, false),
        Field::new("string", DataType::Utf8, false),
    ]);

    let rows = vec![
        MyStruct {
            int32: 5,
            string: "bar".to_string(),
        },
        MyStruct {
            int32: 8,
            string: "foo".to_string(),
        },
    ];

    let mut decoder = ReaderBuilder::new(Arc::new(schema))
        .build_decoder()
        .unwrap();
    decoder.serialize(&rows).unwrap();

    let batch = decoder.flush().unwrap().unwrap();

    // Expect batch containing two columns
    let int32 = batch.column(0).as_primitive::<Int32Type>();
    assert_eq!(int32.values(), &[5, 8]);

    let string = batch.column(1).as_string::<i32>();
    assert_eq!(string.value(0), "bar");
    assert_eq!(string.value(1), "foo");
    println!("{:?}", string.value(0));
}
