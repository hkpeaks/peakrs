use std::fs::File;
use std::io::prelude::*;
use serde_json:: Serializer;
use serde::ser::{SerializeMap, Serializer as SerdeSerializer};
use flate2::Compression;
use flate2::write::ZlibEncoder;
use serde::Serialize;

#[derive(Serialize)]
struct Column {
    id: u8,
    data_type: String,
    byte_addresses: String,
    compression: String,
}

fn main() -> std::io::Result<()> {
    let shop_values = vec!["S01", "S01", "S02", "S02", "S02", "S05", "S06", "S08", "S10", "S10"];
    let shop_string = shop_values.join("\x03");
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(shop_string.as_bytes())?;
    let compressed_bytes = encoder.finish()?;
    let shop_hex = compressed_bytes.iter().map(|b| format!("{:02X}", b)).collect::<Vec<String>>().join("");

    let meta_data = vec![
        ("Shop", Column {
            id: 0,
            data_type: "string".to_string(),
            byte_addresses: "1..25000".to_string(),
            compression: "flate2".to_string(),
        }),
        ("Product", Column {
            id: 1,
            data_type: "string".to_string(),
            byte_addresses: "25001..30000".to_string(),
            compression: "none".to_string(),
        }),
        ("Style", Column {
            id: 2,
            data_type: "string".to_string(),
            byte_addresses: "30001..35000".to_string(),
            compression: "none".to_string(),
        }),
        ("Date", Column {
            id: 3,
            data_type: "date_time".to_string(),
            byte_addresses: "35001..40000".to_string(),
            compression: "none".to_string(),
        }),
        ("Quantity", Column {
            id: 4,
            data_type: "float64".to_string(),
            byte_addresses: "40001..45000".to_string(),
            compression: "none".to_string(),
        }),
    ];

    let column_values = vec![
        ("Shop", shop_hex),
        // ... other columns in order ...
    ];

    let mut file = File::create("data.json")?;
    
    let mut serializer = Serializer::new(&mut file);
    let mut map = serializer.serialize_map(None)?;

    map.serialize_entry("meta_data", &meta_data)?;
    map.serialize_entry("column_values", &column_values)?;

    map.end()?;

    Ok(())
}

