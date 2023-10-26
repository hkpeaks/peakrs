use std::fs::File;
use std::io::BufReader;
use std::time::Instant;
use byteorder::{ReadBytesExt, LittleEndian};
use std::collections::HashMap;
use rayon::prelude::*;
use std::sync::{Arc, Mutex};

fn main() {
    let start = Instant::now();

    let files = vec![
        "col_0.jbin", "col_2.jbin", "col_3.jbin", "col_4.jbin", 
        "col_5.jbin", "col_6.jbin", "col_7.jbin", "col_8.jbin", 
        "col_9.jbin"
    ];

    let map: Arc<Mutex<HashMap<String, Vec<u16>>>> = Arc::new(Mutex::new(HashMap::new()));

    files.par_iter().for_each(|file| {
        let mut reader = BufReader::new(File::open(file).expect("Unable to open file"));
        let mut vec = Vec::new();

        while let Ok(value) = reader.read_u16::<LittleEndian>() {
            vec.push(value);
        }

        if let Err(e) = reader.read_u16::<LittleEndian>() {
            if e.kind() != std::io::ErrorKind::UnexpectedEof {
                println!("Error reading data: {}", e);
            }
        }

        map.lock().unwrap().insert(file.to_string(), vec);
    });

    println!("Read pairs of 16-bit integer to memory for each column: {:?}", map.lock().unwrap().iter().map(|(k, v)| (k, v.len())).collect::<HashMap<_, _>>());

    let duration = start.elapsed();    
    println!("Time elapsed is: {:?}", duration);
}
