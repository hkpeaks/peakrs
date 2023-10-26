use std::fs::File;
//use std::io::BufWriter;
use std::io::BufReader;
//use std::io::Write; // Add this line
use std::time::Instant;
//use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};
use byteorder::{ReadBytesExt, LittleEndian};

fn main() {
    /*
    let mut start = Instant::now();
    let n = 1_000_100_000 / 65536; // Adjusted to ensure write_col_0's size is a multiple of 2
    let mut write_col_0 = Vec::with_capacity(n as usize * 65536);   
    
    for num in 0..=65535 {
        for _ in 0..n {
            write_col_0.push(num);
        }
    }   

    // Open a file in write-only mode
    let file = File::create("col_0.jbin").expect("Unable to create file");
    let mut writer = BufWriter::new(file);

    for &value in write_col_0.iter() {
        // Write u16 bytes to file
        writer.write_u16::<LittleEndian>(value).expect("Unable to write to file");
    }

    // Ensure all data is written
    writer.flush().expect("Unable to flush writer");

    println!("Write pairs of 16-bit bytes to disk: {} ", write_col_0.len());

    let duration = start.elapsed();    
    println!("Time elapsed is: {:?}", duration);
*/
    let start = Instant::now();

     // Open the file in read-only mode.
     let file = File::open("col_0.jbin").expect("Unable to open file");
     let mut reader = BufReader::new(file);
 
     let mut read_col_0 = Vec::new();
 
     // Read u16s from the file.
     while let Ok(value) = reader.read_u16::<LittleEndian>() {
         read_col_0.push(value);
     }

     if let Err(e) = reader.read_u16::<LittleEndian>() {
        if e.kind() != std::io::ErrorKind::UnexpectedEof {
            println!("Error reading data: {}", e);
        }
    }

     println!("Read pairs of 16-bit bytes to memory: {} ", read_col_0.len());
/*     
     // Verify whether read_col_0 is equal to write_col_0
     if read_col_0 == write_col_0 {
         println!("Success: read_col_0 is equal to write_col_0");
     } else {
         println!("Error: read_col_0 is not equal to write_col_0");
     }
*/
     let duration = start.elapsed();    
     println!("Time elapsed is: {:?}", duration);
}
