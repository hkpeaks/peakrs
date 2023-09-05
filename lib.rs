use pyo3::prelude::*;
//use chrono::{DateTime, Utc};
//use pyo3::types::PyDateTime;
extern crate rayon;
extern crate regex;
use std::fs::{File, metadata};
use std::str;
use std::io::{Write, BufWriter};
use rayon::prelude::*;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;
//use std::fs::File;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use regex::Regex;
use std::time::SystemTime;
use std::fs;
mod extract;
mod utility;
mod parse_setting;
mod query;

#[pyclass]
#[derive(Clone)]
pub struct Dataframe {
    pub alt_column_name: Vec<String>,
    pub column_name: Vec<String>,
    pub column_name_end_address: u32,
    pub command_name: String,
    pub command_setting: String,
    pub delimiter: u8,
    pub duration_second: f64,
    pub end_column_count: i32,
    pub end_partition_count: i32,
    pub end_row_count: i64,
    pub end_time: Instant,
    pub error_message: String,
    pub estimate_row: i64,
    pub file_size: i64,
    pub is_line_br_10_exist: bool,
    pub is_line_br_13_exist: bool,
    pub keyvalue_table: HashMap<String, Vec<u8>>,
    pub log_file_name: String,
    pub partition_address: Vec<i64>,
    pub partition_count: i32,
    pub partition_data: Vec<u8>,
    pub partition_size_mb: i32,
    pub processed_partition: i32,
    pub start_column_count: i32,
    pub start_partition_count: i32,
    pub start_row_count: i64,
    pub start_time: Instant,
    pub streaming_batch: i32,
    pub thread: i32,
    pub validate_row: i64,
    pub value_column_name: Vec<String>,
    pub vector_table: Vec<u8>,
    pub vector_table_group: HashMap<usize, Vec<u8>>,
}
/*
use chrono::{DateTime, Duration, Utc};
use std::time::{Instant, SystemTime};

let instant = Instant::now();
let system_time = SystemTime::now();
let datetime = DateTime::<Utc>::from(system_time);
let duration = instant.duration_since(SystemTime::UNIX_EPOCH).expect("Time went backwards");
let datetime_from_instant = datetime + Duration::from_std(duration).unwrap();
*/
 
 
#[pymethods]
impl Dataframe {
       #[new]
       fn new() -> Self {
           Dataframe {
               alt_column_name: Vec::new(),
               column_name: Vec::new(),
               column_name_end_address: 0,
               command_name: String::new(),
               command_setting: String::new(),
               delimiter: 0,
               duration_second: 0.0,
               end_time: Instant::now(),
               end_column_count: 0,
               end_partition_count: 0,
               end_row_count: 0,
               error_message: String::new(),
               estimate_row: 0,
               file_size: 0,
               is_line_br_10_exist: false,
               is_line_br_13_exist: false,
               keyvalue_table: HashMap::new(),
               log_file_name: String::new(),
               partition_address: Vec::new(),
               partition_count: 0,
               partition_data: Vec::new(),
               partition_size_mb: 0, 
               processed_partition: 0,
               start_time: Instant::now(), 
               start_column_count: 0, 
               start_partition_count: 0, 
               start_row_count: 0, 
               streaming_batch: 0, 
               thread: 0, 
               validate_row: 0, 
               value_column_name: Vec::new(), 
               vector_table: Vec::new(), 
               vector_table_group: HashMap::new()
            }
        }

        #[getter]
        fn get_alt_column_name(&self) -> PyResult<Vec<String>> {
            Ok(self.alt_column_name.clone())
        }   
    
        #[setter]
        fn set_alt_column_name(&mut self, value: Vec<String>) -> PyResult<()> {
            self.alt_column_name = value;
            Ok(())
        }
    
        #[getter]
        fn get_column_name(&self) -> PyResult<Vec<String>> {
            Ok(self.column_name.clone())
        }
    
        #[setter]
        fn set_column_name(&mut self, value: Vec<String>) -> PyResult<()> {
            self.column_name = value;
            Ok(())
        }
    
        #[getter]
        fn get_column_name_end_address(&self) -> PyResult<u32> {
            Ok(self.column_name_end_address)
        }
    
        #[setter]
        fn set_column_name_end_address(&mut self, value: u32) -> PyResult<()> {
            self.column_name_end_address = value;
            Ok(())
        }
    
        #[getter]
        fn get_command_name(&self) -> PyResult<String> {
            Ok(self.command_name.clone())
        }
    
        #[setter]
        fn set_command_name(&mut self, value: String) -> PyResult<()> {
            self.command_name = value;
            Ok(())
        }
    
        #[getter]
        fn get_command_setting(&self) -> PyResult<String> {
            Ok(self.command_setting.clone())
        }
    
        #[setter]
        fn set_command_setting(&mut self, value: String) -> PyResult<()> {
            self.command_setting = value;
            Ok(())
        }
    
        #[getter]
        fn get_delimiter(&self) -> PyResult<u8> {
            Ok(self.delimiter)
        }
    
        #[setter]
        fn set_delimiter(&mut self, value: u8) -> PyResult<()> {
            self.delimiter = value;
            Ok(())
        }
    
        #[getter]
        fn get_duration_second(&self) -> PyResult<f64> {
            Ok(self.duration_second)
        }
    
        #[setter]
        fn set_duration_second(&mut self, value: f64) -> PyResult<()> {
            self.duration_second = value;
            Ok(())
        }
    
        #[getter]
        fn get_end_column_count(&self) -> PyResult<i32> {
            Ok(self.end_column_count)
        }
    
         #[setter]
         fn set_end_column_count(&mut self, value: i32) -> PyResult<()> {
             self.end_column_count = value;
             Ok(())
         }
    
         #[getter]
         fn get_end_partition_count(&self) -> PyResult<i32> {
             Ok(self.end_partition_count)
         }
     
         #[setter]
         fn set_end_partition_count(&mut self, value: i32) -> PyResult<()> {
             self.end_partition_count = value;
             Ok(())
         }
     
         #[getter]
         fn get_end_row_count(&self) -> PyResult<i64> {
             Ok(self.end_row_count)
         }
     
         #[setter]
         fn set_end_row_count(&mut self, value: i64) -> PyResult<()> {
             self.end_row_count = value;
             Ok(())
         }     
         /*
         #[getter]
         fn get_end_time(&self) -> PyResult<Instant> {
             Ok(self.end_time)
         }

         #[setter]
         fn set_end_time(&mut self, value: Instant) -> PyResult<()> {
            self.end_time = value;
            Ok(())
        }

        #[getter]
        fn get_end_time(&self, py: Python) -> PyResult<PyObject> {
            let datetime = DateTime::<Utc>::from(self.end_time);
            PyDateTime::from_timestamp(py, datetime.timestamp(), 0)
        }
    
        #[setter]
        fn set_end_time(&mut self, value: &PyDateTime) -> PyResult<()> {
            let datetime = value.to_object(value.py()).extract::<DateTime<Utc>>()?;
            self.end_time = datetime.into();
            Ok(())
        }*/
     
         #[getter]
         fn get_error_message(&self) -> PyResult<String> {
             Ok(self.error_message.clone())
         }
     
         #[setter]
         fn set_error_message(&mut self, value: String) -> PyResult<()> {
            self.error_message = value;
            Ok(())
        }
       
        #[getter]
        fn get_estimate_row(&self) -> PyResult<i64> {
            Ok(self.estimate_row)
        }
       
        #[setter]
        fn set_estimate_row(&mut self, value: i64) -> PyResult<()> {
            self.estimate_row = value;
            Ok(())
        }
       
        #[getter]
        fn get_file_size(&self) -> PyResult<i64> {
            Ok(self.file_size)
        }
       
        #[setter]
        fn set_file_size(&mut self, value: i64) -> PyResult<()> {
            self.file_size = value;
            Ok(())
        }
       
        #[getter]
        fn get_is_line_br_10_exist(&self) -> PyResult<bool> {
            Ok(self.is_line_br_10_exist)
        }
       
        #[setter]
        fn set_is_line_br_10_exist(&mut self, value: bool) -> PyResult<()> {
            self.is_line_br_10_exist = value;
            Ok(())
        }
       
        #[getter]
        fn get_is_line_br_13_exist(&self) -> PyResult<bool> {
            Ok(self.is_line_br_13_exist)
        }
       
        #[setter]
        fn set_is_line_br_13_exist(&mut self, value: bool) -> PyResult<()> {
            self.is_line_br_13_exist = value;
            Ok(())
        }
       
        #[getter]
        fn get_keyvalue_table(&self) -> PyResult<HashMap<String, Vec<u8>>> {
            Ok(self.keyvalue_table.clone())
        }
       
        #[setter]
        fn set_keyvalue_table(&mut self, value: HashMap<String, Vec<u8>>) -> PyResult<()> {
            self.keyvalue_table = value;
            Ok(())
        }
       
        #[getter]
        fn get_log_file_name(&self) -> PyResult<String> {
            Ok(self.log_file_name.clone())
        }
       
        #[setter]
        fn set_log_file_name(&mut self, value: String) -> PyResult<()> {
            self.log_file_name = value;
            Ok(())
        }
       
        #[getter]
        fn get_partition_address(&self) -> PyResult<Vec<i64>> {
            Ok(self.partition_address.clone())
        }
       
        #[setter]
        fn set_partition_address(&mut self, value: Vec<i64>) -> PyResult<()> {
            self.partition_address = value;
            Ok(())
        }
       
        #[getter]
        fn get_partition_count(&self) -> PyResult<i32> {
            Ok(self.partition_count)
        }
       
        #[setter]
        fn set_partition_count(&mut self, value: i32) -> PyResult<()> {
            self.partition_count = value;
            Ok(())
        }
       
        #[getter]
        fn get_partition_data(&self) -> PyResult<Vec<u8>> {
            Ok(self.partition_data.clone())
        }
       
        #[setter]
        fn set_partition_data(&mut self, value: Vec<u8>) -> PyResult<()> {
            self.partition_data = value;
            Ok(())
        }
       
        #[getter]
        fn get_partition_size_mb(&self) -> PyResult<i32> {
            Ok(self.partition_size_mb)
        }
       
        #[setter]
        fn set_partition_size_mb(&mut self, value: i32) -> PyResult<()> {
            self.partition_size_mb = value;
            Ok(())
        }
       
        #[getter]
        fn get_processed_partition(&self) -> PyResult<i32> {
            Ok(self.processed_partition)
        }
       
        #[setter]
        fn set_processed_partition(&mut self, value: i32) -> PyResult<()> {
            self.processed_partition = value;
            Ok(())
        }
       
        #[getter]
        fn get_start_column_count(&self) -> PyResult<i32> {
            Ok(self.start_column_count)
        }
        
         #[setter]
         fn set_start_column_count(&mut self, value: i32) -> PyResult<()> {
             self.start_column_count = value;
             Ok(())
         }
        
         #[getter]
         fn get_start_partition_count(&self) -> PyResult<i32> {
             Ok(self.start_partition_count)
         }
        
         #[setter]
         fn set_start_partition_count(&mut self, value: i32) -> PyResult<()> {
             self.start_partition_count = value;
             Ok(())
         }
        
         #[getter]
         fn get_start_row_count(&self) -> PyResult<i64> {
             Ok(self.start_row_count)
         }
        
         #[setter]
         fn set_start_row_count(&mut self, value: i64) -> PyResult<()> {
             self.start_row_count = value;
             Ok(())
         }        
         /*
         #[getter]
         fn get_start_time(&self) -> PyResult<Instant> {
             Ok(self.start_time)
         }

         #[setter]
         fn set_start_time(&mut self, value: Instant) -> PyResult<()> {
            self.start_time = value;
            Ok(())
        }*/
        
         #[getter]
         fn get_streaming_batch(&self) -> PyResult<i32> {
             Ok(self.streaming_batch)
         }
        
         #[setter]
         fn set_streaming_batch(&mut self, value: i32) -> PyResult<()> {
             self.streaming_batch = value;
             Ok(())
         }
        
         #[getter]
         fn get_thread(&self) -> PyResult<i32> {
             Ok(self.thread)
         }
        
         #[setter]
         fn set_thread(&mut self, value: i32) -> PyResult<()> {
             self.thread = value;
             Ok(())
         }
        
         #[getter]
         fn get_validate_row(&self) -> PyResult<i64> {
             Ok(self.validate_row)
         }
        
         #[setter]
         fn set_validate_row(&mut self, value: i64) -> PyResult<()> {
             self.validate_row = value;
             Ok(())
         }
        
         #[getter]
         fn get_value_column_name(&self) -> PyResult<Vec<String>> {
             Ok(self.value_column_name.clone())
         }
        
         #[setter]
         fn set_value_column_name(&mut self, value: Vec<String>) -> PyResult<()> {
             self.value_column_name = value;
             Ok(())
         }
        
         #[getter]
         fn get_vector_table(&self) -> PyResult<Vec<u8>> {
             Ok(self.vector_table.clone())
         }
        
         #[setter]
         fn set_vector_table(&mut self, value: Vec<u8>) -> PyResult<()> {
             self.vector_table = value;
             Ok(())
         }
        
         #[getter]
         fn get_vector_table_group(&self) -> PyResult<HashMap<usize, Vec<u8>>> {
             Ok(self.vector_table_group.clone())
         }
        
         #[setter]
         fn set_vector_table_group(&mut self, value: HashMap<usize, Vec<u8>>) -> PyResult<()> {
             self.vector_table_group = value;
             Ok(())
         }       
   }



#[pyclass]
#[derive(Clone)]
pub struct MetaInfo {
    pub alt_column_name: Vec<String>,
    pub column_name: Vec<String>,
    pub column_name_end_address: u32,
    pub command_name: String,
    pub command_setting: String,
    pub delimiter: u8,
    pub duration_second: f64,
    pub end_column_count: i32,
    pub end_partition_count: i32,
    pub end_row_count: i64,
    pub end_time: Instant,
    pub error_message: String,
    pub estimate_row: i64,
    pub file_size: i64,
    pub is_line_br_10_exist: bool,
    pub is_line_br_13_exist: bool,
    pub log_file_name: String,
    pub partition_address: Vec<i64>,
    pub partition_count: i32,
    pub partition_data: Vec<u8>,
    pub partition_size_mb: i32,
    pub processed_partition: i32,
    pub start_column_count: i32,
    pub start_partition_count: i32,
    pub start_row_count: i64,
    pub start_time: Instant,
    pub streaming_batch: i32,
    pub thread: i32,
    pub validate_row: i64,
    pub value_column_name: Vec<String>,
}



impl Default for Dataframe {
    fn default() -> Self {
        Dataframe {
            alt_column_name: Vec::new(),
            column_name: Vec::new(),
            column_name_end_address: 0,
            command_name: String::new(),
            command_setting: String::new(),
            delimiter: 0,
            duration_second: 0.0,
            end_column_count: 0,
            end_partition_count: 0,
            end_row_count: 0,
            end_time: Instant::now(),
            error_message: String::new(),
            estimate_row: 0,
            file_size: 0,
            is_line_br_10_exist: false,
            is_line_br_13_exist: false,
            keyvalue_table: HashMap::new(),
            log_file_name: String::new(),
            partition_address: Vec::new(),
            partition_count: 0,
            partition_data: Vec::new(),
            partition_size_mb: 0,
            processed_partition: 0,
            start_column_count: 0,
            start_partition_count: 0,
            start_row_count: 0,
            start_time: Instant::now(),
            streaming_batch: 0,
            thread: 0,
            validate_row: 0,
            value_column_name: Vec::new(),
            vector_table: Vec::new(),
            vector_table_group: HashMap::new(),
        }
    }
}



#[derive(Clone)]
pub struct QuerySetting {
    pub agg_column: Vec<String>,
    pub agg_function: Vec<String>,
    pub column_index: Vec<i32>,
    pub condition: Vec<Vec<String>>,
    pub current_command: String,
    pub current_setting: String,
    pub error_message: String,
    pub ref_column: Vec<String>,
    pub upper_column_name_to_alt_compare_float_value: HashMap<String, Vec<String>>,
    pub upper_column_name_to_alt_compare_value: HashMap<String, Vec<String>>,
    pub upper_column_name_to_compare_float_value: HashMap<String, Vec<String>>,
    pub upper_column_name_to_compare_value: HashMap<String, Vec<String>>,
    pub upper_column_name_to_data_type: HashMap<String, Vec<String>>,
    pub upper_column_name_to_operator: HashMap<String, Vec<String>>,

}
/*
impl Dataframe {
    pub fn clear_tables(&mut self) {
        self.vector_table.clear();
        self.vector_table_group.clear();
        self.keyvalue_table.clear();
    }
}*/

pub fn test_filter() {
    let column: Vec<String> = vec!["Shop".to_string(), "Date".to_string(), "Product".to_string(), "Style".to_string(), "Quantity".to_string(), "Unit_Price".to_string(), "Currency".to_string(), "Original Amount".to_string(), "Exchange_Rate".to_string(), "Base Amount".to_string()];
    let query = "Product(100..200)";
    //let query = "Quantity(Float 100..200, 900..999) Shop(S10..S90) Product(=222, >=900, <=110, != 100) Base Amount(Float>20000)";

    let result = parse_setting::validate_filter_setting(query, column);
    if !result.error_message.is_empty() {
        println!("{}", result.error_message);
        return;
    }

    println!("Return Value");
    println!("Column Names: {:?}", result.ref_column);
    println!();
    println!("Column Indices: {:?}", result.column_index);
    println!("condition");
    println!("{:?}", result.condition);
    println!("Upper Column Name to Operator");
    println!("{:?}", result.upper_column_name_to_operator);
    println!("Upper Column Name to Data Type");
    println!("{:?}", result.upper_column_name_to_data_type);
    println!("Upper Column Name to Compare Value");
    println!("{:?}", result.upper_column_name_to_compare_value);
    println!("Upper Column Name to Alt Compare Value");
    println!("{:?}", result.upper_column_name_to_alt_compare_value);
    println!("Upper Column Name to Compare Float Value");
    println!("{:?}", result.upper_column_name_to_compare_float_value);
    println!("Upper Column Name to Alt Compare Float Value");
    println!("{:?}", result.upper_column_name_to_alt_compare_float_value);
}
// *** Read Write ***
#[pyfunction]
pub fn view_csv(df: Dataframe) {          

    let meta_info: MetaInfo = MetaInfo {
        alt_column_name: df.alt_column_name.clone(),
        column_name: df.column_name.clone(),
        column_name_end_address: df.column_name_end_address,
        command_name: df.command_name.clone(),
        command_setting: df.command_setting.clone(),
        delimiter: df.delimiter,
        duration_second: df.duration_second,
        end_column_count: df.end_column_count,
        end_partition_count: df.end_partition_count,
        end_row_count: df.end_row_count,
        end_time: df.end_time,
        error_message: df.error_message.clone(),
        estimate_row: df.estimate_row,
        file_size: df.file_size,
        is_line_br_10_exist: df.is_line_br_10_exist,
        is_line_br_13_exist: df.is_line_br_13_exist,       
        log_file_name: df.log_file_name.clone(),
        partition_address: df.partition_address.clone(),
        partition_count: df.partition_count,
        partition_data: df.partition_data.clone(),
        partition_size_mb: df.partition_size_mb,
        processed_partition: df.processed_partition,
        start_column_count: df.start_column_count,
        start_partition_count: df.start_partition_count,
        start_row_count: df.start_row_count,
        start_time: df.start_time,
        streaming_batch: df.streaming_batch ,
        thread: df.thread,
        validate_row: df.validate_row ,
        value_column_name: df.value_column_name .clone(),        
    };  

    rs_view_csv(&df.vector_table, &meta_info);
    
    
}

pub fn rs_view_csv(vector: &Vec<u8>, meta_info: &MetaInfo) {

    let (is_zero_row, max_column_width) = extract::max_column_width(&vector, meta_info.clone());

    if !is_zero_row {
        let mut total_width = 0;
        let mut current_width = 0;

        for &width in max_column_width.values() {
            total_width += width;
        }

        let mut total_row = 20;
        let mut table_count = 1;

        println!();

        let mut current_column = 0;

        let mut start_column = 0;

        if total_width > 150 {
            total_row = 8;

            while current_column < max_column_width.len() {
                //current_width += max_column_width[&current_column];
                current_width += max_column_width[&(current_column as i32)];
                if current_width > 100 * table_count {
                    extract::current_view(&vector, meta_info.clone(), start_column, current_column as i32, total_row);
                    start_column = current_column as i32;
                    table_count += 1;
                }

                current_column += 1;
            }

            extract::current_view(
                &vector,
                meta_info.clone(),
                start_column,
                max_column_width.len() as i32,
                total_row,
            );
        } else if total_width > 100 {
            total_row = 8;

            while current_column < max_column_width.len() {
               // current_width += max_column_width[&current_column];
               current_width += max_column_width[&(current_column as i32)];
                if current_width > total_width / 2 {
                    extract::current_view(&vector, meta_info.clone(), 0, current_column as i32, total_row);
                    start_column = current_column as i32;
                    break;
                }
                current_column += 1;
            }

            extract::current_view(
                &vector,
                meta_info.clone(),
                start_column,
                max_column_width.len() as i32,
                total_row,
            );
        } else {
            extract::current_view(&vector, meta_info.clone(), 0, max_column_width.len() as i32, total_row);
        }
    }
}

#[pyfunction]
pub fn get_csv_sample(filepath: &str, sample_row: i32) -> PyResult<Dataframe> {       

    let (vector, meta_info) = rs_get_csv_sample(filepath, sample_row);
   
    let result_df = Dataframe {
        alt_column_name: meta_info.alt_column_name.clone(),
        column_name: meta_info.column_name.clone(),
        column_name_end_address: meta_info.column_name_end_address,
        command_name: meta_info.command_name.clone(),
        command_setting: meta_info.command_setting.clone(),
        delimiter: meta_info.delimiter,
        duration_second: meta_info.duration_second,
        end_column_count: meta_info.end_column_count,
        end_partition_count: meta_info.end_partition_count,
        end_row_count: meta_info.end_row_count,
        end_time: meta_info.end_time,
        error_message: meta_info.error_message.clone(),
        estimate_row: meta_info.estimate_row,
        file_size: meta_info.file_size,
        is_line_br_10_exist: meta_info.is_line_br_10_exist,
        is_line_br_13_exist: meta_info.is_line_br_13_exist,
        keyvalue_table: HashMap::new(),
        log_file_name: meta_info.log_file_name.clone(),
        partition_address: meta_info.partition_address.clone(),
        partition_count: meta_info.partition_count,
        partition_data: meta_info.partition_data.clone(),
        partition_size_mb: meta_info.partition_size_mb,
        processed_partition: meta_info.processed_partition,
        start_column_count: meta_info.start_column_count,
        start_partition_count: meta_info.start_partition_count,
        start_row_count: meta_info.start_row_count,
        start_time: meta_info.start_time,
        streaming_batch :meta_info.streaming_batch ,
        thread :meta_info.thread ,
        validate_row :meta_info.validate_row ,
        value_column_name :meta_info.value_column_name .clone(),
        vector_table :  vector,
        vector_table_group : HashMap::new(),
    };   
    Ok(result_df)
}

pub fn rs_get_csv_sample(filepath: &str, mut sample_row: i32) -> (Vec<u8>, MetaInfo) {
    
    let mut meta_info = MetaInfo {
        end_column_count: 0,
        validate_row: 0,
        estimate_row: 0,
        is_line_br_13_exist: false,
        is_line_br_10_exist: false,
        column_name: Vec::new(),
        column_name_end_address: 0,        
        file_size: 0,       
        partition_address: Vec::new(),
        partition_count: 0,
        delimiter: 0,        
        error_message: String::new(),
        alt_column_name: Vec::new(),
        command_name: String::new(),
        command_setting: String::new(),
        duration_second: 0.0,       
        end_partition_count: 1,
        end_row_count: 0,
        end_time: Instant::now(),
        log_file_name: "".to_string(),
        partition_data: Vec::new(),
        partition_size_mb: 0,
        processed_partition: 0,
        start_column_count: 0,
        start_partition_count: 0,
        start_row_count: 0,
        start_time: Instant::now(),
        streaming_batch: 0,
        thread: 0,
        value_column_name: Vec::new(),       
    };     
    
    let mut _is_error: bool = false;
    let mut error_message = String::new();    
    let mut frequency_distribution_by_sample = HashMap::new();  
    let mut csv_vector = Vec::new();
    let mut start_byte = 0;
    let mut sample_byte_count = 0;  
    let mut n = 0;
  
    let mut _delimiter_scenario = HashMap::new(); 

    let file = File::open(filepath);
    let mut file = file.unwrap();  
    let fileinfo = metadata(filepath);         
    let fileinfo = fileinfo.unwrap();    

    meta_info.file_size = fileinfo.len() as i64;

    // Default output number of sample rows 
    if sample_row <= 0 || meta_info.file_size <= 10000 {
        sample_row = 10;
    }

    if meta_info.file_size <= 1000 || sample_row <= 2 {
        sample_row = 2;
    }

    // Column Name
   let (_current_row_byte_count, _frequency_distribution, _current_row_byte) = extract::get_current_row_frequency_distribution(filepath, &mut file, 0);
   
    _delimiter_scenario = _frequency_distribution.clone();

    // Data Row
    while n <= sample_row as i64 - 1 {
        start_byte += 1;

        let (_current_row_byte_count, _frequency_distribution, _current_row_byte) = extract::get_current_row_frequency_distribution(filepath, &mut file, start_byte);
   
        csv_vector.extend(_current_row_byte);
        sample_byte_count += _current_row_byte_count;
        
        frequency_distribution_by_sample.insert(n, _frequency_distribution.clone());       

        let mut temp_delimiter_scenario = HashMap::new();

        for key in _delimiter_scenario.keys() {
            if frequency_distribution_by_sample[&n].contains_key(key) {
                if frequency_distribution_by_sample[&n][key] == _delimiter_scenario[key] {
                    temp_delimiter_scenario.insert(*key, _delimiter_scenario[key]);
                }
            }
        }

        _delimiter_scenario = temp_delimiter_scenario;

        start_byte = meta_info.file_size * n / sample_row as i64;
        n += 1;
    }

    meta_info.validate_row = n;

    // Remove line break from current delimiters
    let mut delimiter_exclude_line_br = HashMap::new();

    for key in _delimiter_scenario.keys() {
        if *key != 10 && *key != 13 {
            delimiter_exclude_line_br.insert(*key, _delimiter_scenario[key]);
        } else {
            if *key == 10 {
                meta_info.is_line_br_10_exist = true;
            } else if *key == 13 {
                meta_info.is_line_br_13_exist = true;
            }
        }
    }

    // Remove abc123 from current delimiters
    let mut delimiter_exclude_abc123 = HashMap::new();

    if delimiter_exclude_line_br.contains_key(&44) {

        meta_info.delimiter = 44;
        meta_info.end_column_count = delimiter_exclude_line_br[&44] + 1;

    } else if delimiter_exclude_line_br.len() == 1 {

        for key in delimiter_exclude_line_br.keys() {

            if *key <= 47
                || (*key >= 58 && *key <= 64)
                || (*key >= 91 && *key <= 96)
                || *key >= 123
            {
                meta_info.delimiter = *key;
                meta_info.end_column_count = delimiter_exclude_line_br[key] + 1;
            }
        }

    // If more than one delimiters, this may be recognized as error
    } else if delimiter_exclude_line_br.len() > 1 {

        for key in delimiter_exclude_line_br.keys() {

            if *key <= 47
                || (*key >= 58 && *key <= 64)
                || (*key >= 91 && *key <= 96)
                || *key >= 123
            {
                delimiter_exclude_abc123.insert(*key, delimiter_exclude_line_br[key]);
            }
        }

        if delimiter_exclude_abc123.len() == 1 {

            for key in delimiter_exclude_abc123.keys() {
                meta_info.delimiter = *key;
                meta_info.end_column_count = delimiter_exclude_line_br[key] + 1;
            }
        } else if delimiter_exclude_abc123.len() > 1 {

            error_message.push_str("** More than one possible delimiter ** \n");
            for key in delimiter_exclude_abc123.keys() {
                error_message.push_str(&format!(
                    "   ASCII{} ({})\n",
                    key,
                    str::from_utf8(&[*key]).unwrap()
                ));
            }           
        }
    }

    // Record error messages
   if _is_error == false  {

    //get_column_name(_filepath: &str, file: &mut File, delimiter: u8)
         let (column_name, column_name_end_address) = extract::get_column_name(filepath, &mut file, meta_info.delimiter);

         meta_info.column_name = column_name;
         meta_info.column_name_end_address = column_name_end_address;
         
         meta_info.estimate_row =
         meta_info.file_size as i64 / sample_byte_count as i64 * sample_row as i64;

         if delimiter_exclude_line_br.is_empty() {            
             error_message.push_str("** Fail to find delimiter ** \n");

         } else {
             if meta_info.end_column_count == 0 {
                 error_message.push_str("** Fail to count number of column ** \n");
             }

             if meta_info.estimate_row == 0 {
                 error_message.push_str("** Fail to estimate number of row ** \n");
             }

             if meta_info.column_name.is_empty() {
                 error_message.push_str("** Fail to find any column name ** \n");
             }

             if meta_info.column_name.len() != meta_info.end_column_count as usize {
                 error_message.push_str("** Number of column name is ");
                 error_message.push_str(&meta_info.column_name.len().to_string());
                 error_message.push_str(", but number of column is ");
                 error_message.push_str(&meta_info.end_column_count.to_string());
                 error_message.push_str(" ** \n");
             }
         }
         meta_info.error_message = error_message
     }    

     (csv_vector, meta_info)

}

#[pyfunction]
pub fn view_sample(file_path: &str) {
    let (vector, meta_info) = rs_get_csv_sample(file_path, 20); // 20 Rows

    if !meta_info.error_message.is_empty() {
        println!();
        println!("{}", meta_info.error_message);
    } else {
        rs_view_csv(&vector, &meta_info);

        print!("File Size: {} bytes", num_format(meta_info.file_size as f64));
        println!("  Total Column: {}", num_format(meta_info.end_column_count as f64));
        print!("Validated Row: {}", num_format(meta_info.validate_row as f64));
        println!("  Estimated Row: {}", num_format(meta_info.estimate_row as f64));
        println!("Column Name End Address: {}", num_format(meta_info.column_name_end_address as f64));
        print!("Column Name: ");

        for (i, column_name) in meta_info.column_name.iter().enumerate() {
            if i < meta_info.column_name.len() - 1 {
                print!("{},", column_name);
            } else {
                println!("{}", column_name);
            }
        }

        if meta_info.delimiter == 0 {
            println!("Delimiter: ");
        } else {
            println!(
                "Delimiter: {} [{}]",
                num_format(meta_info.delimiter as f64),
                meta_info.delimiter as char
            );
        }

        println!(
            "Is Line Br 10/13 Exist: {}/{}",
            meta_info.is_line_br_10_exist, meta_info.is_line_br_13_exist
        );
    }
}

#[pyfunction]
pub fn read_csv(df: &Dataframe, partition_batch: i32, filepath: &str) -> PyResult<Dataframe> {       

    let meta_info: MetaInfo = MetaInfo {
        alt_column_name: df.alt_column_name.clone(),
        column_name: df.column_name.clone(),
        column_name_end_address: df.column_name_end_address,
        command_name: df.command_name.clone(),
        command_setting: df.command_setting.clone(),
        delimiter: df.delimiter,
        duration_second: df.duration_second,
        end_column_count: df.end_column_count,
        end_partition_count: df.end_partition_count,
        end_row_count: df.end_row_count,
        end_time: df.end_time,
        error_message: df.error_message.clone(),
        estimate_row: df.estimate_row,
        file_size: df.file_size,
        is_line_br_10_exist: df.is_line_br_10_exist,
        is_line_br_13_exist: df.is_line_br_13_exist,       
        log_file_name: df.log_file_name.clone(),
        partition_address: df.partition_address.clone(),
        partition_count: df.partition_count,
        partition_data: df.partition_data.clone(),
        partition_size_mb: df.partition_size_mb,
        processed_partition: df.processed_partition,
        start_column_count: df.start_column_count,
        start_partition_count: df.start_partition_count,
        start_row_count: df.start_row_count,
        start_time: df.start_time,
        streaming_batch: df.streaming_batch ,
        thread: df.thread,
        validate_row: df.validate_row ,
        value_column_name: df.value_column_name .clone(),        
    };  

    let (vector_group, meta_info) = rs_read_csv(partition_batch, &meta_info, &filepath);   
   
    let result_df = Dataframe {
        alt_column_name: meta_info.alt_column_name.clone(),
        column_name: meta_info.column_name.clone(),
        column_name_end_address: meta_info.column_name_end_address,
        command_name: meta_info.command_name.clone(),
        command_setting: meta_info.command_setting.clone(),
        delimiter: meta_info.delimiter,
        duration_second: meta_info.duration_second,
        end_column_count: meta_info.end_column_count,
        end_partition_count: meta_info.end_partition_count,
        end_row_count: meta_info.end_row_count,
        end_time: meta_info.end_time,
        error_message: meta_info.error_message.clone(),
        estimate_row: meta_info.estimate_row,
        file_size: meta_info.file_size,
        is_line_br_10_exist: meta_info.is_line_br_10_exist,
        is_line_br_13_exist: meta_info.is_line_br_13_exist,
        keyvalue_table: HashMap::new(),
        log_file_name: meta_info.log_file_name.clone(),
        partition_address: meta_info.partition_address.clone(),
        partition_count: meta_info.partition_count,
        partition_data: meta_info.partition_data.clone(),
        partition_size_mb: meta_info.partition_size_mb,
        processed_partition: meta_info.processed_partition,
        start_column_count: meta_info.start_column_count,
        start_partition_count: meta_info.start_partition_count,
        start_row_count: meta_info.start_row_count,
        start_time: meta_info.start_time,
        streaming_batch :meta_info.streaming_batch ,
        thread :meta_info.thread ,
        validate_row :meta_info.validate_row ,
        value_column_name :meta_info.value_column_name .clone(),
        vector_table :  Vec::new(),
        vector_table_group : vector_group,
    };   
    Ok(result_df)
}

pub fn rs_read_csv(partition_batch: i32, meta_info: &MetaInfo, file_path: &str) -> (HashMap<usize, Vec<u8>>, MetaInfo) {

    let start_time = Instant::now();

    let mut result_meta_info: MetaInfo = meta_info.clone();
    result_meta_info.command_name = "Read_CSV".to_string();
    result_meta_info.command_setting = file_path.to_string();
    result_meta_info.start_column_count = meta_info.end_column_count as i32;
    result_meta_info.start_partition_count = partition_batch as i32;
    result_meta_info.start_row_count = meta_info.end_row_count;
    result_meta_info.start_time = Instant::now();   

    let mut start;
    let mut end;
    let mut file = File::open(file_path).unwrap();
    let mut vector_group: HashMap<usize, Vec<u8>> = HashMap::new();

    for i  in 0..partition_batch {
        start = result_meta_info.partition_address[meta_info.processed_partition as usize + i as usize];
        end = result_meta_info.partition_address[meta_info.processed_partition as usize + 1 + i as usize];
       
        let partition_size = (end - start + 1) as usize;

        let mut vector = vec![0; partition_size as usize];        
        file.seek(SeekFrom::Start(start as u64)).unwrap();
        file.read_exact(&mut vector).unwrap();

        vector_group.insert(i as usize, vector);
        
    }

    result_meta_info.end_partition_count = vector_group.len() as i32;
    result_meta_info.end_column_count = meta_info.end_column_count;
    result_meta_info.start_row_count = meta_info.start_row_count;
    result_meta_info.end_row_count = meta_info.end_row_count;
    result_meta_info.end_time = Instant::now();        

    let end_time = Instant::now();
    result_meta_info.duration_second =
        ((end_time - start_time).as_secs_f64() * 1000.0).round() / 1000.0;
    
    append_log(result_meta_info.clone());

    (vector_group, result_meta_info)
}

#[pyfunction]
pub fn write_csv(df: Dataframe, filepath: &str) {          

    let meta_info: MetaInfo = MetaInfo {
        alt_column_name: df.alt_column_name.clone(),
        column_name: df.column_name.clone(),
        column_name_end_address: df.column_name_end_address,
        command_name: df.command_name.clone(),
        command_setting: df.command_setting.clone(),
        delimiter: df.delimiter,
        duration_second: df.duration_second,
        end_column_count: df.end_column_count,
        end_partition_count: df.end_partition_count,
        end_row_count: df.end_row_count,
        end_time: df.end_time,
        error_message: df.error_message.clone(),
        estimate_row: df.estimate_row,
        file_size: df.file_size,
        is_line_br_10_exist: df.is_line_br_10_exist,
        is_line_br_13_exist: df.is_line_br_13_exist,       
        log_file_name: df.log_file_name.clone(),
        partition_address: df.partition_address.clone(),
        partition_count: df.partition_count,
        partition_data: df.partition_data.clone(),
        partition_size_mb: df.partition_size_mb,
        processed_partition: df.processed_partition,
        start_column_count: df.start_column_count,
        start_partition_count: df.start_partition_count,
        start_row_count: df.start_row_count,
        start_time: df.start_time,
        streaming_batch: df.streaming_batch ,
        thread: df.thread,
        validate_row: df.validate_row ,
        value_column_name: df.value_column_name .clone(),        
    };  

    if df.vector_table_group.len() > 1 {
        rs_write_csv_from_vector_group(&df.vector_table_group, meta_info, filepath)
    } else {
        rs_write_csv(df.vector_table, meta_info, filepath);   
    }
    
}


pub fn rs_write_csv_from_vector_group(vector_group: &HashMap<usize, Vec<u8>>, meta_info: MetaInfo, filepath: &str) {
    let mut csv_string = String::new();

    csv_string.push_str(&meta_info.column_name[0]);

    for column_name in &meta_info.column_name[1..] {
        csv_string.push(',');
        csv_string.push_str(column_name);
    }

    csv_string.push_str("\r\n");

    let mut f = File::create(filepath).unwrap();

    f.write_all(csv_string.as_bytes()).unwrap();
    
     for value in vector_group.values() {
         f.write_all(value).unwrap();
     }
}



pub fn rs_write_csv(vector: Vec<u8>, meta_info: MetaInfo, filepath: &str) {   

    let mut result_meta_info = meta_info.clone();
    result_meta_info.command_name = "Write_CSV".to_string();
    result_meta_info.command_setting = "".to_string();
    result_meta_info.start_partition_count = 1;
    result_meta_info.start_time = Instant::now();

    let mut file = File::create(filepath).unwrap();

    let first_row = utility::column_name_to_byte(&meta_info);  

    file.write_all(&first_row).unwrap();
    file.write_all(&vector).unwrap();    

    result_meta_info.end_partition_count = 1;   
    result_meta_info.end_column_count = meta_info.end_column_count;  
    result_meta_info.start_row_count = meta_info.end_row_count;
    result_meta_info.end_row_count = meta_info.end_row_count;
    result_meta_info.command_setting = filepath.to_string();
    result_meta_info.end_time = Instant::now();

    let end_time = Instant::now();
        result_meta_info.end_time = end_time;

    result_meta_info.duration_second =
    ((end_time - result_meta_info.start_time).as_secs_f64() * 1000.0).round() / 1000.0;
    append_log(result_meta_info);
    
}

#[pyfunction]
pub fn append_csv(df: &Dataframe, filepath: &str) {         

    let mut meta_info: MetaInfo = MetaInfo {
        alt_column_name: df.alt_column_name.clone(),
        column_name: df.column_name.clone(),
        column_name_end_address: df.column_name_end_address,
        command_name: df.command_name.clone(),
        command_setting: df.command_setting.clone(),
        delimiter: df.delimiter,
        duration_second: df.duration_second,
        end_column_count: df.end_column_count,
        end_partition_count: df.end_partition_count,
        end_row_count: df.end_row_count,
        end_time: df.end_time,
        error_message: df.error_message.clone(),
        estimate_row: df.estimate_row,
        file_size: df.file_size,
        is_line_br_10_exist: df.is_line_br_10_exist,
        is_line_br_13_exist: df.is_line_br_13_exist,       
        log_file_name: df.log_file_name.clone(),
        partition_address: df.partition_address.clone(),
        partition_count: df.partition_count,
        partition_data: df.partition_data.clone(),
        partition_size_mb: df.partition_size_mb,
        processed_partition: df.processed_partition,
        start_column_count: df.start_column_count,
        start_partition_count: df.start_partition_count,
        start_row_count: df.start_row_count,
        start_time: df.start_time,
        streaming_batch: df.streaming_batch ,
        thread: df.thread,
        validate_row: df.validate_row ,
        value_column_name: df.value_column_name .clone(),        
    };  

    let error_message = rs_append_csv(&df.vector_table_group, &meta_info, filepath);    

    if error_message.len() > 0 {
        meta_info.error_message = error_message;
        append_log(meta_info);
    }

}

pub fn rs_append_csv(vector_group: &HashMap<usize, Vec<u8>>, meta_info: &MetaInfo, filepath: &str) -> String {
    let start_time = Instant::now();
    let mut result_meta_info = meta_info.clone();
    result_meta_info.command_name = "Append_CSV".to_string();
    result_meta_info.command_setting = "".to_string();
    result_meta_info.start_column_count = meta_info.end_column_count as i32;
    result_meta_info.start_partition_count = meta_info.end_partition_count as i32;
    result_meta_info.start_row_count = meta_info.end_row_count;
    result_meta_info.start_time = Instant::now();

    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(filepath)
        .expect("Failed to open file");

    let mut file = BufWriter::new(file);

    if meta_info.processed_partition == 0 {
        let first_row = utility::column_name_to_byte(&result_meta_info);
        file.write_all(&first_row).unwrap();

        for value in vector_group.values() {
            file.write_all(value).unwrap();
        }
    } else {
        for value in vector_group.values() {
            file.write_all(value).unwrap();
        }
    }

    result_meta_info.end_partition_count = 1;
    result_meta_info.end_column_count = meta_info.end_column_count;
    result_meta_info.start_row_count = meta_info.end_row_count;
    result_meta_info.end_row_count = meta_info.end_row_count;
    result_meta_info.command_setting = filepath.to_string();
    result_meta_info.end_time = Instant::now();

    let end_time = Instant::now();
    result_meta_info.duration_second =
        (end_time - start_time).as_secs_f64().round() / 1000.0;
    append_log(result_meta_info);

    return "".to_string();
}

#[pyfunction]
pub fn get_file_partition(filepath: &str, df: Dataframe, start_byte: i64, end_byte: i64) -> PyResult<Dataframe> {

    let meta_info: MetaInfo = MetaInfo {
        alt_column_name: df.alt_column_name.clone(),
        column_name: df.column_name.clone(),
        column_name_end_address: df.column_name_end_address,
        command_name: df.command_name.clone(),
        command_setting: df.command_setting.clone(),
        delimiter: df.delimiter,
        duration_second: df.duration_second,
        end_column_count: df.end_column_count,
        end_partition_count: df.end_partition_count,
        end_row_count: df.end_row_count,
        end_time: df.end_time,
        error_message: df.error_message.clone(),
        estimate_row: df.estimate_row,
        file_size: df.file_size,
        is_line_br_10_exist: df.is_line_br_10_exist,
        is_line_br_13_exist: df.is_line_br_13_exist,       
        log_file_name: df.log_file_name.clone(),
        partition_address: df.partition_address.clone(),
        partition_count: df.partition_count,
        partition_data: df.partition_data.clone(),
        partition_size_mb: df.partition_size_mb,
        processed_partition: df.processed_partition,
        start_column_count: df.start_column_count,
        start_partition_count: df.start_partition_count,
        start_row_count: df.start_row_count,
        start_time: df.start_time,
        streaming_batch: df.streaming_batch ,
        thread: df.thread,
        validate_row: df.validate_row ,
        value_column_name: df.value_column_name .clone(),        
    };  

    let (vector, meta_info) = rs_get_file_partition(filepath, meta_info, start_byte, end_byte);

    let result_df = Dataframe {
        alt_column_name: meta_info.alt_column_name.clone(),
        column_name: meta_info.column_name.clone(),
        column_name_end_address: meta_info.column_name_end_address,
        command_name: meta_info.command_name.clone(),
        command_setting: meta_info.command_setting.clone(),
        delimiter: meta_info.delimiter,
        duration_second: meta_info.duration_second,
        end_column_count: meta_info.end_column_count,
        end_partition_count: meta_info.end_partition_count,
        end_row_count: meta_info.end_row_count,
        end_time: meta_info.end_time,
        error_message: meta_info.error_message.clone(),
        estimate_row: meta_info.estimate_row,
        file_size: meta_info.file_size,
        is_line_br_10_exist: meta_info.is_line_br_10_exist,
        is_line_br_13_exist: meta_info.is_line_br_13_exist,
        keyvalue_table: HashMap::new(),
        log_file_name: meta_info.log_file_name.clone(),
        partition_address: meta_info.partition_address.clone(),
        partition_count: meta_info.partition_count,
        partition_data: meta_info.partition_data.clone(),
        partition_size_mb: meta_info.partition_size_mb,
        processed_partition: meta_info.processed_partition,
        start_column_count: meta_info.start_column_count,
        start_partition_count: meta_info.start_partition_count,
        start_row_count: meta_info.start_row_count,
        start_time: meta_info.start_time,
        streaming_batch: meta_info.streaming_batch ,
        thread :meta_info.thread ,
        validate_row: meta_info.validate_row ,
        value_column_name :meta_info.value_column_name .clone(),
        vector_table: vector,
        vector_table_group: HashMap::new(),
    };   
    Ok(result_df)
}


pub fn rs_get_file_partition(filepath: &str, meta_info: MetaInfo, start_byte: i64, end_byte: i64) -> (Vec<u8>, MetaInfo) {

    let file = File::open(filepath);
    let mut file = file.unwrap();      
    let partition_size = end_byte - start_byte + 1;
    let mut vector = vec![0; partition_size as usize];
        
    file.seek(SeekFrom::Start(start_byte as u64)).unwrap();
    file.read_exact(&mut vector).unwrap();
   
    (vector, meta_info)
}

#[pyfunction]
pub fn get_default_partition_address(filepath: &str) -> PyResult<Dataframe> {
   
    let meta_info = MetaInfo {
        end_column_count: 0,
        validate_row: 0,
        estimate_row: 0,
        is_line_br_13_exist: false,
        is_line_br_10_exist: false,
        column_name: Vec::new(),
        column_name_end_address: 0,        
        file_size: 0,       
        partition_address: Vec::new(),
        partition_count: 0,
        delimiter: 0,        
        error_message: String::new(),
        alt_column_name: Vec::new(),
        command_name: String::new(),
        command_setting: String::new(),
        duration_second: 0.0,       
        end_partition_count: 1,
        end_row_count: 0,
        end_time: Instant::now(),
        log_file_name: "".to_string(),
        partition_data: Vec::new(),
        partition_size_mb: 0,
        processed_partition: 0,
        start_column_count: 0,
        start_partition_count: 0,
        start_row_count: 0,
        start_time: Instant::now(),
        streaming_batch: 0,
        thread: 0,
        value_column_name: Vec::new(),       
    };     

    let (vector, meta_info) = rs_get_csv_partition_address(&filepath, &meta_info);
   
    let result_df = Dataframe {
        alt_column_name: meta_info.alt_column_name.clone(),
        column_name: meta_info.column_name.clone(),
        column_name_end_address: meta_info.column_name_end_address,
        command_name: meta_info.command_name.clone(),
        command_setting: meta_info.command_setting.clone(),
        delimiter: meta_info.delimiter,
        duration_second: meta_info.duration_second,
        end_column_count: meta_info.end_column_count,
        end_partition_count: meta_info.end_partition_count,
        end_row_count: meta_info.end_row_count,
        end_time: meta_info.end_time,
        error_message: meta_info.error_message.clone(),
        estimate_row: meta_info.estimate_row,
        file_size: meta_info.file_size,
        is_line_br_10_exist: meta_info.is_line_br_10_exist,
        is_line_br_13_exist: meta_info.is_line_br_13_exist,
        keyvalue_table: HashMap::new(),
        log_file_name: meta_info.log_file_name.clone(),
        partition_address: meta_info.partition_address.clone(),
        partition_count: meta_info.partition_count,
        partition_data: meta_info.partition_data.clone(),
        partition_size_mb: meta_info.partition_size_mb,
        processed_partition: meta_info.processed_partition,
        start_column_count: meta_info.start_column_count,
        start_partition_count: meta_info.start_partition_count,
        start_row_count: meta_info.start_row_count,
        start_time: meta_info.start_time,
        streaming_batch :meta_info.streaming_batch ,
        thread :meta_info.thread ,
        validate_row :meta_info.validate_row ,
        value_column_name :meta_info.value_column_name .clone(),
        vector_table :  vector,
        vector_table_group : HashMap::new(),
    }; 
    Ok(result_df)
}


#[pyfunction]
pub fn get_csv_partition_address(df: &Dataframe, filepath: &str) -> PyResult<Dataframe> {

    let meta_info: MetaInfo = MetaInfo {
        alt_column_name: df.alt_column_name.clone(),
        column_name: df.column_name.clone(),
        column_name_end_address: df.column_name_end_address,
        command_name: df.command_name.clone(),
        command_setting: df.command_setting.clone(),
        delimiter: df.delimiter,
        duration_second: df.duration_second,
        end_column_count: df.end_column_count,
        end_partition_count: df.end_partition_count,
        end_row_count: df.end_row_count,
        end_time: df.end_time,
        error_message: df.error_message.clone(),
        estimate_row: df.estimate_row,
        file_size: df.file_size,
        is_line_br_10_exist: df.is_line_br_10_exist,
        is_line_br_13_exist: df.is_line_br_13_exist,       
        log_file_name: df.log_file_name.clone(),
        partition_address: df.partition_address.clone(),
        partition_count: df.partition_count,
        partition_data: df.partition_data.clone(),
        partition_size_mb: df.partition_size_mb,
        processed_partition: df.processed_partition,
        start_column_count: df.start_column_count,
        start_partition_count: df.start_partition_count,
        start_row_count: df.start_row_count,
        start_time: df.start_time,
        streaming_batch: df.streaming_batch ,
        thread: df.thread,
        validate_row: df.validate_row ,
        value_column_name: df.value_column_name .clone(),        
    };  

    let (vector, meta_info) = rs_get_csv_partition_address(&filepath, &meta_info);
   
    let result_df = Dataframe {
        alt_column_name: meta_info.alt_column_name.clone(),
        column_name: meta_info.column_name.clone(),
        column_name_end_address: meta_info.column_name_end_address,
        command_name: meta_info.command_name.clone(),
        command_setting: meta_info.command_setting.clone(),
        delimiter: meta_info.delimiter,
        duration_second: meta_info.duration_second,
        end_column_count: meta_info.end_column_count,
        end_partition_count: meta_info.end_partition_count,
        end_row_count: meta_info.end_row_count,
        end_time: meta_info.end_time,
        error_message: meta_info.error_message.clone(),
        estimate_row: meta_info.estimate_row,
        file_size: meta_info.file_size,
        is_line_br_10_exist: meta_info.is_line_br_10_exist,
        is_line_br_13_exist: meta_info.is_line_br_13_exist,
        keyvalue_table: HashMap::new(),
        log_file_name: meta_info.log_file_name.clone(),
        partition_address: meta_info.partition_address.clone(),
        partition_count: meta_info.partition_count,
        partition_data: meta_info.partition_data.clone(),
        partition_size_mb: meta_info.partition_size_mb,
        processed_partition: meta_info.processed_partition,
        start_column_count: meta_info.start_column_count,
        start_partition_count: meta_info.start_partition_count,
        start_row_count: meta_info.start_row_count,
        start_time: meta_info.start_time,
        streaming_batch :meta_info.streaming_batch ,
        thread :meta_info.thread ,
        validate_row :meta_info.validate_row ,
        value_column_name :meta_info.value_column_name .clone(),
        vector_table :  vector,
        vector_table_group : HashMap::new(),
    }; 
    Ok(result_df)
}

pub fn rs_get_csv_partition_address(filepath: &str, meta_info: &MetaInfo) -> (Vec<u8>, MetaInfo) {  

    let start_time = Instant::now();
    
    let mut result_meta_info = meta_info.clone();
    result_meta_info.command_name =  "Get_CSV_Partition_Address".to_string();
    result_meta_info.command_setting = filepath.to_string();
    result_meta_info.start_column_count = 0;
    result_meta_info.start_partition_count = 0;
    result_meta_info.start_column_count = meta_info.end_column_count as i32;
    result_meta_info.start_partition_count = meta_info.partition_count as i32;
    result_meta_info.start_row_count = meta_info.end_row_count;
    result_meta_info.start_time = Instant::now();  

    if result_meta_info.partition_size_mb <= 0 {
        result_meta_info.partition_size_mb = 20;
    }

    let partition_size_in_byte = result_meta_info.partition_size_mb * 1024 * 1024;
    let (_, meta_info) = rs_get_csv_sample(filepath, 100);
    
    let number_of_partition:i32  = (meta_info.file_size / partition_size_in_byte as i64) as i32;   
   
    let (vector, _) = rs_get_csv_sample(filepath, number_of_partition as i32);
    

    let mut partition_address = vec![];

    let mut file = File::open(filepath).unwrap();
    
    let current_address = extract::get_current_row_ending_address(&mut file, 0);
   
    partition_address.push(current_address + 1);
  

    let mut n = 1;
    
	while n <= number_of_partition {
        let current_start_byte: i64 = partition_size_in_byte as i64 * n as i64;
		let current_address = extract::get_current_row_ending_address(&mut file, current_start_byte);
		partition_address.push(current_address + 1);
		n += 1;
	}   

    partition_address.push(meta_info.file_size - 1);
    result_meta_info.estimate_row = meta_info.estimate_row;
    result_meta_info.is_line_br_13_exist = meta_info.is_line_br_13_exist;
    result_meta_info.is_line_br_10_exist = meta_info.is_line_br_10_exist;
    result_meta_info.partition_count = n;
    result_meta_info.partition_address = partition_address;
    result_meta_info.end_partition_count = n;
    result_meta_info.end_column_count = meta_info.end_column_count;
    result_meta_info.start_row_count = 0;
    result_meta_info.end_row_count = 0;
    result_meta_info.estimate_row = meta_info.estimate_row;
    result_meta_info.file_size = meta_info.file_size;
    result_meta_info.delimiter = meta_info.delimiter.clone();
    result_meta_info.end_time = Instant::now();

    result_meta_info.column_name = meta_info.column_name.clone();

    result_meta_info.duration_second =
        (result_meta_info.end_time.duration_since(start_time).as_secs_f64() * 1000.0).round()/ 1000.0;
 
    let end_time = Instant::now();
    result_meta_info.duration_second =
        ((end_time - start_time).as_secs_f64() * 1000.0).round() / 1000.0;
    
    append_log(result_meta_info.clone());

    (vector, result_meta_info)
}

fn _write_csv_from_vector_group(vector_group: &HashMap<usize, Vec<u8>>, meta_info: Dataframe, filepath: &str) {

    
    let mut csv_string = String::new();

    csv_string.push_str(&meta_info.column_name[0]);

    for column_name in &meta_info.column_name[1..] {
        csv_string.push(',');
        csv_string.push_str(column_name);
    }

    csv_string.push_str("\r\n");

    let mut f = File::create(filepath).unwrap();

    f.write_all(csv_string.as_bytes()).unwrap();
    
     for value in vector_group.values() {
         f.write_all(value).unwrap();
     }
}

// *** Query ***

#[pyfunction]
pub fn add_column(df: &Dataframe, query: &str) -> PyResult<Dataframe> {    

    let meta_info: MetaInfo = MetaInfo {
        alt_column_name: df.alt_column_name.clone(),
        column_name: df.column_name.clone(),
        column_name_end_address: df.column_name_end_address,
        command_name: df.command_name.clone(),
        command_setting: df.command_setting.clone(),
        delimiter: df.delimiter,
        duration_second: df.duration_second,
        end_column_count: df.end_column_count,
        end_partition_count: df.end_partition_count,
        end_row_count: df.end_row_count,
        end_time: df.end_time,
        error_message: df.error_message.clone(),
        estimate_row: df.estimate_row,
        file_size: df.file_size,
        is_line_br_10_exist: df.is_line_br_10_exist,
        is_line_br_13_exist: df.is_line_br_13_exist,       
        log_file_name: df.log_file_name.clone(),
        partition_address: df.partition_address.clone(),
        partition_count: df.partition_count,
        partition_data: df.partition_data.clone(),
        partition_size_mb: df.partition_size_mb,
        processed_partition: df.processed_partition,
        start_column_count: df.start_column_count,
        start_partition_count: df.start_partition_count,
        start_row_count: df.start_row_count,
        start_time: df.start_time,
        streaming_batch: df.streaming_batch ,
        thread: df.thread,
        validate_row: df.validate_row ,
        value_column_name: df.value_column_name .clone(),        
    };   

    let (vector_group, meta_info) = rs_add_column(&df.vector_table_group, &meta_info, query);
   
    let result_df = Dataframe {
        alt_column_name: meta_info.alt_column_name.clone(),
        column_name: meta_info.column_name.clone(),
        column_name_end_address: meta_info.column_name_end_address,
        command_name: meta_info.command_name.clone(),
        command_setting: meta_info.command_setting.clone(),
        delimiter: meta_info.delimiter,
        duration_second: meta_info.duration_second,
        end_column_count: meta_info.end_column_count,
        end_partition_count: meta_info.end_partition_count,
        end_row_count: meta_info.end_row_count,
        end_time: meta_info.end_time,
        error_message: meta_info.error_message.clone(),
        estimate_row: meta_info.estimate_row,
        file_size: meta_info.file_size,
        is_line_br_10_exist: meta_info.is_line_br_10_exist,
        is_line_br_13_exist: meta_info.is_line_br_13_exist,
        keyvalue_table: HashMap::new(),
        log_file_name: meta_info.log_file_name.clone(),
        partition_address: meta_info.partition_address.clone(),
        partition_count: meta_info.partition_count,
        partition_data: meta_info.partition_data.clone(),
        partition_size_mb: meta_info.partition_size_mb,
        processed_partition: meta_info.processed_partition,
        start_column_count: meta_info.start_column_count,
        start_partition_count: meta_info.start_partition_count,
        start_row_count: meta_info.start_row_count,
        start_time: meta_info.start_time,
        streaming_batch: meta_info.streaming_batch ,
        thread :meta_info.thread ,
        validate_row: meta_info.validate_row ,
        value_column_name :meta_info.value_column_name .clone(),
        vector_table: Vec::new(),
        vector_table_group: vector_group,
    };   
    Ok(result_df)

}

pub fn rs_add_column(vector_group: &HashMap<usize, Vec<u8>>, meta_info: &MetaInfo, query: &str) -> (HashMap<usize, Vec<u8>>, MetaInfo) {
    
    let mut query_validation = parse_setting::validate_add_column_setting("Add_Column", query, meta_info.column_name.clone());
    query_validation.current_command = "Add_Column".to_string();
    let mut result_meta_info = update_start_meta_info(&vector_group, &meta_info, query, query_validation.current_command.clone());   
    
    
    let mut result_vector_group = HashMap::new();
    let mut result_meta_info_group: HashMap<usize, MetaInfo> = HashMap::new();

    if !query_validation.error_message.is_empty() {
        println!("{}", query_validation.error_message);
        result_meta_info.error_message = query_validation.error_message;
        append_log(result_meta_info.clone());
        return (result_vector_group, result_meta_info);

    } else {

        if meta_info.thread <= 1 {

            let (result_vector, result_meta_info) = query::current_add_column(1, 0, &vector_group, &meta_info, &query_validation);
            result_vector_group.insert(0, result_vector);
            result_meta_info_group.insert(0, result_meta_info.clone());
            let result_meta_info = update_end_meta_info(&vector_group, &result_meta_info, &result_meta_info_group);
            return (result_vector_group, result_meta_info);

        } else {

            let return_vector_group: Mutex<HashMap<usize, Vec<u8>>> = Mutex::new(HashMap::<usize, Vec<u8>>::new());
            let return_meta_info_group = Mutex::new(HashMap::<usize, MetaInfo>::new());        
            let total_partition: usize = vector_group.len();    
                
            (0..total_partition).into_par_iter().for_each(|n| {               
                let (result_vector, result_meta_info) = query::current_add_column(1, n as usize, &vector_group, &meta_info, &query_validation);          
                return_vector_group.lock().unwrap().insert(n, result_vector);
                return_meta_info_group.lock().unwrap().insert(n, result_meta_info);                    
            });
        
            let result_vector_group: HashMap<usize, Vec<u8>> = return_vector_group.into_inner().unwrap();
            let result_meta_info_group: HashMap<usize, MetaInfo> = return_meta_info_group.into_inner().unwrap();    
            result_meta_info = update_end_meta_info(&vector_group, &result_meta_info, &result_meta_info_group);
            return (result_vector_group, result_meta_info);
        }
	}
}

#[pyfunction]
pub fn build_key_value(df: &Dataframe, query: &str) -> PyResult<Dataframe> {   

    let meta_info: MetaInfo = MetaInfo {
        alt_column_name: df.alt_column_name.clone(),
        column_name: df.column_name.clone(),
        column_name_end_address: df.column_name_end_address,
        command_name: df.command_name.clone(),
        command_setting: df.command_setting.clone(),
        delimiter: df.delimiter,
        duration_second: df.duration_second,
        end_column_count: df.end_column_count,
        end_partition_count: df.end_partition_count,
        end_row_count: df.end_row_count,
        end_time: df.end_time,
        error_message: df.error_message.clone(),
        estimate_row: df.estimate_row,
        file_size: df.file_size,
        is_line_br_10_exist: df.is_line_br_10_exist,
        is_line_br_13_exist: df.is_line_br_13_exist,       
        log_file_name: df.log_file_name.clone(),
        partition_address: df.partition_address.clone(),
        partition_count: df.partition_count,
        partition_data: df.partition_data.clone(),
        partition_size_mb: df.partition_size_mb,
        processed_partition: df.processed_partition,
        start_column_count: df.start_column_count,
        start_partition_count: df.start_partition_count,
        start_row_count: df.start_row_count,
        start_time: df.start_time,
        streaming_batch: df.streaming_batch ,
        thread: df.thread,
        validate_row: df.validate_row ,
        value_column_name: df.value_column_name .clone(),        
    };   

    let (key_value, meta_info) = rs_build_key_value(&df.vector_table_group , &meta_info, query);
   
       
    let result_df = Dataframe {
        alt_column_name: meta_info.alt_column_name.clone(),
        column_name: meta_info.column_name.clone(),
        column_name_end_address: meta_info.column_name_end_address,
        command_name: meta_info.command_name.clone(),
        command_setting: meta_info.command_setting.clone(),
        delimiter: meta_info.delimiter,
        duration_second: meta_info.duration_second,
        end_column_count: meta_info.end_column_count,
        end_partition_count: meta_info.end_partition_count,
        end_row_count: meta_info.end_row_count,
        end_time: meta_info.end_time,
        error_message: meta_info.error_message.clone(),
        estimate_row: meta_info.estimate_row,
        file_size: meta_info.file_size,
        is_line_br_10_exist: meta_info.is_line_br_10_exist,
        is_line_br_13_exist: meta_info.is_line_br_13_exist,
        keyvalue_table: key_value,
        log_file_name: meta_info.log_file_name.clone(),
        partition_address: meta_info.partition_address.clone(),
        partition_count: meta_info.partition_count,
        partition_data: meta_info.partition_data.clone(),
        partition_size_mb: meta_info.partition_size_mb,
        processed_partition: meta_info.processed_partition,
        start_column_count: meta_info.start_column_count,
        start_partition_count: meta_info.start_partition_count,
        start_row_count: meta_info.start_row_count,
        start_time: meta_info.start_time,
        streaming_batch :meta_info.streaming_batch ,
        thread :meta_info.thread ,
        validate_row :meta_info.validate_row ,
        value_column_name :meta_info.value_column_name.clone(),
        vector_table :  Vec::new(),
        vector_table_group : HashMap::new(),
    };  
     
    Ok(result_df)
}

pub fn rs_build_key_value(vector_group: &HashMap<usize, Vec<u8>>, meta_info: &MetaInfo, query: &str) -> (HashMap<String, Vec<u8>>, MetaInfo) {      

    let mut query_validation = parse_setting::validate_add_column_setting("Build_Key_Value", query, meta_info.column_name.clone());
    query_validation.current_command = "Build_Key_Value".to_string();
    let result_meta_info: MetaInfo = update_start_meta_info(&vector_group, meta_info, query, query_validation.current_command.clone());   

    let (result_key_value, result_meta_info) = query::current_build_key_value(1, 0, &vector_group, &result_meta_info, &query_validation);

    let mut result_meta_info_group: HashMap<usize, MetaInfo> = HashMap::new();
    result_meta_info_group.insert(0, result_meta_info.clone());

    let result_meta_info: MetaInfo = update_end_meta_info(&vector_group, &result_meta_info, &result_meta_info_group);
    
    return (result_key_value, result_meta_info);
}

#[pyfunction]
pub fn create_folder_lake(df: &Dataframe, query: &str) -> PyResult<Dataframe> {

    let meta_info: MetaInfo = MetaInfo {
        alt_column_name: df.alt_column_name.clone(),
        column_name: df.column_name.clone(),
        column_name_end_address: df.column_name_end_address,
        command_name: df.command_name.clone(),
        command_setting: df.command_setting.clone(),
        delimiter: df.delimiter,
        duration_second: df.duration_second,
        end_column_count: df.end_column_count,
        end_partition_count: df.end_partition_count,
        end_row_count: df.end_row_count,
        end_time: df.end_time,
        error_message: df.error_message.clone(),
        estimate_row: df.estimate_row,
        file_size: df.file_size,
        is_line_br_10_exist: df.is_line_br_10_exist,
        is_line_br_13_exist: df.is_line_br_13_exist,       
        log_file_name: df.log_file_name.clone(),
        partition_address: df.partition_address.clone(),
        partition_count: df.partition_count,
        partition_data: df.partition_data.clone(),
        partition_size_mb: df.partition_size_mb,
        processed_partition: df.processed_partition,
        start_column_count: df.start_column_count,
        start_partition_count: df.start_partition_count,
        start_row_count: df.start_row_count,
        start_time: df.start_time,
        streaming_batch: df.streaming_batch ,
        thread: df.thread,
        validate_row: df.validate_row ,
        value_column_name: df.value_column_name .clone(),        
    };  

    let meta_info = rs_create_folder_lake(&df.vector_table_group, &meta_info, query);

    let result_df = Dataframe {
        alt_column_name: meta_info.alt_column_name.clone(),
        column_name: meta_info.column_name.clone(),
        column_name_end_address: meta_info.column_name_end_address,
        command_name: meta_info.command_name.clone(),
        command_setting: meta_info.command_setting.clone(),
        delimiter: meta_info.delimiter,
        duration_second: meta_info.duration_second,
        end_column_count: meta_info.end_column_count,
        end_partition_count: meta_info.end_partition_count,
        end_row_count: meta_info.end_row_count,
        end_time: meta_info.end_time,
        error_message: meta_info.error_message.clone(),
        estimate_row: meta_info.estimate_row,
        file_size: meta_info.file_size,
        is_line_br_10_exist: meta_info.is_line_br_10_exist,
        is_line_br_13_exist: meta_info.is_line_br_13_exist,
        keyvalue_table: HashMap::new(),
        log_file_name: meta_info.log_file_name.clone(),
        partition_address: meta_info.partition_address.clone(),
        partition_count: meta_info.partition_count,
        partition_data: meta_info.partition_data.clone(),
        partition_size_mb: meta_info.partition_size_mb,
        processed_partition: meta_info.processed_partition,
        start_column_count: meta_info.start_column_count,
        start_partition_count: meta_info.start_partition_count,
        start_row_count: meta_info.start_row_count,
        start_time: meta_info.start_time,
        streaming_batch :meta_info.streaming_batch ,
        thread :meta_info.thread ,
        validate_row :meta_info.validate_row ,
        value_column_name :meta_info.value_column_name .clone(),
        vector_table :  Vec::new(),
        vector_table_group : HashMap::new(),
    };   
    Ok(result_df)
}

pub fn rs_create_folder_lake(vector_group: &HashMap<usize, Vec<u8>>, meta_info: &MetaInfo, query: &str) -> MetaInfo {
    
    let mut query_validation = parse_setting::validate_add_column_setting("Create_Folder_Lake", query, meta_info.column_name.clone());
    query_validation.current_command = "Create_Folder_Lake".to_string();
    let mut result_meta_info = update_start_meta_info(&vector_group, &meta_info, query, query_validation.current_command.clone());          
    
    let mut result_meta_info_group: HashMap<usize, MetaInfo> = HashMap::new();

    if !query_validation.error_message.is_empty() {
        println!("{}", query_validation.error_message);
        result_meta_info.error_message = query_validation.error_message;
        append_log(result_meta_info.clone());
        return result_meta_info;

    } else {

        if meta_info.thread <= 1 {

            let result_meta_info = query::current_create_folder_lake(1, 0, &vector_group, &meta_info, &query_validation);           
            result_meta_info_group.insert(0, result_meta_info.clone());
            let result_meta_info = update_end_meta_info(&vector_group, &result_meta_info, &result_meta_info_group);
            return result_meta_info;

        } else {
           
            let return_meta_info_group = Mutex::new(HashMap::<usize, MetaInfo>::new());        
            let total_partition: usize = vector_group.len();    
                
            (0..total_partition).into_par_iter().for_each(|n| {               
                let result_meta_info = query::current_create_folder_lake(1, n as usize, &vector_group, &meta_info, &query_validation);                        
                return_meta_info_group.lock().unwrap().insert(n, result_meta_info);                    
            });       
           
            let result_meta_info_group: HashMap<usize, MetaInfo> = return_meta_info_group.into_inner().unwrap();    
            result_meta_info = update_end_meta_info(&vector_group, &result_meta_info, &result_meta_info_group);
            return result_meta_info;
        }
	}
}

#[pyfunction]
pub fn distinct(df: &Dataframe, query: &str) -> PyResult<Dataframe> {

    let meta_info: MetaInfo = MetaInfo {
        alt_column_name: df.alt_column_name.clone(),
        column_name: df.column_name.clone(),
        column_name_end_address: df.column_name_end_address,
        command_name: df.command_name.clone(),
        command_setting: df.command_setting.clone(),
        delimiter: df.delimiter,
        duration_second: df.duration_second,
        end_column_count: df.end_column_count,
        end_partition_count: df.end_partition_count,
        end_row_count: df.end_row_count,
        end_time: df.end_time,
        error_message: df.error_message.clone(),
        estimate_row: df.estimate_row,
        file_size: df.file_size,
        is_line_br_10_exist: df.is_line_br_10_exist,
        is_line_br_13_exist: df.is_line_br_13_exist,       
        log_file_name: df.log_file_name.clone(),
        partition_address: df.partition_address.clone(),
        partition_count: df.partition_count,
        partition_data: df.partition_data.clone(),
        partition_size_mb: df.partition_size_mb,
        processed_partition: df.processed_partition,
        start_column_count: df.start_column_count,
        start_partition_count: df.start_partition_count,
        start_row_count: df.start_row_count,
        start_time: df.start_time,
        streaming_batch: df.streaming_batch ,
        thread: df.thread,
        validate_row: df.validate_row ,
        value_column_name: df.value_column_name .clone(),        
    };  

    let (vector, meta_info) = rs_distinct(&df.vector_table_group, &meta_info, query);
   
    let result_df = Dataframe {
        alt_column_name: meta_info.alt_column_name.clone(),
        column_name: meta_info.column_name.clone(),
        column_name_end_address: meta_info.column_name_end_address,
        command_name: meta_info.command_name.clone(),
        command_setting: meta_info.command_setting.clone(),
        delimiter: meta_info.delimiter,
        duration_second: meta_info.duration_second,
        end_column_count: meta_info.end_column_count,
        end_partition_count: meta_info.end_partition_count,
        end_row_count: meta_info.end_row_count,
        end_time: meta_info.end_time,
        error_message: meta_info.error_message.clone(),
        estimate_row: meta_info.estimate_row,
        file_size: meta_info.file_size,
        is_line_br_10_exist: meta_info.is_line_br_10_exist,
        is_line_br_13_exist: meta_info.is_line_br_13_exist,
        keyvalue_table: HashMap::new(),
        log_file_name: meta_info.log_file_name.clone(),
        partition_address: meta_info.partition_address.clone(),
        partition_count: meta_info.partition_count,
        partition_data: meta_info.partition_data.clone(),
        partition_size_mb: meta_info.partition_size_mb,
        processed_partition: meta_info.processed_partition,
        start_column_count: meta_info.start_column_count,
        start_partition_count: meta_info.start_partition_count,
        start_row_count: meta_info.start_row_count,
        start_time: meta_info.start_time,
        streaming_batch :meta_info.streaming_batch ,
        thread :meta_info.thread ,
        validate_row :meta_info.validate_row ,
        value_column_name :meta_info.value_column_name .clone(),
        vector_table :  vector,
        vector_table_group : HashMap::new(),
    };   
    Ok(result_df)
}

pub fn rs_distinct(vector_group: &HashMap<usize, Vec<u8>>, meta_info: &MetaInfo, query: &str) -> (Vec<u8>, MetaInfo) {
    
    let mut query_validation = parse_setting::validate_column_setting(query, meta_info.column_name.clone());
    query_validation.current_command = "Distinct".to_string();
    let mut result_meta_info = update_start_meta_info(&vector_group, &meta_info, query, query_validation.current_command.clone());       
    
    if !query_validation.error_message.is_empty() {
        println!("{}", query_validation.error_message);
        result_meta_info.error_message = query_validation.error_message;
        append_log(result_meta_info.clone());
        let vector: Vec<u8> = Vec::new();
        return (vector, result_meta_info);

    } else {

        if meta_info.thread <= 1 {

          let (result_vector, meta_info) = query::current_unique(1, 0, &vector_group, &meta_info, &query_validation);
          
           result_meta_info.end_partition_count = 1;
           result_meta_info.end_column_count = meta_info.end_column_count;
           result_meta_info.end_row_count = meta_info.end_row_count;           

           let end_time = Instant::now();

           result_meta_info.end_time = end_time;

           
           result_meta_info.duration_second =
           ((end_time - result_meta_info.start_time).as_secs_f64() * 1000.0).round() / 1000.0;
           append_log(result_meta_info.clone());
            return (result_vector, result_meta_info);

        } else {

            let return_vector_group: Mutex<HashMap<usize, Vec<u8>>> = Mutex::new(HashMap::<usize, Vec<u8>>::new());
            let return_meta_info_group = Mutex::new(HashMap::<usize, MetaInfo>::new());        
            let total_partition: usize = vector_group.len();             
                
            (0..total_partition).into_par_iter().for_each(|n| {                        
                let (result_vector, result_meta_info) = query::current_unique(1, n as usize, &vector_group, &meta_info, &query_validation);          
                return_vector_group.lock().unwrap().insert(n, result_vector);
                return_meta_info_group.lock().unwrap().insert(n, result_meta_info);                    
            });
        
            let multithread_vector_group: HashMap<usize, Vec<u8>> = return_vector_group.into_inner().unwrap();
            let multithread_meta_info_group: HashMap<usize, MetaInfo> = return_meta_info_group.into_inner().unwrap();    

            let total_partition = multithread_vector_group.len();
            let mut result_vector_group: HashMap<usize, Vec<u8>> = HashMap::new();
            let mut result_meta_info_group: HashMap<usize, MetaInfo>= HashMap::new();
           
            let query_validation = parse_setting::validate_column_setting(query, multithread_meta_info_group[&0].column_name.clone());           
            
            let (vector, meta_info) = query::current_unique(total_partition, 0, &multithread_vector_group, &multithread_meta_info_group[&0], &query_validation);            
        
            result_meta_info_group.insert(0, meta_info);
            result_vector_group.insert(0, vector.clone());
          
            result_meta_info.end_partition_count = 1;
            result_meta_info.end_column_count = result_meta_info_group[&0].end_column_count;
            result_meta_info.end_row_count = result_meta_info_group[&0].end_row_count;
            result_meta_info.column_name = result_meta_info_group[&0].column_name.clone();
			result_meta_info.alt_column_name = result_meta_info_group[&0].alt_column_name.clone();         

            let end_time = Instant::now();
            result_meta_info.end_time = end_time;
            
            result_meta_info.duration_second =
            ((end_time - result_meta_info.start_time).as_secs_f64() * 1000.0).round() / 1000.0;
            append_log(result_meta_info.clone());
            
            return (vector, result_meta_info);
        }
	}
}

#[pyfunction]
pub fn filter(df: &Dataframe, query: &str) -> PyResult<Dataframe> {  

    let meta_info: MetaInfo = MetaInfo {
        alt_column_name: df.alt_column_name.clone(),
        column_name: df.column_name.clone(),
        column_name_end_address: df.column_name_end_address,
        command_name: df.command_name.clone(),
        command_setting: df.command_setting.clone(),
        delimiter: df.delimiter,
        duration_second: df.duration_second,
        end_column_count: df.end_column_count,
        end_partition_count: df.end_partition_count,
        end_row_count: df.end_row_count,
        end_time: df.end_time,
        error_message: df.error_message.clone(),
        estimate_row: df.estimate_row,
        file_size: df.file_size,
        is_line_br_10_exist: df.is_line_br_10_exist,
        is_line_br_13_exist: df.is_line_br_13_exist,       
        log_file_name: df.log_file_name.clone(),
        partition_address: df.partition_address.clone(),
        partition_count: df.partition_count,
        partition_data: df.partition_data.clone(),
        partition_size_mb: df.partition_size_mb,
        processed_partition: df.processed_partition,
        start_column_count: df.start_column_count,
        start_partition_count: df.start_partition_count,
        start_row_count: df.start_row_count,
        start_time: df.start_time,
        streaming_batch: df.streaming_batch ,
        thread: df.thread,
        validate_row: df.validate_row ,
        value_column_name: df.value_column_name .clone(),        
    };  

    let (vector_group, meta_info) = rs_filter(&df.vector_table_group, &meta_info, query);
   
    let result_df = Dataframe {
        alt_column_name: meta_info.alt_column_name.clone(),
        column_name: meta_info.column_name.clone(),
        column_name_end_address: meta_info.column_name_end_address,
        command_name: meta_info.command_name.clone(),
        command_setting: meta_info.command_setting.clone(),
        delimiter: meta_info.delimiter,
        duration_second: meta_info.duration_second,
        end_column_count: meta_info.end_column_count,
        end_partition_count: meta_info.end_partition_count,
        end_row_count: meta_info.end_row_count,
        end_time: meta_info.end_time,
        error_message: meta_info.error_message.clone(),
        estimate_row: meta_info.estimate_row,
        file_size: meta_info.file_size,
        is_line_br_10_exist: meta_info.is_line_br_10_exist,
        is_line_br_13_exist: meta_info.is_line_br_13_exist,
        keyvalue_table: HashMap::new(),
        log_file_name: meta_info.log_file_name.clone(),
        partition_address: meta_info.partition_address.clone(),
        partition_count: meta_info.partition_count,
        partition_data: meta_info.partition_data.clone(),
        partition_size_mb: meta_info.partition_size_mb,
        processed_partition: meta_info.processed_partition,
        start_column_count: meta_info.start_column_count,
        start_partition_count: meta_info.start_partition_count,
        start_row_count: meta_info.start_row_count,
        start_time: meta_info.start_time,
        streaming_batch :meta_info.streaming_batch ,
        thread :meta_info.thread ,
        validate_row :meta_info.validate_row ,
        value_column_name :meta_info.value_column_name .clone(),
        vector_table :  Vec::new(),
        vector_table_group : vector_group,
    };   
    Ok(result_df)
}

pub fn rs_filter(vector_group: &HashMap<usize, Vec<u8>>, meta_info: &MetaInfo, query: &str) -> (HashMap<usize, Vec<u8>>, MetaInfo) {
   
    let mut query_validation = parse_setting::validate_filter_setting(query, meta_info.column_name.clone());
    query_validation.current_command = "Filter".to_string();
    let mut result_meta_info = update_start_meta_info(&vector_group, &meta_info, query, query_validation.current_command.clone());   
    
    
    let mut result_vector_group = HashMap::new();
    let mut result_meta_info_group: HashMap<usize, MetaInfo> = HashMap::new();

    if !query_validation.error_message.is_empty() {
        println!("{}", query_validation.error_message);
        result_meta_info.error_message = query_validation.error_message;
        append_log(result_meta_info.clone());
        return (result_vector_group, result_meta_info);

    } else {

        if meta_info.thread <= 1 {

            let (result_vector, result_meta_info) = query::current_filter(1, 0, &vector_group, &meta_info, &query_validation);
            result_vector_group.insert(0, result_vector);
            result_meta_info_group.insert(0, result_meta_info.clone());
            let result_meta_info = update_end_meta_info(&vector_group, &result_meta_info, &result_meta_info_group);
            return (result_vector_group, result_meta_info);

        } else {

            let return_vector_group: Mutex<HashMap<usize, Vec<u8>>> = Mutex::new(HashMap::<usize, Vec<u8>>::new());
            let return_meta_info_group = Mutex::new(HashMap::<usize, MetaInfo>::new());        
            let total_partition: usize = vector_group.len();    
                
            (0..total_partition).into_par_iter().for_each(|n| {               
                let (result_vector, result_meta_info) = query::current_filter(1, n as usize, &vector_group, &meta_info, &query_validation);          
                return_vector_group.lock().unwrap().insert(n, result_vector);
                return_meta_info_group.lock().unwrap().insert(n, result_meta_info);                    
            });
        
            let result_vector_group: HashMap<usize, Vec<u8>> = return_vector_group.into_inner().unwrap();
            let result_meta_info_group: HashMap<usize, MetaInfo> = return_meta_info_group.into_inner().unwrap();    
            result_meta_info = update_end_meta_info(&vector_group, &result_meta_info, &result_meta_info_group);
            return (result_vector_group, result_meta_info);
        }
	}
}

#[pyfunction]
pub fn filter_unmatch(df: &Dataframe, query: &str) -> PyResult<Dataframe> {
   
    let meta_info: MetaInfo = MetaInfo {
        alt_column_name: df.alt_column_name.clone(),
        column_name: df.column_name.clone(),
        column_name_end_address: df.column_name_end_address,
        command_name: df.command_name.clone(),
        command_setting: df.command_setting.clone(),
        delimiter: df.delimiter,
        duration_second: df.duration_second,
        end_column_count: df.end_column_count,
        end_partition_count: df.end_partition_count,
        end_row_count: df.end_row_count,
        end_time: df.end_time,
        error_message: df.error_message.clone(),
        estimate_row: df.estimate_row,
        file_size: df.file_size,
        is_line_br_10_exist: df.is_line_br_10_exist,
        is_line_br_13_exist: df.is_line_br_13_exist,       
        log_file_name: df.log_file_name.clone(),
        partition_address: df.partition_address.clone(),
        partition_count: df.partition_count,
        partition_data: df.partition_data.clone(),
        partition_size_mb: df.partition_size_mb,
        processed_partition: df.processed_partition,
        start_column_count: df.start_column_count,
        start_partition_count: df.start_partition_count,
        start_row_count: df.start_row_count,
        start_time: df.start_time,
        streaming_batch: df.streaming_batch ,
        thread: df.thread,
        validate_row: df.validate_row ,
        value_column_name: df.value_column_name .clone(),        
    };  

    let (vector_group, meta_info) = rs_filter_unmatch(&df.vector_table_group, &meta_info, query);
   
    let result_df = Dataframe {
        alt_column_name: meta_info.alt_column_name.clone(),
        column_name: meta_info.column_name.clone(),
        column_name_end_address: meta_info.column_name_end_address,
        command_name: meta_info.command_name.clone(),
        command_setting: meta_info.command_setting.clone(),
        delimiter: meta_info.delimiter,
        duration_second: meta_info.duration_second,
        end_column_count: meta_info.end_column_count,
        end_partition_count: meta_info.end_partition_count,
        end_row_count: meta_info.end_row_count,
        end_time: meta_info.end_time,
        error_message: meta_info.error_message.clone(),
        estimate_row: meta_info.estimate_row,
        file_size: meta_info.file_size,
        is_line_br_10_exist: meta_info.is_line_br_10_exist,
        is_line_br_13_exist: meta_info.is_line_br_13_exist,
        keyvalue_table: HashMap::new(),
        log_file_name: meta_info.log_file_name.clone(),
        partition_address: meta_info.partition_address.clone(),
        partition_count: meta_info.partition_count,
        partition_data: meta_info.partition_data.clone(),
        partition_size_mb: meta_info.partition_size_mb,
        processed_partition: meta_info.processed_partition,
        start_column_count: meta_info.start_column_count,
        start_partition_count: meta_info.start_partition_count,
        start_row_count: meta_info.start_row_count,
        start_time: meta_info.start_time,
        streaming_batch :meta_info.streaming_batch ,
        thread :meta_info.thread ,
        validate_row :meta_info.validate_row ,
        value_column_name :meta_info.value_column_name .clone(),
        vector_table :  Vec::new(),
        vector_table_group : vector_group,
    };   
    Ok(result_df)
}

pub fn rs_filter_unmatch(vector_group: &HashMap<usize, Vec<u8>>, meta_info: &MetaInfo, query: &str) -> (HashMap<usize, Vec<u8>>, MetaInfo) {
   
    let mut query_validation = parse_setting::validate_filter_setting(query, meta_info.column_name.clone());
    query_validation.current_command = "Filter_Unmatch".to_string();
    let mut result_meta_info = update_start_meta_info(&vector_group, &meta_info, query, query_validation.current_command.clone());   
    
    
    let mut result_vector_group = HashMap::new();
    let mut result_meta_info_group: HashMap<usize, MetaInfo> = HashMap::new();

    if !query_validation.error_message.is_empty() {
        println!("{}", query_validation.error_message);
        result_meta_info.error_message = query_validation.error_message;
        append_log(result_meta_info.clone());
        return (result_vector_group, result_meta_info);

    } else {

        if meta_info.thread <= 1 {

            let (result_vector, result_meta_info) = query::current_filter(1, 0, &vector_group, &meta_info, &query_validation);
            result_vector_group.insert(0, result_vector);
            result_meta_info_group.insert(0, result_meta_info.clone());
            let result_meta_info = update_end_meta_info(&vector_group, &result_meta_info, &result_meta_info_group);
            return (result_vector_group, result_meta_info);

        } else {

            let return_vector_group: Mutex<HashMap<usize, Vec<u8>>> = Mutex::new(HashMap::<usize, Vec<u8>>::new());
            let return_meta_info_group = Mutex::new(HashMap::<usize, MetaInfo>::new());        
            let total_partition: usize = vector_group.len();    
                
            (0..total_partition).into_par_iter().for_each(|n| {               
                let (result_vector, result_meta_info) = query::current_filter(1, n as usize, &vector_group, &meta_info, &query_validation);          
                return_vector_group.lock().unwrap().insert(n, result_vector);
                return_meta_info_group.lock().unwrap().insert(n, result_meta_info);                    
            });
        
            let result_vector_group: HashMap<usize, Vec<u8>> = return_vector_group.into_inner().unwrap();
            let result_meta_info_group: HashMap<usize, MetaInfo> = return_meta_info_group.into_inner().unwrap();    
            result_meta_info = update_end_meta_info(&vector_group, &result_meta_info, &result_meta_info_group);
            return (result_vector_group, result_meta_info);
        }
	}
}

#[pyfunction]
pub fn final_distinct(df_group: HashMap<usize, Dataframe>, query: &str) -> PyResult<Dataframe> {
       
    let mut vector_group: HashMap<usize, Vec<u8>> = HashMap::new();   

    for (k, v) in df_group.iter() {
        vector_group.insert(*k, v.vector_table.clone());
    }   

    let mut meta_info_group: HashMap<usize, MetaInfo> = HashMap::new();

    for i in 0..df_group.len() {
        meta_info_group.insert(i, MetaInfo {
            alt_column_name: df_group[&i].alt_column_name.clone(),
            column_name: df_group[&i].column_name.clone(),
            column_name_end_address: df_group[&i].column_name_end_address,
            command_name: df_group[&i].command_name.clone(),
            command_setting: df_group[&i].command_setting.clone(),
            delimiter: df_group[&i].delimiter,
            duration_second: df_group[&i].duration_second,
            end_column_count: df_group[&i].end_column_count,
            end_partition_count: df_group[&i].end_partition_count,
            end_row_count: df_group[&i].end_row_count,
            end_time: df_group[&i].end_time,
            error_message: df_group[&i].error_message.clone(),
            estimate_row: df_group[&i].estimate_row,
            file_size: df_group[&i].file_size,
            is_line_br_10_exist: df_group[&i].is_line_br_10_exist,
            is_line_br_13_exist: df_group[&i].is_line_br_13_exist,       
            log_file_name: df_group[&i].log_file_name.clone(),
            partition_address: df_group[&i].partition_address.clone(),
            partition_count: df_group[&i].partition_count,
            partition_data: df_group[&i].partition_data.clone(),
            partition_size_mb: df_group[&i].partition_size_mb,
            processed_partition: df_group[&i].processed_partition,
            start_column_count: df_group[&i].start_column_count,
            start_partition_count: df_group[&i].start_partition_count,
            start_row_count: df_group[&i].start_row_count,
            start_time: df_group[&i].start_time,
            streaming_batch: df_group[&i].streaming_batch ,
            thread: df_group[&i].thread,
            validate_row: df_group[&i].validate_row ,
            value_column_name: df_group[&i].value_column_name .clone(),        
        });
    }      

    let (vector, meta_info) = rs_final_distinct(&vector_group, &meta_info_group, &query);    
    
    let result_df = Dataframe {
        alt_column_name: meta_info.alt_column_name.clone(),
        column_name: meta_info.column_name.clone(),
        column_name_end_address: meta_info.column_name_end_address,
        command_name: meta_info.command_name.clone(),
        command_setting: meta_info.command_setting.clone(),
        delimiter: meta_info.delimiter,
        duration_second: meta_info.duration_second,
        end_column_count: meta_info.end_column_count,
        end_partition_count: meta_info.end_partition_count,
        end_row_count: meta_info.end_row_count,
        end_time: meta_info.end_time,
        error_message: meta_info.error_message.clone(),
        estimate_row: meta_info.estimate_row,
        file_size: meta_info.file_size,
        is_line_br_10_exist: meta_info.is_line_br_10_exist,
        is_line_br_13_exist: meta_info.is_line_br_13_exist,
        keyvalue_table: HashMap::new(),
        log_file_name: meta_info.log_file_name.clone(),
        partition_address: meta_info.partition_address.clone(),
        partition_count: meta_info.partition_count,
        partition_data: meta_info.partition_data.clone(),
        partition_size_mb: meta_info.partition_size_mb,
        processed_partition: meta_info.processed_partition,
        start_column_count: meta_info.start_column_count,
        start_partition_count: meta_info.start_partition_count,
        start_row_count: meta_info.start_row_count,
        start_time: meta_info.start_time,
        streaming_batch :meta_info.streaming_batch ,
        thread :meta_info.thread ,
        validate_row :meta_info.validate_row ,
        value_column_name :meta_info.value_column_name .clone(),
        vector_table :  vector,
        vector_table_group : HashMap::new(),
    };   
    Ok(result_df)
}

pub fn rs_final_distinct(vector_group: &HashMap<usize, Vec<u8>>, meta_info_group: &HashMap<usize, MetaInfo>, query: &str) -> (Vec<u8>, MetaInfo) {
   
    let mut query_validation = parse_setting::validate_column_setting(query, meta_info_group[&0].column_name.clone());
    query_validation.current_command = "Final_Distinct".to_string();
    let mut result_meta_info = update_start_meta_info(&vector_group, &meta_info_group[&0], query, query_validation.current_command.clone());       
    
    if !query_validation.error_message.is_empty() {
        println!("{}", query_validation.error_message);
        result_meta_info.error_message = query_validation.error_message;
        append_log(result_meta_info.clone());
        let vector: Vec<u8> = Vec::new();        
        return (vector, result_meta_info);

    } else {   

    
        let mut start_row_count = 0;

        for p in 0..vector_group.len() {
            start_row_count += meta_info_group.get(&(p as usize)).unwrap().end_row_count;
        }

        result_meta_info.start_row_count = start_row_count;
     
        let (_vector, mut _meta_info) =
            query::current_unique(vector_group.len() as usize, 0, &vector_group, &result_meta_info, &query_validation.clone());
    
        _meta_info.column_name = _meta_info.alt_column_name.clone();

        result_meta_info.end_partition_count = 1;
        result_meta_info.end_column_count = _meta_info.end_column_count;
        result_meta_info.end_row_count = _meta_info.end_row_count;
      //  result_meta_info.column_name = _meta_info.column_name.clone();
        result_meta_info.column_name = _meta_info.column_name;

        let end_time = Instant::now();
        result_meta_info.end_time = end_time;

        result_meta_info.duration_second =
        ((end_time - result_meta_info.start_time).as_secs_f64() * 1000.0).round() / 1000.0;
        append_log(result_meta_info.clone());

        (_vector, result_meta_info)
    }
}

#[pyfunction]
pub fn final_group_by(df_group: HashMap<usize, Dataframe>, query: &str) -> PyResult<Dataframe> {
       
    let mut vector_group: HashMap<usize, Vec<u8>> = HashMap::new();   

    for (k, v) in df_group.iter() {
        vector_group.insert(*k, v.vector_table.clone());
    }   

    let mut meta_info_group: HashMap<usize, MetaInfo> = HashMap::new();

    for i in 0..df_group.len() {
        meta_info_group.insert(i, MetaInfo {
            alt_column_name: df_group[&i].alt_column_name.clone(),
            column_name: df_group[&i].column_name.clone(),
            column_name_end_address: df_group[&i].column_name_end_address,
            command_name: df_group[&i].command_name.clone(),
            command_setting: df_group[&i].command_setting.clone(),
            delimiter: df_group[&i].delimiter,
            duration_second: df_group[&i].duration_second,
            end_column_count: df_group[&i].end_column_count,
            end_partition_count: df_group[&i].end_partition_count,
            end_row_count: df_group[&i].end_row_count,
            end_time: df_group[&i].end_time,
            error_message: df_group[&i].error_message.clone(),
            estimate_row: df_group[&i].estimate_row,
            file_size: df_group[&i].file_size,
            is_line_br_10_exist: df_group[&i].is_line_br_10_exist,
            is_line_br_13_exist: df_group[&i].is_line_br_13_exist,       
            log_file_name: df_group[&i].log_file_name.clone(),
            partition_address: df_group[&i].partition_address.clone(),
            partition_count: df_group[&i].partition_count,
            partition_data: df_group[&i].partition_data.clone(),
            partition_size_mb: df_group[&i].partition_size_mb,
            processed_partition: df_group[&i].processed_partition,
            start_column_count: df_group[&i].start_column_count,
            start_partition_count: df_group[&i].start_partition_count,
            start_row_count: df_group[&i].start_row_count,
            start_time: df_group[&i].start_time,
            streaming_batch: df_group[&i].streaming_batch ,
            thread: df_group[&i].thread,
            validate_row: df_group[&i].validate_row ,
            value_column_name: df_group[&i].value_column_name .clone(),        
        });
    }      

    let (vector, meta_info) = rs_final_group_by(&vector_group, &meta_info_group, &query);    
    
    let result_df = Dataframe {
        alt_column_name: meta_info.alt_column_name.clone(),
        column_name: meta_info.column_name.clone(),
        column_name_end_address: meta_info.column_name_end_address,
        command_name: meta_info.command_name.clone(),
        command_setting: meta_info.command_setting.clone(),
        delimiter: meta_info.delimiter,
        duration_second: meta_info.duration_second,
        end_column_count: meta_info.end_column_count,
        end_partition_count: meta_info.end_partition_count,
        end_row_count: meta_info.end_row_count,
        end_time: meta_info.end_time,
        error_message: meta_info.error_message.clone(),
        estimate_row: meta_info.estimate_row,
        file_size: meta_info.file_size,
        is_line_br_10_exist: meta_info.is_line_br_10_exist,
        is_line_br_13_exist: meta_info.is_line_br_13_exist,
        keyvalue_table: HashMap::new(),
        log_file_name: meta_info.log_file_name.clone(),
        partition_address: meta_info.partition_address.clone(),
        partition_count: meta_info.partition_count,
        partition_data: meta_info.partition_data.clone(),
        partition_size_mb: meta_info.partition_size_mb,
        processed_partition: meta_info.processed_partition,
        start_column_count: meta_info.start_column_count,
        start_partition_count: meta_info.start_partition_count,
        start_row_count: meta_info.start_row_count,
        start_time: meta_info.start_time,
        streaming_batch :meta_info.streaming_batch ,
        thread :meta_info.thread ,
        validate_row :meta_info.validate_row ,
        value_column_name :meta_info.value_column_name .clone(),
        vector_table :  vector,
        vector_table_group : HashMap::new(),
    };      
    Ok(result_df)
}

pub fn rs_final_group_by(vector_group: &HashMap<usize, Vec<u8>>, meta_info_group: &HashMap<usize, MetaInfo>, query: &str) -> (Vec<u8>, MetaInfo) {
   
    let mut query_validation = parse_setting::validate_group_by_setting(query, meta_info_group[&0].column_name.clone());
    query_validation.current_command = "Final_Group_By".to_string();
    let mut result_meta_info = update_start_meta_info(&vector_group, &meta_info_group[&0], query, query_validation.current_command.clone());       
    
    if !query_validation.error_message.is_empty() {
        println!("{}", query_validation.error_message);
        result_meta_info.error_message = query_validation.error_message;
        append_log(result_meta_info.clone());
        let vector: Vec<u8> = Vec::new();        
        return (vector, result_meta_info);

    } else {   

    
        let mut start_row_count = 0;

        for p in 0..vector_group.len() {
            start_row_count += meta_info_group.get(&(p as usize)).unwrap().end_row_count;
        }

        result_meta_info.start_row_count = start_row_count;
     
        let (_vector, mut _meta_info) =
            query::current_unique(vector_group.len() as usize, 0, &vector_group, &result_meta_info, &query_validation.clone());
    
        _meta_info.column_name = _meta_info.alt_column_name.clone();

        result_meta_info.end_partition_count = 1;
        result_meta_info.end_column_count = _meta_info.end_column_count;
        result_meta_info.end_row_count = _meta_info.end_row_count;
      //  result_meta_info.column_name = _meta_info.column_name.clone();
        result_meta_info.column_name = _meta_info.alt_column_name;

        let end_time = Instant::now();
        result_meta_info.end_time = end_time;

        result_meta_info.duration_second =
        ((end_time - result_meta_info.start_time).as_secs_f64() * 1000.0).round() / 1000.0;
        append_log(result_meta_info.clone());

        (_vector, result_meta_info)
    }
}

#[pyfunction]
pub fn group_by(df: &Dataframe, query: &str) -> PyResult<Dataframe> {  
    
    let meta_info: MetaInfo = MetaInfo {
        alt_column_name: df.alt_column_name.clone(),
        column_name: df.column_name.clone(),
        column_name_end_address: df.column_name_end_address,
        command_name: df.command_name.clone(),
        command_setting: df.command_setting.clone(),
        delimiter: df.delimiter,
        duration_second: df.duration_second,
        end_column_count: df.end_column_count,
        end_partition_count: df.end_partition_count,
        end_row_count: df.end_row_count,
        end_time: df.end_time,
        error_message: df.error_message.clone(),
        estimate_row: df.estimate_row,
        file_size: df.file_size,
        is_line_br_10_exist: df.is_line_br_10_exist,
        is_line_br_13_exist: df.is_line_br_13_exist,       
        log_file_name: df.log_file_name.clone(),
        partition_address: df.partition_address.clone(),
        partition_count: df.partition_count,
        partition_data: df.partition_data.clone(),
        partition_size_mb: df.partition_size_mb,
        processed_partition: df.processed_partition,
        start_column_count: df.start_column_count,
        start_partition_count: df.start_partition_count,
        start_row_count: df.start_row_count,
        start_time: df.start_time,
        streaming_batch: df.streaming_batch ,
        thread: df.thread,
        validate_row: df.validate_row ,
        value_column_name: df.value_column_name .clone(),        
    };  

    let (vector, meta_info) = rs_group_by(&df.vector_table_group, &meta_info, query);
   
    let result_df = Dataframe {
        alt_column_name: meta_info.alt_column_name.clone(),
        column_name: meta_info.column_name.clone(),
        column_name_end_address: meta_info.column_name_end_address,
        command_name: meta_info.command_name.clone(),
        command_setting: meta_info.command_setting.clone(),
        delimiter: meta_info.delimiter,
        duration_second: meta_info.duration_second,
        end_column_count: meta_info.end_column_count,
        end_partition_count: meta_info.end_partition_count,
        end_row_count: meta_info.end_row_count,
        end_time: meta_info.end_time,
        error_message: meta_info.error_message.clone(),
        estimate_row: meta_info.estimate_row,
        file_size: meta_info.file_size,
        is_line_br_10_exist: meta_info.is_line_br_10_exist,
        is_line_br_13_exist: meta_info.is_line_br_13_exist,
        keyvalue_table: HashMap::new(),
        log_file_name: meta_info.log_file_name.clone(),
        partition_address: meta_info.partition_address.clone(),
        partition_count: meta_info.partition_count,
        partition_data: meta_info.partition_data.clone(),
        partition_size_mb: meta_info.partition_size_mb,
        processed_partition: meta_info.processed_partition,
        start_column_count: meta_info.start_column_count,
        start_partition_count: meta_info.start_partition_count,
        start_row_count: meta_info.start_row_count,
        start_time: meta_info.start_time,
        streaming_batch :meta_info.streaming_batch ,
        thread :meta_info.thread ,
        validate_row :meta_info.validate_row ,
        value_column_name :meta_info.value_column_name .clone(),
        vector_table :  vector,
        vector_table_group : HashMap::new(),
    };   
    Ok(result_df)
}

pub fn rs_group_by(vector_group: &HashMap<usize, Vec<u8>>, meta_info: &MetaInfo, query: &str) -> (Vec<u8>, MetaInfo) {
    
    let mut query_validation = parse_setting::validate_group_by_setting(query, meta_info.column_name.clone());
    query_validation.current_command = "Group_By".to_string();
    let mut result_meta_info = update_start_meta_info(&vector_group, &meta_info, query, query_validation.current_command.clone());       
    
    if !query_validation.error_message.is_empty() {
        println!("{}", query_validation.error_message);
        result_meta_info.error_message = query_validation.error_message;
        append_log(result_meta_info.clone());
        let vector: Vec<u8> = Vec::new();
        return (vector, result_meta_info);

    } else {

        if meta_info.thread <= 1 {

          let (result_vector, meta_info) = query::current_unique(1, 0, &vector_group, &meta_info, &query_validation);
          
           result_meta_info.end_partition_count = 1;
           result_meta_info.end_column_count = meta_info.end_column_count;
           result_meta_info.end_row_count = meta_info.end_row_count;           

           let end_time = Instant::now();

           result_meta_info.end_time = end_time;

           
           result_meta_info.duration_second =
           ((end_time - result_meta_info.start_time).as_secs_f64() * 1000.0).round() / 1000.0;
           append_log(result_meta_info.clone());
            return (result_vector, result_meta_info);

        } else {

            let return_vector_group: Mutex<HashMap<usize, Vec<u8>>> = Mutex::new(HashMap::<usize, Vec<u8>>::new());
            let return_meta_info_group: Mutex<HashMap<usize, MetaInfo>> = Mutex::new(HashMap::<usize, MetaInfo>::new());        
            let total_partition: usize = vector_group.len();             
                
            (0..total_partition).into_par_iter().for_each(|n| {                        
                let (result_vector, result_meta_info) = query::current_unique(1, n as usize, &vector_group, &meta_info, &query_validation);          
                return_vector_group.lock().unwrap().insert(n, result_vector);
                return_meta_info_group.lock().unwrap().insert(n, result_meta_info);                    
            });
        
            let multithread_vector_group: HashMap<usize, Vec<u8>> = return_vector_group.into_inner().unwrap();
            let multithread_meta_info_group: HashMap<usize, MetaInfo> = return_meta_info_group.into_inner().unwrap();    

            let total_partition = multithread_vector_group.len();
            let mut result_vector_group: HashMap<usize, Vec<u8>> = HashMap::new();
            let mut result_meta_info_group: HashMap<usize, MetaInfo>= HashMap::new();
           
            let revised_query: String = replace_count_by_sum_count(query);           
           
            let query_validation = parse_setting::validate_group_by_setting(revised_query.as_str(), multithread_meta_info_group[&0].column_name.clone());           
            
            let (vector, meta_info) = query::current_unique(total_partition, 0, &multithread_vector_group, &multithread_meta_info_group[&0], &query_validation);            
        
            result_meta_info_group.insert(0, meta_info);
            result_vector_group.insert(0, vector.clone());
          
            result_meta_info.end_partition_count = 1;
            result_meta_info.end_column_count = result_meta_info_group[&0].end_column_count;
            result_meta_info.end_row_count = result_meta_info_group[&0].end_row_count;
            result_meta_info.column_name = result_meta_info_group[&0].column_name.clone();
			result_meta_info.alt_column_name = result_meta_info_group[&0].alt_column_name.clone();            

            let end_time = Instant::now();
            result_meta_info.end_time = end_time;
            
            result_meta_info.duration_second =
            ((end_time - result_meta_info.start_time).as_secs_f64() * 1000.0).round() / 1000.0;
            append_log(result_meta_info.clone());
            
            return (vector, result_meta_info);
        }
	}
}

#[pyfunction]
pub fn join_key_value(df: &Dataframe, master_df: &Dataframe, query: &str) -> PyResult<Dataframe> {    

    let meta_info: MetaInfo = MetaInfo {
        alt_column_name: df.alt_column_name.clone(),
        column_name: df.column_name.clone(),
        column_name_end_address: df.column_name_end_address,
        command_name: df.command_name.clone(),
        command_setting: df.command_setting.clone(),
        delimiter: df.delimiter,
        duration_second: df.duration_second,
        end_column_count: df.end_column_count,
        end_partition_count: df.end_partition_count,
        end_row_count: df.end_row_count,
        end_time: df.end_time,
        error_message: df.error_message.clone(),
        estimate_row: df.estimate_row,
        file_size: df.file_size,
        is_line_br_10_exist: df.is_line_br_10_exist,
        is_line_br_13_exist: df.is_line_br_13_exist,       
        log_file_name: df.log_file_name.clone(),
        partition_address: df.partition_address.clone(),
        partition_count: df.partition_count,
        partition_data: df.partition_data.clone(),
        partition_size_mb: df.partition_size_mb,
        processed_partition: df.processed_partition,
        start_column_count: df.start_column_count,
        start_partition_count: df.start_partition_count,
        start_row_count: df.start_row_count,
        start_time: df.start_time,
        streaming_batch: df.streaming_batch ,
        thread: df.thread,
        validate_row: df.validate_row ,
        value_column_name: master_df.value_column_name.clone(),        
    };  

    let (vector_group, meta_info) = rs_join_key_value(&df.vector_table_group, &meta_info, &master_df.keyvalue_table, query);
  
   
    let result_df = Dataframe {
        alt_column_name: meta_info.alt_column_name.clone(),
        column_name: meta_info.column_name.clone(),
        column_name_end_address: meta_info.column_name_end_address,
        command_name: meta_info.command_name.clone(),
        command_setting: meta_info.command_setting.clone(),
        delimiter: meta_info.delimiter,
        duration_second: meta_info.duration_second,
        end_column_count: meta_info.end_column_count,
        end_partition_count: meta_info.end_partition_count,
        end_row_count: meta_info.end_row_count,
        end_time: meta_info.end_time,
        error_message: meta_info.error_message.clone(),
        estimate_row: meta_info.estimate_row,
        file_size: meta_info.file_size,
        is_line_br_10_exist: meta_info.is_line_br_10_exist,
        is_line_br_13_exist: meta_info.is_line_br_13_exist,
        keyvalue_table: HashMap::new(),
        log_file_name: meta_info.log_file_name.clone(),
        partition_address: meta_info.partition_address.clone(),
        partition_count: meta_info.partition_count,
        partition_data: meta_info.partition_data.clone(),
        partition_size_mb: meta_info.partition_size_mb,
        processed_partition: meta_info.processed_partition,
        start_column_count: meta_info.start_column_count,
        start_partition_count: meta_info.start_partition_count,
        start_row_count: meta_info.start_row_count,
        start_time: meta_info.start_time,
        streaming_batch :meta_info.streaming_batch ,
        thread :meta_info.thread ,
        validate_row :meta_info.validate_row ,
        value_column_name :meta_info.value_column_name .clone(),
        vector_table :  Vec::new(),
        vector_table_group : vector_group,
    };   
    Ok(result_df)
}

pub fn rs_join_key_value(vector_group: &HashMap<usize, Vec<u8>>, meta_info: &MetaInfo, keyvalue_table: &HashMap<String, Vec<u8>>, query: &str) -> (HashMap<usize, Vec<u8>>, MetaInfo) {
    
    let mut query_validation = parse_setting::validate_add_column_setting("Join_Key_Value", query, meta_info.column_name.clone());
    query_validation.current_command = "Join_Key_Value".to_string();
    let mut result_meta_info = update_start_meta_info(&vector_group, &meta_info, query, query_validation.current_command.clone());   
    
    
    let mut result_vector_group = HashMap::new();
    let mut result_meta_info_group: HashMap<usize, MetaInfo> = HashMap::new();

    if !query_validation.error_message.is_empty() {
        println!("{}", query_validation.error_message);
        result_meta_info.error_message = query_validation.error_message;
        append_log(result_meta_info.clone());
        return (result_vector_group, result_meta_info);

    } else {

        if meta_info.thread <= 1 {

            let (result_vector, result_meta_info) = query::current_join_key_value(1, 0, &vector_group, &meta_info, &keyvalue_table, &query_validation);
            result_vector_group.insert(0, result_vector);
            result_meta_info_group.insert(0, result_meta_info.clone());
            let result_meta_info = update_end_meta_info(&vector_group, &result_meta_info, &result_meta_info_group);
            return (result_vector_group, result_meta_info);

        } else {

            let return_vector_group: Mutex<HashMap<usize, Vec<u8>>> = Mutex::new(HashMap::<usize, Vec<u8>>::new());
            let return_meta_info_group = Mutex::new(HashMap::<usize, MetaInfo>::new());        
            let total_partition: usize = vector_group.len();    
                
            (0..total_partition).into_par_iter().for_each(|n| {               
                let (result_vector, result_meta_info) = query::current_join_key_value(1, n as usize, &vector_group, &meta_info, &keyvalue_table, &query_validation);          
                return_vector_group.lock().unwrap().insert(n, result_vector);
                return_meta_info_group.lock().unwrap().insert(n, result_meta_info);                    
            });
        
            let result_vector_group: HashMap<usize, Vec<u8>> = return_vector_group.into_inner().unwrap();
            let result_meta_info_group: HashMap<usize, MetaInfo> = return_meta_info_group.into_inner().unwrap();    
            result_meta_info = update_end_meta_info(&vector_group, &result_meta_info, &result_meta_info_group);
            return (result_vector_group, result_meta_info);
        }
	}
}

#[pyfunction]
pub fn select_column(df: &Dataframe, query: &str) -> PyResult<Dataframe> {
  
    let meta_info: MetaInfo = MetaInfo {
        alt_column_name: df.alt_column_name.clone(),
        column_name: df.column_name.clone(),
        column_name_end_address: df.column_name_end_address,
        command_name: df.command_name.clone(),
        command_setting: df.command_setting.clone(),
        delimiter: df.delimiter,
        duration_second: df.duration_second,
        end_column_count: df.end_column_count,
        end_partition_count: df.end_partition_count,
        end_row_count: df.end_row_count,
        end_time: df.end_time,
        error_message: df.error_message.clone(),
        estimate_row: df.estimate_row,
        file_size: df.file_size,
        is_line_br_10_exist: df.is_line_br_10_exist,
        is_line_br_13_exist: df.is_line_br_13_exist,       
        log_file_name: df.log_file_name.clone(),
        partition_address: df.partition_address.clone(),
        partition_count: df.partition_count,
        partition_data: df.partition_data.clone(),
        partition_size_mb: df.partition_size_mb,
        processed_partition: df.processed_partition,
        start_column_count: df.start_column_count,
        start_partition_count: df.start_partition_count,
        start_row_count: df.start_row_count,
        start_time: df.start_time,
        streaming_batch: df.streaming_batch ,
        thread: df.thread,
        validate_row: df.validate_row ,
        value_column_name: df.value_column_name .clone(),        
    };  

    let (vector_group, meta_info) = rs_select_column(&df.vector_table_group, &meta_info, query);
   
    let result_df = Dataframe {
        alt_column_name: meta_info.alt_column_name.clone(),
        column_name: meta_info.column_name.clone(),
        column_name_end_address: meta_info.column_name_end_address,
        command_name: meta_info.command_name.clone(),
        command_setting: meta_info.command_setting.clone(),
        delimiter: meta_info.delimiter,
        duration_second: meta_info.duration_second,
        end_column_count: meta_info.end_column_count,
        end_partition_count: meta_info.end_partition_count,
        end_row_count: meta_info.end_row_count,
        end_time: meta_info.end_time,
        error_message: meta_info.error_message.clone(),
        estimate_row: meta_info.estimate_row,
        file_size: meta_info.file_size,
        is_line_br_10_exist: meta_info.is_line_br_10_exist,
        is_line_br_13_exist: meta_info.is_line_br_13_exist,
        keyvalue_table: HashMap::new(),
        log_file_name: meta_info.log_file_name.clone(),
        partition_address: meta_info.partition_address.clone(),
        partition_count: meta_info.partition_count,
        partition_data: meta_info.partition_data.clone(),
        partition_size_mb: meta_info.partition_size_mb,
        processed_partition: meta_info.processed_partition,
        start_column_count: meta_info.start_column_count,
        start_partition_count: meta_info.start_partition_count,
        start_row_count: meta_info.start_row_count,
        start_time: meta_info.start_time,
        streaming_batch :meta_info.streaming_batch ,
        thread :meta_info.thread ,
        validate_row :meta_info.validate_row ,
        value_column_name :meta_info.value_column_name .clone(),
        vector_table :  Vec::new(),
        vector_table_group : vector_group,
    };   
    Ok(result_df)
}

pub fn rs_select_column(vector_group: &HashMap<usize, Vec<u8>>, meta_info: &MetaInfo, query: &str) -> (HashMap<usize, Vec<u8>>, MetaInfo) {
   
    let mut query_validation = parse_setting::validate_column_setting(query, meta_info.column_name.clone());
    query_validation.current_command = "Select_Column".to_string();
    let mut result_meta_info = update_start_meta_info(&vector_group, &meta_info, query, query_validation.current_command.clone());   
    
    
    let mut result_vector_group = HashMap::new();
    let mut result_meta_info_group: HashMap<usize, MetaInfo> = HashMap::new();

    if !query_validation.error_message.is_empty() {
        println!("{}", query_validation.error_message);
        result_meta_info.error_message = query_validation.error_message;
        append_log(result_meta_info.clone());
        return (result_vector_group, result_meta_info);

    } else {

        if meta_info.thread <= 1 {

            let (result_vector, result_meta_info) = query::current_select_column(1, 0, &vector_group, &meta_info, &query_validation);
            result_vector_group.insert(0, result_vector);
            result_meta_info_group.insert(0, result_meta_info.clone());
            let result_meta_info = update_end_meta_info(&vector_group, &result_meta_info, &result_meta_info_group);
            return (result_vector_group, result_meta_info);

        } else {

            let return_vector_group: Mutex<HashMap<usize, Vec<u8>>> = Mutex::new(HashMap::<usize, Vec<u8>>::new());
            let return_meta_info_group: Mutex<HashMap<usize, MetaInfo>> = Mutex::new(HashMap::<usize, MetaInfo>::new());        
            let total_partition: usize = vector_group.len();    
                
            (0..total_partition).into_par_iter().for_each(|n| {               
                let (result_vector, result_meta_info) = query::current_select_column(1, n as usize, &vector_group, &meta_info, &query_validation);          
                return_vector_group.lock().unwrap().insert(n, result_vector);
                return_meta_info_group.lock().unwrap().insert(n, result_meta_info);                    
            });
        
            let result_vector_group: HashMap<usize, Vec<u8>> = return_vector_group.into_inner().unwrap();
            let result_meta_info_group: HashMap<usize, MetaInfo> = return_meta_info_group.into_inner().unwrap();    
            result_meta_info = update_end_meta_info(&vector_group, &result_meta_info, &result_meta_info_group);
            return (result_vector_group, result_meta_info);
        }
	}
}

// *** Utility ***

#[pyfunction]
pub fn create_log(df: &Dataframe) {

    if !std::path::Path::new(&df.log_file_name).exists() {
        let mut column_name = Vec::new();

        column_name.push("Batch".to_string());
        column_name.push("Command".to_string());
        column_name.push("Start Time".to_string());
        column_name.push("Second".to_string());
        column_name.push("Command Setting".to_string());
        column_name.push("S Partition".to_string());
        column_name.push("E Partition".to_string());
        column_name.push("S Column".to_string());
        column_name.push("E Column".to_string());
        column_name.push("S Row".to_string());
        column_name.push("E Row".to_string());
        column_name.push("Error Message".to_string());

        let mut csv_string = String::new();

        csv_string.push_str(&column_name[0]);

        for name in &column_name[1..] {
            csv_string.push(',');
            csv_string.push_str(name);
        }

        csv_string.push_str("\r\n");      

        fs::write(&df.log_file_name, csv_string).expect("Unable to write file");
    }
}

#[pyfunction]
pub fn append_log(meta_info: MetaInfo) {

    if let Err(err) = fs::metadata(&meta_info.log_file_name) {
        if err.kind() == std::io::ErrorKind::NotFound {
            // handle file not found
        }
    } else {
        let mut csv_string = String::new();
        csv_string.push_str(&format!("{},", meta_info.streaming_batch));
        csv_string.push_str(&format!("{},", meta_info.command_name));
        csv_string.push_str(&format!("{},", format_time(meta_info.start_time)));
        //csv_string.push_str(&format!("{},", meta_info.end_time.format("15:04:05")));
        csv_string.push_str(&format!("{},", num_format(meta_info.duration_second as f64)));
        csv_string.push_str(&format!("\"{}\",", meta_info.command_setting));
        csv_string.push_str(&format!("{},", meta_info.start_partition_count));
        csv_string.push_str(&format!("{},", meta_info.end_partition_count));
        csv_string.push_str(&format!("{},", meta_info.start_column_count));
        csv_string.push_str(&format!("{},", meta_info.end_column_count));
        csv_string.push_str(&format!("{},", meta_info.start_row_count));
        csv_string.push_str(&format!("{},", meta_info.end_row_count));
        csv_string.push_str(&format!("\"{}\"", meta_info.error_message));
        csv_string.push('\n');

        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(meta_info.log_file_name)
            .unwrap();
        if let Err(_err) = file.write_all(csv_string.as_bytes()) {
            println!("*** Fail to write file Processing_log.csv ***");
        }
    }
}

fn update_start_meta_info(vector_group: &HashMap<usize, Vec<u8>>, meta_info: &MetaInfo, query: &str,  command: String) -> MetaInfo {
    
    let partition_count = &vector_group.len();
    let mut result_meta_info = meta_info.clone();
    result_meta_info.command_name = command;
    result_meta_info.command_setting = query.to_string();
    result_meta_info.start_column_count = meta_info.column_name.len() as i32;
    result_meta_info.start_partition_count = partition_count.clone() as i32;
    result_meta_info.start_row_count = meta_info.end_row_count;
    result_meta_info.start_time = Instant::now();

    result_meta_info
}

fn update_end_meta_info(result_vector_group: &HashMap<usize, Vec<u8>>, meta_info: &MetaInfo, result_meta_info_group: &HashMap<usize, MetaInfo>) -> MetaInfo {

    let partition_count = &result_vector_group.len();

    let mut result_meta_info = meta_info.clone();

    if result_meta_info.thread <= 1 {
        result_meta_info.end_partition_count = partition_count.clone() as i32;
        result_meta_info.end_column_count = result_meta_info_group[&0].end_column_count;
        result_meta_info.start_row_count = result_meta_info_group[&0].start_row_count;
        result_meta_info.end_row_count = result_meta_info_group[&0].end_row_count;
        result_meta_info.end_time = Instant::now();
        result_meta_info.column_name = result_meta_info_group[&0].column_name.clone();

    } else {

        let mut start_row_count = 0;
        let mut end_row_count = 0;

        for current_partition in 0..result_meta_info_group.len() as usize {
            start_row_count += result_meta_info_group[&current_partition].start_row_count;
            end_row_count += result_meta_info_group[&current_partition].end_row_count;
        }

        result_meta_info.end_partition_count = partition_count.clone() as i32;
        result_meta_info.end_column_count = result_meta_info_group[&0].end_column_count;
        result_meta_info.start_row_count = start_row_count;
        result_meta_info.end_row_count = end_row_count;
        result_meta_info.end_time = Instant::now();
        result_meta_info.column_name = result_meta_info_group[&0].column_name.clone();
        result_meta_info.alt_column_name = result_meta_info_group[&0].alt_column_name.clone();       
    }

    let end_time = Instant::now();
    result_meta_info.duration_second =
    ((end_time - result_meta_info.start_time).as_secs_f64() * 1000.0).round() / 1000.0;
    append_log(result_meta_info.clone());
    
    return result_meta_info;
    
}

pub fn format_time(start_time: Instant) -> String {
    let now = SystemTime::now();
    let duration = now.duration_since(SystemTime::UNIX_EPOCH).expect("Time went backwards");
    let start_duration = start_time.elapsed();
    let total_duration = duration - start_duration;
    let seconds = total_duration.as_secs();
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let seconds = seconds % 60;
    format!("{:02}-{:02}-{:02}", hours, minutes, seconds)
}

pub fn num_format(num: f64) -> String {
    let num_str = num.to_string();
    let parts: Vec<&str> = num_str.split('.').collect();
    let num_of_digits = parts[0].len();
    let num_of_commas = (num_of_digits - 1) / 3;
    let mut parts_0 = String::from(parts[0]);

    for i in 1..=num_of_commas {
        let comma_index = num_of_digits - i * 3;
        parts_0.insert(comma_index, ',');
    }

    if parts.len() > 1 {
        return format!("{}.{}", parts_0, parts[1]);
    } else {
        return parts_0;
    }
}

pub fn replace_count_by_sum_count(query: &str) -> String {
    let re = Regex::new(r"(?i)count\(\)").unwrap();
    let result = re.replace_all(query, |caps: &regex::Captures| {
        format!("Sum({})", &caps[0][..caps[0].len() - 2])
    });
    result.to_string()
}

#[pymodule]
fn peakrs(_py: Python, m: &PyModule) -> PyResult<()> {   

    m.add_function(wrap_pyfunction!(add_column, m)?)?;
    m.add_function(wrap_pyfunction!(append_csv, m)?)?;
    m.add_function(wrap_pyfunction!(append_log, m)?)?;
    m.add_function(wrap_pyfunction!(build_key_value, m)?)?;
    m.add_function(wrap_pyfunction!(create_folder_lake, m)?)?;
    m.add_function(wrap_pyfunction!(create_log, m)?)?;
    m.add_function(wrap_pyfunction!(distinct, m)?)?;
    m.add_function(wrap_pyfunction!(filter, m)?)?;
    m.add_function(wrap_pyfunction!(filter_unmatch, m)?)?;
    m.add_function(wrap_pyfunction!(final_distinct, m)?)?;
    m.add_function(wrap_pyfunction!(final_group_by, m)?)?;
    m.add_function(wrap_pyfunction!(get_csv_partition_address, m)?)?;
    m.add_function(wrap_pyfunction!(get_default_partition_address, m)?)?;    
    m.add_function(wrap_pyfunction!(get_csv_sample, m)?)?;
    m.add_function(wrap_pyfunction!(get_file_partition, m)?)?;
    m.add_function(wrap_pyfunction!(group_by, m)?)?;
    m.add_function(wrap_pyfunction!(join_key_value, m)?)?;
    m.add_function(wrap_pyfunction!(read_csv, m)?)?;
    m.add_function(wrap_pyfunction!(select_column, m)?)?;
    m.add_function(wrap_pyfunction!(view_csv, m)?)?;
    m.add_function(wrap_pyfunction!(view_sample, m)?)?;
    m.add_function(wrap_pyfunction!(write_csv, m)?)?; 
    m.add_class::<Dataframe>().unwrap();    
    m.add_class::<MetaInfo>().unwrap();    
    Ok(())
}
