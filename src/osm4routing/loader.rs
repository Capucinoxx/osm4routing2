extern crate serde;
extern crate serde_json;
extern crate csv;
use std::io::BufReader;
use std::collections::HashMap;
use serde::Deserialize;
use rmp_serde::from_read;
use std::fs::File;
use serde_json::Value; 
use std::collections::HashSet;
use std::error::Error;
use std::path::Path;
use serde_json::{Map};
use csv::{WriterBuilder, StringRecord};
use std::time::{Duration, Instant};
fn write_lists_to_csv(
    id_list: &Vec<i64>,
    node_list: &Vec<Vec<i64>>,
    tag_list: &Vec<Map<String, Value>>,
    output_file_path: &str,
) -> Result<Vec<StringRecord>, Box<dyn Error>> {
    let file = File::create(output_file_path)?;
    let mut writer = WriterBuilder::new().delimiter(b',').from_writer(file);
    writer.write_record(&["id", "nodes", "tags"])?;

    let mut records: Vec<StringRecord> = Vec::new();

    for (id, (nodes, tags)) in id_list.iter().zip(node_list.iter().zip(tag_list.iter())) {
        let nodes_str = nodes
            .iter()
            .map(|n| n.to_string())
            .collect::<Vec<String>>()
            .join(", ");
        let record = StringRecord::from(vec![id.to_string(), nodes_str, serde_json::to_string(tags).unwrap_or_default()]);
        writer.write_record(&record)?;
        records.push(record);
    }

    writer.flush()?;

    println!("CSV file has been created: {}", output_file_path);

    Ok(records)
}

fn write_lists_to_csv_nodes(
    id_list: &Vec<i64>,
    lat_list: &Vec<f64>,
    lon_list: &Vec<f64>,
    output_file_path: &str,
) -> Result<Vec<StringRecord>, Box<dyn Error>> {
    let file = File::create(output_file_path)?;
    let mut writer = WriterBuilder::new().delimiter(b',').from_writer(file);
    writer.write_record(&["ID", "Nodes", "Tags"])?;

    let mut records: Vec<StringRecord> = Vec::new();

    for (id, (nodes, tags)) in id_list.iter().zip(lat_list.iter().zip(lon_list.iter())) {
        let record = StringRecord::from(vec![id.to_string(),nodes.to_string(),tags.to_string() ]);
        writer.write_record(&record)?;
        records.push(record);
    }

    writer.flush()?;

    println!("CSV file has been created: {}", output_file_path);

    Ok(records)
}

// Define a function to filter based on the subset of IDs
fn filter_lists_by_subset(
    id_list: &Vec<i64>,
    node_list: &Vec<Vec<i64>>,
    tag_list: &Vec<Map<String, Value>>, // Use Map instead of HashMap
    subset_of_ids: &HashSet<i64>,
) -> (Vec<i64>, Vec<Vec<i64>>, Vec<Map<String, Value>>) {
    // Create empty lists to collect the filtered values
    let mut filtered_id_list: Vec<i64> = Vec::new();
    let mut filtered_node_list: Vec<Vec<i64>> = Vec::new();
    let mut filtered_tag_list: Vec<Map<String, Value>> = Vec::new(); // Use Map instead of HashMap

    // Iterate through the id_list and filter based on the subset_of_ids
    for (index, id) in id_list.iter().enumerate() {
        if subset_of_ids.contains(id) {
            // Add the values to the filtered lists
            filtered_id_list.push(*id);
            filtered_node_list.push(node_list[index].clone());
            filtered_tag_list.push(tag_list[index].clone()); // Use clone to copy Map
        }
    }

    // Return the filtered lists
    (filtered_id_list, filtered_node_list, filtered_tag_list)
}


// Define a function to filter based on the subset of IDs
fn filter_lists_by_subset_nodes(
    id_list: &Vec<i64>,
    lat_list: &Vec<i64>,
    lon_list:&Vec<i64>, // Use Map instead of HashMap
    subset_of_ids: &HashSet<i64>,
) -> (Vec<i64>,Vec<i64>,Vec<i64>) {
    // Create empty lists to collect the filtered values
    let mut filtered_id_list: Vec<i64> = Vec::new();
    let mut filtered_node_list: Vec<i64> = Vec::new();
    let mut filtered_tag_list: Vec<i64> = Vec::new(); // Use Map instead of HashMap

    // Iterate through the id_list and filter based on the subset_of_ids
    for (index, id) in id_list.iter().enumerate() {
        if subset_of_ids.contains(id) {
            // Add the values to the filtered lists
            filtered_id_list.push(*id);
            filtered_node_list.push(lat_list[index]);
            filtered_tag_list.push(lon_list[index]);
        }
    }

    // Return the filtered lists
    (filtered_id_list, filtered_node_list, filtered_tag_list)
}

fn extract_values_ways(data: &serde_json::Map<std::string::String, Value>, id_list:& mut Vec<i64>, nodes_list:&mut Vec<Vec<i64>>,tags_list:& mut Vec<Map<String,Value>>)
 -> Result<(), Box<dyn Error>> {
    // Extract "id"
    if let Some(id_value) = data.get("id") {
        if let Some(id_list_values) = id_value.as_array() {
            id_list.extend(id_list_values.iter().filter_map(|v| v.as_i64()));
        }
    }

    // Extract "nodes"
    if let Some(nodes_value) = data.get("node_list") {
        if let Some(nodes_list_values) = nodes_value.as_array() {
            for node_values in nodes_list_values.iter() {
                if let Some(nodes) = node_values.as_array() {
                    let nodes_values: Vec<i64> = nodes.iter().filter_map(|v| v.as_i64()).collect();
                    nodes_list.push(nodes_values);
                }
            }
        }
    }

    // Extract "tags"
    if let Some(tags_value) = data.get("tags") {
        if let Some(tags_list_values) = tags_value.as_array() {
            let tags_values: Vec<Map<String, Value>> = tags_list_values
                .iter()
                .filter_map(|v| v.as_object())
                .map(|obj| obj.clone())
                .collect();
            tags_list.extend(tags_values);
        }
    }
    Ok(())
}
fn convert_lat_lon_value(input: Vec<i64>) -> Vec<f64> {
    input.iter().map(|&x| x as f64 / 10000000.0).collect()
}


fn extract_values_nodes(data: &serde_json::Map<std::string::String, Value>, id_list:& mut Vec<i64>,lat_list:& mut Vec<i64>,lon_list:& mut Vec<i64>)
 -> Result<(), Box<dyn Error>> {
    // Extract "node_id"
    if let Some(id_value) = data.get("node_id") {
        if let Some(id_list_values) = id_value.as_array() {
            id_list.extend(id_list_values.iter().filter_map(|v| v.as_i64()));
        }
    }

    // Extract "lat"
    if let Some(lat_value) = data.get("lat") {
        if let Some(lat_values) = lat_value.as_array() {
            lat_list.extend(lat_values.iter().filter_map(|v| v.as_i64()));
        }
    }

    // Extract "lon"
    if let Some(lon_value) = data.get("lon") {
        if let Some(lon_values) = lon_value.as_array() {
            lon_list.extend(lon_values.iter().filter_map(|v| v.as_i64()));
        }
    }
    Ok(())
}

pub fn merge_csv_ways(
    filenames: Vec<&str>,
    merged_filename: &str,
    geohash_ptr: Vec<&str>,
    ways_to_load: HashSet<i64>,
    adjacent_geohashes: Vec<&str>
) -> Result<(Vec<StringRecord>, Vec<i64>), Box<dyn Error>> {
    let mut merged_data = Vec::new(); 
    
    // These lists will contain data for all ways passing through 
    let mut id_list: Vec<i64> = Vec::new();
    let mut nodes_list: Vec<Vec<i64>> = Vec::new();
    let mut tags_list: Vec<serde_json::Map<String, Value>> = Vec::new();
    // These lists are for ways that do not start in the 9 adjacent geohashes, but pass through them
    let mut id_list_ptr: Vec<i64> = Vec::new();
    let mut nodes_list_ptr: Vec<Vec<i64>> = Vec::new();
    let mut tags_list_ptr: Vec<serde_json::Map<String, Value>> = Vec::new();
    for filename in filenames {
        let mut start = Instant::now();
        // Open the MessagePack file
        let file = File::open(filename).expect("Failed to open file");
        let buffer_size = 8192*64;

        // Create a BufReader with the specified buffer size
        let mut reader = BufReader::with_capacity(buffer_size,file);
        // Deserialize the MessagePack data into a serde::Value
        match from_read::<_, Value>(reader) {
            Ok(value) => {
     // Iterate through the geohash objects
     if let Value::Object(top_level_map) = value {
        for (key, sub_map) in top_level_map.iter() {

            //Handle the case where we are in the original 9 geohashes
            if adjacent_geohashes.iter().any(|&s| s == key.as_str()){
                if let Value::Object(sub_map) = sub_map {
                    _=extract_values_ways(sub_map,&mut id_list,& mut nodes_list,&mut tags_list)
                }
                continue;
            }
            //Handle the case where we are not in one of the 9 original geohashes
            if geohash_ptr.iter().any(|&s| s == key.as_str()){
                if let Value::Object(sub_map) = sub_map {
                    _=extract_values_ways(sub_map,&mut id_list_ptr,& mut nodes_list_ptr,&mut tags_list_ptr)
                }
            }
        }
    }
        } 
            Err(e) => {
                eprintln!("Error deserializing MessagePack data: {}", e);
            }
        }
        let mut duration = start.elapsed();
        println!("Sngle file parse time {:?}", duration);
    }

    let mut filtered_id_list_ptr: Vec<i64> = Vec::new();
    let mut filtered_nodes_list_ptr: Vec<Vec<i64>> = Vec::new();
    let mut filtered_tags_list_ptr: Vec<serde_json::Map<String, Value>> = Vec::new();
    //keep only ways that touch our original 9 geohashes in the filtered_list 
    (filtered_id_list_ptr, filtered_nodes_list_ptr, filtered_tags_list_ptr) = filter_lists_by_subset(&id_list_ptr,&nodes_list_ptr,&tags_list_ptr,&ways_to_load);
    //let ddd = create_record_list(&aaa,&bbb,&ccc);
    id_list.extend(filtered_id_list_ptr);
    nodes_list.extend(filtered_nodes_list_ptr);
    tags_list.extend(filtered_tags_list_ptr);

    match write_lists_to_csv(&id_list,&nodes_list,&tags_list,merged_filename){
        Ok(f_records) => {
            merged_data=f_records;},
        Err(e) => println!("Error: {}", e),
    };
    Ok((merged_data, nodes_list.iter().flatten().cloned().collect()))
}


pub fn merge_csv_nodes(
    filenames: Vec<&str>,
    geohash_ptr: Vec<&str>,
    nodes_to_load: HashSet<i64>,
) -> Result<Vec<StringRecord>, Box<dyn Error>> {
    let mut merged_data = Vec::new(); 
    let mut id_list: Vec<i64> = Vec::new();
    let mut lat_list: Vec<i64> = Vec::new();
    let mut lon_list: Vec<i64> = Vec::new();
    for filename in filenames {

        // Open the MessagePack file
        let file = File::open(filename).expect("Failed to open file");
        let mut reader = BufReader::new(file);
        // Deserialize the MessagePack data into a serde::Value
        match from_read::<_, Value>(reader) {
            Ok(value) => {
     // Iterate through the geohash objects
     if let Value::Object(top_level_map) = value {
        for (key, sub_map) in top_level_map.iter() {
            //Fetch all nodes required from each available geohash
            if geohash_ptr.iter().any(|&s| s == key.as_str()){
                if let Value::Object(sub_map) = sub_map {
                    _=extract_values_nodes(sub_map,&mut id_list,& mut lat_list,&mut lon_list)
                }
                continue;
            }
        }
    }
        } 
            Err(e) => {
                eprintln!("Error deserializing MessagePack data: {}", e);
            }
        }
    }
    let mut filtered_id_list: Vec<i64> = Vec::new();
    let mut filtered_lat_list: Vec<i64> = Vec::new();
    let mut filtered_lon_list: Vec<i64> = Vec::new();
    let mut filtered_lat_list_float: Vec<f64> = Vec::new();
    let mut filtered_lon_list_float: Vec<f64> = Vec::new();
    //println!("id_list: {:?}", id_list);
    //println!("lat_list: {:?}", id_list);
    //println!("lon_list: {:?}", id_list);

    (filtered_id_list, filtered_lat_list, filtered_lon_list) = filter_lists_by_subset_nodes(&id_list,&lat_list,&lon_list,&nodes_to_load);
    filtered_lat_list_float = convert_lat_lon_value(filtered_lat_list);
    filtered_lon_list_float =  convert_lat_lon_value(filtered_lon_list);
    match write_lists_to_csv_nodes(&filtered_id_list,&filtered_lat_list_float,&filtered_lon_list_float,"./full_nodes.csv"){
        Ok(f_records) => {
            merged_data=f_records;},
        Err(e) => println!("Error: {}", e),
    };
    Ok(merged_data)
}